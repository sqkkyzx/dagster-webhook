"""Handled outputs are read from mounted storage and have a minimal contract."""

import pickle
from unittest.mock import Mock

import pytest
from dagster_graphql import DagsterGraphQLClientError

from gateway import run_outputs
from gateway.dagster_adapter import DagsterAdapter


def run_result(plan=True):
    return {
        "runOrError": {
            "__typename": "Run",
            "runId": "run-id",
            "status": "SUCCESS",
            "executionPlan": {
                "steps": [
                    {
                        "key": "export",
                        "solidHandleID": "export",
                        "kind": "COMPUTE",
                        "outputs": [{"name": "result"}, {"name": "optional"}],
                    },
                    {
                        "key": "waiting",
                        "solidHandleID": "waiting",
                        "kind": "COMPUTE",
                        "outputs": [{"name": "result"}],
                    },
                ]
            }
            if plan
            else None,
            "stepStats": [{"stepKey": "export", "status": "SUCCESS"}],
        }
    }


def handled(
    path=None,
    *,
    name="result",
    manager="io_manager",
    step="export",
    kind="PathMetadataEntry",
):
    entries = (
        []
        if path is None
        else [
            {
                "label": "path",
                "__typename": kind,
                "path" if kind == "PathMetadataEntry" else "text": str(path),
            }
        ]
    )
    return {
        "__typename": "HandledOutputEvent",
        "stepKey": step,
        "solidHandleID": step,
        "outputName": name,
        "managerKey": manager,
        "metadataEntries": entries,
    }


def page(events, cursor="end", more=False):
    return {
        "logsForRun": {
            "__typename": "EventConnection",
            "events": events,
            "cursor": cursor,
            "hasMore": more,
        }
    }


def query(events, *, run=None, **options):
    client = Mock()
    client._execute.side_effect = [
        run if run is not None else run_result(),
        page(events),
    ]
    return DagsterAdapter(client).get_run_outputs("run-id", **options), client


def stored(tmp_path, monkeypatch, name, value):
    monkeypatch.setenv("RUN_OUTPUT_STORAGE_ROOT", str(tmp_path))
    target = tmp_path / name
    target.write_bytes(pickle.dumps(value))
    return target


def test_simple_contract_reads_handled_file_and_keeps_step_states(
    tmp_path, monkeypatch
):
    path = stored(tmp_path, monkeypatch, "result", {"rows": [1, False, None]})
    result, _ = query([handled(path)])
    assert set(result) == {
        "run_id",
        "run_status",
        "items",
        "next_cursor",
        "has_more",
        "complete",
    }
    assert result["run_status"] == "SUCCESS" and result["complete"] is True
    step = result["items"][0]
    assert set(step) == {"step_key", "status", "outputs"}
    assert step["status"] == "SUCCESS"
    assert step["outputs"] == [
        {"output_name": "result", "output_value": {"rows": [1, False, None]}},
        {"output_name": "optional", "output_value": None},
    ]
    assert result["items"][1] == {
        "step_key": "waiting",
        "status": "NOT_STARTED",
        "outputs": [{"output_name": "result", "output_value": None}],
    }


@pytest.mark.parametrize(
    "value", [None, False, 0, "", "markdown\n# title", [1, 2], {"ok": True}]
)
def test_real_pickle_values_are_not_lost_by_truthiness(tmp_path, monkeypatch, value):
    path = stored(tmp_path, monkeypatch, "result", value)
    result, _ = query([handled(path)])
    found = result["items"][0]["outputs"][0]
    assert set(found) == {"output_name", "output_value"}
    assert found["output_value"] == value
    assert type(found["output_value"]) is type(value)


def test_step_output_is_ignored_even_with_exported_metadata(monkeypatch):
    read = Mock(side_effect=AssertionError("STEP_OUTPUT must not read files"))
    monkeypatch.setattr(run_outputs, "read_output_file", read)
    event = {
        "__typename": "ExecutionStepOutputEvent",
        "stepKey": "export",
        "outputName": "result",
        "metadataEntries": [
            {
                "__typename": "JsonMetadataEntry",
                "label": "api_value",
                "jsonString": "123",
            }
        ],
    }
    result, _ = query([event])
    assert result["items"][0]["outputs"][0] == {
        "output_name": "result",
        "output_value": None,
    }
    read.assert_not_called()


@pytest.mark.parametrize(
    "event", [handled(), handled("/not-opened", manager="another_manager")]
)
def test_missing_path_and_other_manager_return_null_without_read(monkeypatch, event):
    read = Mock(side_effect=AssertionError("Must not read unsupported output"))
    monkeypatch.setattr(run_outputs, "read_output_file", read)
    result, _ = query([event])
    assert result["items"][0]["outputs"][0]["output_value"] is None
    read.assert_not_called()


def test_missing_file_is_null_and_text_path_metadata_is_supported(
    tmp_path, monkeypatch
):
    path = stored(tmp_path, monkeypatch, "result", "actual text")
    result, _ = query([handled(path, kind="TextMetadataEntry")])
    assert result["items"][0]["outputs"][0]["output_value"] == "actual text"
    path.unlink()
    result, _ = query([handled(path)])
    assert result["items"][0]["outputs"][0]["output_value"] is None


def test_retry_same_path_reads_once_but_dynamic_paths_keep_same_names(
    tmp_path, monkeypatch
):
    first = stored(tmp_path, monkeypatch, "first", "one")
    second = stored(tmp_path, monkeypatch, "second", "two")
    read = Mock(wraps=run_outputs.read_output_file)
    monkeypatch.setattr(run_outputs, "read_output_file", read)
    result, _ = query([handled(first), handled(first), handled(second)])
    values = [
        entry
        for entry in result["items"][0]["outputs"]
        if entry["output_name"] == "result"
    ]
    assert values == [
        {"output_name": "result", "output_value": "one"},
        {"output_name": "result", "output_value": "two"},
    ]
    assert read.call_count == 2


def test_last_handling_replaces_previous_even_when_it_is_unreadable(
    tmp_path, monkeypatch
):
    path = stored(tmp_path, monkeypatch, "result", "old")
    result, _ = query([handled(path), handled(path, manager="unknown")])
    assert result["items"][0]["outputs"][0]["output_value"] is None


def test_partial_and_incremental_pages_do_not_invent_missing_outputs(
    tmp_path, monkeypatch
):
    path = stored(tmp_path, monkeypatch, "result", "value")
    client = Mock()
    client._execute.side_effect = [run_result(), page([handled(path)], "next", True)]
    result = DagsterAdapter(client).get_run_outputs("run-id", max_events=1)
    assert result["has_more"] is True and result["complete"] is False
    assert result["items"][0]["outputs"] == [
        {"output_name": "result", "output_value": "value"}
    ]
    assert result["items"][1]["outputs"] == []
    result, _ = query([], cursor="next")
    assert result["complete"] is False and not result["has_more"]
    assert all(not item["outputs"] for item in result["items"])


def test_reads_all_event_pages_and_counts_non_output_events(tmp_path, monkeypatch):
    path = stored(tmp_path, monkeypatch, "result", 42)
    client = Mock()
    client._execute.side_effect = [
        run_result(),
        page([{"__typename": "LogMessageEvent"}] * 1000, "first", True),
        page([handled(path)], "last"),
    ]
    result = DagsterAdapter(client).get_run_outputs("run-id", max_events=1001)
    assert result["complete"] is True
    assert result["items"][0]["outputs"][0]["output_value"] == 42
    assert [call.args[1]["limit"] for call in client._execute.call_args_list[1:]] == [
        1000,
        1,
    ]
    assert client._execute.call_args_list[2].args[1]["cursor"] == "first"


@pytest.mark.parametrize(
    "connection",
    [
        page([], more=True),
        page([{"__typename": "LogMessageEvent"}], cursor=None, more=True),
    ],
)
def test_nonadvancing_event_cursor_does_not_loop(connection):
    client = Mock()
    client._execute.side_effect = [run_result(), connection]
    with pytest.raises(DagsterGraphQLClientError, match="cursor"):
        DagsterAdapter(client).get_run_outputs("run-id")


def test_missing_run_does_not_read_events_or_files(monkeypatch):
    read = Mock()
    monkeypatch.setattr(run_outputs, "read_output_file", read)
    client = Mock()
    client._execute.return_value = {
        "runOrError": {"__typename": "RunNotFoundError", "message": "missing"}
    }
    with pytest.raises(DagsterGraphQLClientError, match="RunNotFoundError"):
        DagsterAdapter(client).get_run_outputs("missing")
    assert client._execute.call_count == 1
    read.assert_not_called()


def test_unknown_snapshot_status_and_missing_plan_are_not_fabricated():
    run = run_result(plan=False)
    run["runOrError"]["stepStats"][0]["status"] = None
    result, _ = query([handled()], run=run)
    assert result["items"][0]["status"] == "UNKNOWN"
    assert len(result["items"]) == 1


def test_observed_step_after_snapshot_is_unknown_not_not_started():
    run = run_result()
    run["runOrError"]["stepStats"] = []
    result, _ = query(
        [{"__typename": "ExecutionStepStartEvent", "stepKey": "export"}], run=run
    )
    assert result["items"][0]["status"] == "UNKNOWN"
    assert result["items"][1]["status"] == "NOT_STARTED"


def test_concrete_dynamic_steps_replace_their_unresolved_template():
    run = run_result()
    run["runOrError"]["executionPlan"] = {
        "steps": [
            {
                "key": "mapped[?]",
                "solidHandleID": "mapped",
                "kind": "UNRESOLVED_MAPPED",
                "outputs": [{"name": "result"}],
            }
        ]
    }
    run["runOrError"]["stepStats"] = [{"stepKey": "mapped[a]", "status": "IN_PROGRESS"}]
    result, _ = query(
        [
            {
                "__typename": "ExecutionStepStartEvent",
                "stepKey": "mapped[a]",
                "solidHandleID": "mapped",
            }
        ],
        run=run,
    )
    assert result["items"] == [
        {
            "step_key": "mapped[a]",
            "status": "IN_PROGRESS",
            "outputs": [{"output_name": "result", "output_value": None}],
        }
    ]


def test_response_value_budget_stops_reading_more_files(tmp_path, monkeypatch):
    path = stored(tmp_path, monkeypatch, "result", 0)
    read = Mock(wraps=run_outputs.read_output_file)
    monkeypatch.setattr(run_outputs, "read_output_file", read)
    monkeypatch.setattr(run_outputs, "VALUE_BUDGET_BYTES", 1)
    result, _ = query([handled(path), handled(path, name="second")])
    assert result["items"][0]["outputs"][:2] == [
        {"output_name": "result", "output_value": 0},
        {"output_name": "second", "output_value": None},
    ]
    assert read.call_count == 1
