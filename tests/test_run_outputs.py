"""Output-value and history contracts use the pinned remote GraphQL shapes."""

import json
from unittest.mock import Mock

import pytest
from dagster_graphql import DagsterGraphQLClientError

from gateway import run_outputs
from gateway.dagster_adapter import DagsterAdapter


def run_result(plan=True, stats=True):
    return {
        "runOrError": {
            "__typename": "Run",
            "runId": "run-id",
            "status": "SUCCESS",
            "executionPlan": {
                "steps": [
                    {
                        "key": "export",
                        "solidHandleID": "nested.export",
                        "kind": "COMPUTE",
                        "outputs": [{"name": "payload"}, {"name": "optional"}],
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
            "stepStats": [
                {
                    "stepKey": "export",
                    "status": "SUCCESS",
                    "startTime": 1,
                    "endTime": 3,
                    "attempts": [
                        {"startTime": 1, "endTime": 2},
                        {"startTime": 2, "endTime": 3},
                    ],
                }
            ]
            if stats
            else [],
        }
    }


def metadata(kind="JsonMetadataEntry", label="api_value", **value):
    return {"__typename": kind, "label": label, "description": None, **value}


def output(
    *entries, kind="ExecutionStepOutputEvent", step="export", timestamp="2000", **extra
):
    return {
        "__typename": kind,
        "stepKey": step,
        "solidHandleID": "nested.export",
        "timestamp": timestamp,
        "outputName": "payload",
        "typeCheck": {"success": True},
        "metadataEntries": list(entries),
        **extra,
    }


def page(events, cursor="tail", more=False):
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


def event_result(response, index=0):
    return response["items"][0]["outputs"][index]


def test_plan_stats_and_output_history_are_preserved_without_inventing_values():
    response, client = query(
        [
            output(metadata(jsonString='{"count":3}')),
            output(
                metadata("PathMetadataEntry", label="path", path="/data/result"),
                kind="HandledOutputEvent",
                managerKey="io_manager",
            ),
            output(metadata(jsonString='{"count":4}'), timestamp="3000"),
        ]
    )
    assert response["run_status"] == "SUCCESS"
    assert response["execution_plan_available"] is True
    assert response["complete"] is True and response["has_more"] is False
    assert response["scanned_events"] == 3
    step = response["items"][0]
    assert step["op_handle"] == "nested.export"
    assert step["status"] == "SUCCESS"
    assert step["declared_outputs"] == ["payload", "optional"]
    assert len(step["attempts"]) == 2
    assert [entry["event_type"] for entry in step["outputs"]] == [
        "STEP_OUTPUT",
        "HANDLED_OUTPUT",
        "STEP_OUTPUT",
    ]
    assert event_result(response)["value"] == {"count": 3}
    assert event_result(response, 1)["value_unavailable_reason"] == "NOT_EXPOSED"
    assert event_result(response, 1)["manager_key"] == "io_manager"
    assert event_result(response, 2)["value"] == {"count": 4}
    assert response["items"][1]["status"] == "NOT_STARTED"
    assert client._execute.call_count == 2


@pytest.mark.parametrize(
    "entry,expected",
    [
        (
            metadata(jsonString='{"nested":[0,false,null]}'),
            {"nested": [0, False, None]},
        ),
        (metadata(jsonString="null"), None),
        (metadata("NullMetadataEntry"), None),
        (metadata("TextMetadataEntry", text=""), ""),
        (metadata("IntMetadataEntry", intValue=0, intRepr="0"), 0),
        (metadata("IntMetadataEntry", intValue=None, intRepr=str(2**70)), 2**70),
        (metadata("FloatMetadataEntry", floatValue=0, floatRepr="0.0"), 0.0),
        (metadata("BoolMetadataEntry", boolValue=False), False),
    ],
)
def test_actual_scalar_and_json_values_do_not_lose_null_false_or_zero(entry, expected):
    response, _ = query([output(entry)])
    found = event_result(response)
    assert found["value_available"] is True
    assert found["value"] == expected
    assert found["value_source"] == "api_value"
    assert found["value_unavailable_reason"] is None
    assert found["metadata"][0]["value_available"] is True


@pytest.mark.parametrize(
    "entry",
    [
        metadata(jsonString="NaN"),
        metadata(jsonString="Infinity"),
        metadata(jsonString="{"),
        metadata("FloatMetadataEntry", floatValue=None, floatRepr="nan"),
        metadata("FloatMetadataEntry", floatValue=None, floatRepr="inf"),
        metadata("PathMetadataEntry", path="/secret/must-not-be-opened"),
        metadata("UrlMetadataEntry", url="https://must-not-be-fetched.test/value"),
        metadata("FutureMetadataEntry"),
    ],
)
def test_unsupported_or_invalid_values_are_explicitly_unavailable(entry):
    response, client = query([output(entry)])
    found = event_result(response)
    assert found["value_available"] is False
    assert found["value"] is None
    assert found["value_unavailable_reason"] == "INVALID_VALUE"
    assert client._execute.call_count == 2  # Only Run and event queries.
    json.dumps(response, allow_nan=False)


def test_custom_label_and_duplicate_labels_are_unambiguous():
    event = output(metadata(label="custom", jsonString="[1,2]"))
    response, _ = query([event])
    assert event_result(response)["value_unavailable_reason"] == "NOT_EXPOSED"
    response, _ = query([event], value_metadata_key="custom")
    assert event_result(response)["value"] == [1, 2]
    response, _ = query([output(metadata(jsonString="1"), metadata(jsonString="2"))])
    assert event_result(response)["value_unavailable_reason"] == "INVALID_VALUE"


def test_value_size_limit_and_text_preview_are_separate():
    value = "x" * (run_outputs.MAX_VALUE_BYTES - 2)
    response, _ = query([output(metadata("TextMetadataEntry", text=value))])
    found = event_result(response)
    assert found["value_available"] is True
    preview = found["metadata"][0]
    assert preview["truncated"] is True and preview["value_available"] is False
    assert preview["value"]
    assert (
        len(json.dumps(preview["value"], ensure_ascii=False).encode())
        <= run_outputs.METADATA_PREVIEW_BYTES
    )
    response, _ = query([output(metadata("TextMetadataEntry", text=value + "x"))])
    assert event_result(response)["value_unavailable_reason"] == "VALUE_TOO_LARGE"


def test_escaped_unicode_is_measured_after_json_decoding():
    value = "中" * 12000
    response, _ = query([output(metadata(jsonString=json.dumps(value)))])
    assert event_result(response)["value_available"] is True
    assert event_result(response)["value"] == value


def test_deep_json_is_rejected_without_internal_error():
    deep = "[" * 2000 + "0" + "]" * 2000
    response, _ = query([output(metadata(jsonString=deep))])
    assert event_result(response)["value_unavailable_reason"] == "INVALID_VALUE"


def test_value_and_metadata_budgets_are_independent_and_explicit(monkeypatch):
    monkeypatch.setattr(run_outputs, "VALUE_BUDGET_BYTES", 6)
    monkeypatch.setattr(run_outputs, "METADATA_BUDGET_BYTES", 6)
    response, _ = query(
        [
            output(metadata("TextMetadataEntry", text="abcd")),
            output(metadata("NullMetadataEntry")),
        ]
    )
    assert event_result(response)["value"] == "abcd"
    second = event_result(response, 1)
    assert second["value_unavailable_reason"] == "RESPONSE_LIMIT"
    assert second["metadata"][0]["value"] is None
    assert second["metadata"][0]["truncated"] is True


def test_missing_execution_plan_uses_observed_steps_without_claiming_full_coverage():
    response, _ = query(
        [output(metadata(jsonString="null"))], run=run_result(plan=False)
    )
    assert response["execution_plan_available"] is False
    assert len(response["items"]) == 1
    assert response["items"][0]["op_handle"] == "nested.export"
    assert response["items"][0]["declared_outputs"] == []


def test_output_after_a_snapshot_with_no_stats_is_not_reported_not_started():
    response, _ = query([output(metadata(jsonString="0"))], run=run_result(stats=False))
    assert response["items"][0]["status"] == "UNKNOWN"
    assert response["items"][1]["status"] == "NOT_STARTED"


def test_dynamic_step_can_obtain_handle_from_non_output_events():
    run = run_result(plan=False, stats=False)
    run["runOrError"]["stepStats"] = [
        {"stepKey": "mapped[a]", "status": "IN_PROGRESS", "attempts": []}
    ]
    response, _ = query(
        [
            {
                "__typename": "ExecutionStepStartEvent",
                "stepKey": "mapped[a]",
                "solidHandleID": "nested.mapped",
                "timestamp": "1000",
            }
        ],
        run=run,
    )
    assert response["items"][0]["op_handle"] == "nested.mapped"
    assert response["items"][0]["status"] == "IN_PROGRESS"
    assert response["items"][0]["outputs"] == []


def test_start_after_snapshot_without_stats_is_not_reported_not_started():
    response, _ = query(
        [
            {
                "__typename": "ExecutionStepStartEvent",
                "stepKey": "export",
                "solidHandleID": "nested.export",
                "timestamp": "1000",
            }
        ],
        run=run_result(stats=False),
    )
    assert response["items"][0]["status"] == "UNKNOWN"
    assert response["items"][1]["status"] == "NOT_STARTED"


def test_paginated_scan_is_bounded_and_counts_non_output_events():
    client = Mock()
    client._execute.side_effect = [
        run_result(),
        page([{"__typename": "LogMessageEvent"}], "one", True),
        page(
            [output(metadata(jsonString="1")), output(metadata(jsonString="2"))],
            "three",
            True,
        ),
    ]
    response = DagsterAdapter(client).get_run_outputs("run-id", max_events=3)
    assert response["scanned_events"] == 3
    assert response["next_cursor"] == "three" and response["has_more"] is True
    assert response["complete"] is False
    assert [call.args[1]["limit"] for call in client._execute.call_args_list[1:]] == [
        3,
        2,
    ]
    response, _ = query([], cursor="three")
    assert response["complete"] is False and response["has_more"] is False


@pytest.mark.parametrize(
    "connection",
    [page([], "same", True), page([{"__typename": "LogMessageEvent"}], "same", True)],
)
def test_nonadvancing_cursor_cannot_loop_forever(connection):
    client = Mock()
    client._execute.side_effect = [run_result(), connection]
    with pytest.raises(DagsterGraphQLClientError, match="cursor did not advance"):
        DagsterAdapter(client).get_run_outputs("run-id", cursor="same")


def test_missing_run_is_returned_before_querying_events():
    client = Mock()
    client._execute.return_value = {
        "runOrError": {"__typename": "RunNotFoundError", "message": "missing"}
    }
    with pytest.raises(DagsterGraphQLClientError, match="RunNotFoundError"):
        DagsterAdapter(client).get_run_outputs("missing")
    client._execute.assert_called_once()


def test_failed_type_check_is_kept_with_output_evidence():
    response, _ = query(
        [output(metadata(jsonString="0"), typeCheck={"success": False})]
    )
    assert event_result(response)["type_check_success"] is False
    assert (
        event_result(response)["value_available"] is True
    )  # Explicitly exported diagnostic value, not a successful op claim.
