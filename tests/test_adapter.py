"""GraphQL contracts are checked locally without executing any Dagster operation."""

import datetime
from unittest.mock import Mock

import pytest
from dagster_graphql import DagsterGraphQLClientError
from dagster_graphql.schema import create_schema
from fastapi import HTTPException
from graphql import get_variable_values, parse, validate

from gateway.dagster_adapter import (
    GRAPHQL_DOCUMENTS,
    DagsterAdapter,
    decode_job_id,
    encode_job_id,
)
from gateway.validation import SUBMISSION_TAG


def assert_graphql_variables_valid(document, variables):
    operation = parse(document).definitions[0]
    result = get_variable_values(
        create_schema().graphql_schema, operation.variable_definitions, variables
    )
    assert not isinstance(result, list), "\n".join(str(error) for error in result)


def test_all_graphql_documents_validate_against_pinned_dagster_schema():
    schema = create_schema().graphql_schema
    for document in GRAPHQL_DOCUMENTS:
        errors = validate(schema, parse(document))
        assert not errors, "\n".join(str(error) for error in errors)


def test_job_ids_keep_same_named_jobs_in_distinct_locations_separate():
    first = encode_job_id("location-a", "repo", "daily_job")
    second = encode_job_id("location-b", "repo", "daily_job")
    assert first != second
    assert decode_job_id(first) == {
        "repositoryLocationName": "location-a",
        "repositoryName": "repo",
        "pipelineName": "daily_job",
    }


@pytest.mark.parametrize(
    "value", ["bad-id", "", "a" * 4097, "e30", "WzEsMiwzXQ", "WyIiLCJyIiwiaiJd"]
)
def test_invalid_job_ids_are_actionable_validation_errors(value):
    with pytest.raises(HTTPException) as error:
        decode_job_id(value)
    assert error.value.status_code == 422


def test_job_discovery_preserves_names_and_filters_exactly():
    client = Mock()
    client._execute.return_value = {
        "repositoriesOrError": {
            "__typename": "RepositoryConnection",
            "nodes": [
                {
                    "name": "repo",
                    "location": {"name": location},
                    "jobs": [
                        {
                            "name": "same_job",
                            "description": None,
                            "isAssetJob": True,
                            "tags": [],
                        }
                    ],
                }
                for location in ["first", "second"]
            ],
        }
    }
    adapter = DagsterAdapter(client)
    records = adapter.list_jobs()["items"]
    assert len({item["job_id"] for item in records}) == 2
    assert adapter.list_jobs(location_name="second")["items"] == [records[1]]


def run_record(run_id="first", status="SUCCESS"):
    return {
        "runId": run_id,
        "jobName": "job",
        "status": status,
        "creationTime": 0,
        "startTime": 1,
        "endTime": 2,
        "tags": [{"key": "source", "value": "webhook"}],
        "parentRunId": None,
        "rootRunId": None,
        "stepStats": [{"stepKey": "failed_op", "status": "FAILURE"}]
        if status == "FAILURE"
        else [],
    }


def test_run_pagination_uses_lookahead_without_skipping_results():
    client = Mock()
    client._execute.return_value = {
        "runsOrError": {
            "__typename": "Runs",
            "results": [run_record("first"), run_record("second")],
        }
    }
    page = DagsterAdapter(client).list_runs(
        statuses=["SUCCESS"],
        job_name="job",
        tags={"a": "b"},
        cursor="previous",
        limit=1,
    )
    assert [item["run_id"] for item in page["items"]] == ["first"]
    assert page["next_cursor"] == "first"
    assert page["has_more"] is True
    variables = client._execute.call_args.args[1]
    assert variables == {
        "filter": {
            "statuses": ["SUCCESS"],
            "pipelineName": "job",
            "tags": [{"key": "a", "value": "b"}],
        },
        "cursor": "previous",
        "limit": 2,
    }


def test_run_detail_has_utc_times_failed_steps_and_no_config():
    client = Mock()
    client._execute.return_value = {
        "runOrError": {
            "__typename": "Run",
            **run_record(status="FAILURE"),
            "runConfigYaml": "private",
        }
    }
    result = DagsterAdapter(client).get_run("first")
    assert datetime.datetime.fromisoformat(
        result["created_at"]
    ).utcoffset() == datetime.timedelta(0)
    assert result["failed_steps"] == ["failed_op"]
    assert "failed_op" in result["failure_summary"]
    assert "runConfigYaml" not in result


def test_event_tail_cursor_is_preserved_for_later_polling():
    client = Mock()
    client._execute.return_value = {
        "logsForRun": {
            "__typename": "EventConnection",
            "events": [],
            "cursor": "tail",
            "hasMore": False,
        }
    }
    page = DagsterAdapter(client).get_events("first", "old", 20)
    assert page == {
        "run_id": "first",
        "items": [],
        "next_cursor": "tail",
        "has_more": False,
    }
    assert client._execute.call_args.args[1] == {
        "runId": "first",
        "cursor": "old",
        "limit": 20,
    }


@pytest.mark.parametrize("valid", [True, False])
def test_configuration_preflight_does_not_create_a_run(valid):
    client = Mock()
    issues = [
        {
            "message": "missing value",
            "stack": {"entries": []},
            "field": {"name": "ops"},
            "reason": "MISSING_REQUIRED_FIELD",
        }
    ]
    client._execute.return_value = {
        "isPipelineConfigValid": {
            "__typename": "PipelineConfigValidationValid"
            if valid
            else "RunConfigValidationInvalid",
            "errors": issues,
        }
    }
    result = DagsterAdapter(client).validate_job(
        encode_job_id("l", "r", "j"), {"ops": {}}, asset_selection=[["n", "a"]]
    )
    assert result["valid"] is valid
    assert result["errors"] == (
        []
        if valid
        else [
            {
                "message": "missing value",
                "path": ["ops"],
                "reason": "MISSING_REQUIRED_FIELD",
            }
        ]
    )
    document, variables = client._execute.call_args.args
    assert "mutation" not in document
    assert variables["selector"]["assetSelection"] == [{"path": ["n", "a"]}]


def test_submit_correlates_submission_without_unsupported_run_id_input():
    client = Mock()
    client._execute.return_value = {
        "launchRun": {
            "__typename": "LaunchRunSuccess",
            "run": run_record("known-id", "QUEUED"),
        }
    }
    result = DagsterAdapter(client).submit_run(
        {
            "job_id": encode_job_id("l", "r", "j"),
            "run_config": {},
            "tags": {"attempt": 2},
        },
        "submission-id",
    )
    params = client._execute.call_args.args[1]["params"]
    assert "runId" not in params["executionMetadata"]
    assert params["executionMetadata"]["tags"] == [
        {"key": "attempt", "value": "2"},
        {"key": SUBMISSION_TAG, "value": "submission-id"},
    ]
    assert result["run_id"] == "known-id"
    assert_graphql_variables_valid(*client._execute.call_args.args)


def test_legacy_submit_preserves_resolved_selector_and_sdk_tag_normalization():
    client = Mock()
    client._execute.return_value = {
        "launchRun": {
            "__typename": "LaunchRunSuccess",
            "run": run_record("new-run", "QUEUED"),
        }
    }
    result = DagsterAdapter(client).submit_legacy_job(
        {
            "job_name": "job",
            "repository_location_name": "location",
            "repository_name": "repo",
            "run_config": {"some": "config"},
            "tags": {"source": "legacy", "nested": {"count": 3}},
            "op_selection": ["extract"],
            "asset_selection": [["warehouse", "orders"]],
        }
    )
    params = client._execute.call_args.args[1]["params"]
    assert params["selector"] == {
        "repositoryLocationName": "location",
        "repositoryName": "repo",
        "pipelineName": "job",
        "solidSelection": ["extract"],
        "assetSelection": [{"path": ["warehouse", "orders"]}],
    }
    assert "runId" not in params["executionMetadata"]
    assert params["executionMetadata"]["tags"] == [
        {"key": "source", "value": "legacy"},
        {"key": "nested", "value": '{"count": 3}'},
    ]
    assert params["runConfigData"] == {"some": "config"}
    assert result == "new-run"
    assert_graphql_variables_valid(*client._execute.call_args.args)


def test_direct_run_does_not_inject_idempotent_submission_marker():
    client = Mock()
    client._execute.return_value = {
        "launchRun": {
            "__typename": "LaunchRunSuccess",
            "run": run_record("direct-run", "QUEUED"),
        }
    }
    result = DagsterAdapter(client).submit_direct_run(
        {
            "job_id": encode_job_id("l", "r", "j"),
            "run_config": {},
            "tags": {"source": "direct"},
        }
    )
    params = client._execute.call_args.args[1]["params"]
    assert params["executionMetadata"] == {
        "tags": [{"key": "source", "value": "direct"}]
    }
    assert result["run_id"] == "direct-run"
    assert_graphql_variables_valid(*client._execute.call_args.args)


def test_submission_lookup_only_reads_the_exact_marker():
    client = Mock()
    record = run_record("actual-run", "QUEUED")
    record["tags"].append({"key": SUBMISSION_TAG, "value": "submission-id"})
    client._execute.return_value = {
        "runsOrError": {"__typename": "Runs", "results": [record]}
    }
    result = DagsterAdapter(client).find_submission_run("submission-id")
    assert result["run_id"] == "actual-run"
    document, variables = client._execute.call_args.args
    assert "mutation" not in document
    assert variables == {
        "filter": {"tags": [{"key": SUBMISSION_TAG, "value": "submission-id"}]},
        "cursor": None,
        "limit": 2,
    }
    assert_graphql_variables_valid(document, variables)


def test_submission_lookup_without_a_match_does_not_create_a_run():
    client = Mock()
    client._execute.return_value = {
        "runsOrError": {"__typename": "Runs", "results": []}
    }
    assert DagsterAdapter(client).find_submission_run("missing-marker") is None
    assert client._execute.call_count == 1
    assert "mutation" not in client._execute.call_args.args[0]


@pytest.mark.parametrize("kind", ["multiple", "missing_marker", "wrong_marker"])
def test_submission_lookup_rejects_ambiguous_or_mismatched_results(kind):
    client = Mock()
    record = run_record("actual-run", "QUEUED")
    if kind == "multiple":
        record["tags"].append({"key": SUBMISSION_TAG, "value": "submission-id"})
        records = [record, {**record, "runId": "other-run"}]
    else:
        if kind == "wrong_marker":
            record["tags"].append({"key": SUBMISSION_TAG, "value": "other-marker"})
        records = [record]
    client._execute.return_value = {
        "runsOrError": {"__typename": "Runs", "results": records}
    }
    with pytest.raises(
        DagsterGraphQLClientError, match="SubmissionCorrelationConflict"
    ):
        DagsterAdapter(client).find_submission_run("submission-id")


def test_partial_batch_termination_keeps_per_run_outcomes():
    client = Mock()
    client._execute.return_value = {
        "terminateRuns": {
            "__typename": "TerminateRunsResult",
            "terminateRunResults": [
                {"__typename": "TerminateRunSuccess", "run": {"runId": "first"}},
                {
                    "__typename": "RunNotFoundError",
                    "runId": "missing",
                    "message": "not found",
                },
            ],
        }
    }
    result = DagsterAdapter(client).terminate_runs(["first", "missing"])
    assert result["succeeded"] == result["failed"] == 1
    assert result["results"][0]["status"] == "accepted"
    assert result["results"][1]["run_id"] == "missing"
    assert result["results"][1]["error_type"] == "RunNotFoundError"


def test_incomplete_termination_result_fails_instead_of_inventing_success():
    client = Mock()
    client._execute.return_value = {
        "terminateRuns": {
            "__typename": "TerminateRunsResult",
            "terminateRunResults": [],
        }
    }
    with pytest.raises(DagsterGraphQLClientError):
        DagsterAdapter(client).terminate_runs(["first"])


def test_upstream_missing_run_keeps_not_found_type():
    client = Mock()
    client._execute.return_value = {
        "runOrError": {"__typename": "RunNotFoundError", "message": "not found"}
    }
    with pytest.raises(DagsterGraphQLClientError) as error:
        DagsterAdapter(client).get_run("missing")
    assert error.value.args[0] == "RunNotFoundError"


def test_exact_job_filter_uses_native_repository_label():
    client = Mock()
    client._execute.return_value = {
        "runsOrError": {"__typename": "Runs", "results": []}
    }
    adapter = DagsterAdapter(client)
    adapter.list_runs(
        job_id=encode_job_id("first", "repo", "same_job"), tags={"source": "webhook"}
    )
    first = client._execute.call_args.args[1]["filter"]
    adapter.list_runs(job_id=encode_job_id("second", "repo", "same_job"))
    second = client._execute.call_args.args[1]["filter"]
    assert first["pipelineName"] == second["pipelineName"] == "same_job"
    assert first["tags"] == [
        {"key": "source", "value": "webhook"},
        {"key": ".dagster/repository", "value": "repo@first"},
    ]
    assert second["tags"] == [{"key": ".dagster/repository", "value": "repo@second"}]


@pytest.mark.parametrize(
    "filters",
    [{"job_name": "other"}, {"tags": {".dagster/repository": "another@place"}}],
)
def test_conflicting_job_filters_fail_before_querying(filters):
    client = Mock()
    with pytest.raises(HTTPException) as error:
        DagsterAdapter(client).list_runs(
            job_id=encode_job_id("first", "repo", "job"), **filters
        )
    assert error.value.status_code == 422
    client._execute.assert_not_called()


def test_run_identity_preserves_origin_and_handles_old_runs_without_origin():
    client = Mock()
    client._execute.return_value = {
        "runOrError": {
            "__typename": "Run",
            **run_record(),
            "repositoryOrigin": {
                "repositoryLocationName": "location",
                "repositoryName": "repo",
            },
        }
    }
    record = DagsterAdapter(client).get_run("first")
    assert record["job_id"] == encode_job_id("location", "repo", "job")
    assert record["repository_location_name"] == "location"
    assert record["repository_name"] == "repo"
    client._execute.return_value["runOrError"]["repositoryOrigin"] = None
    record = DagsterAdapter(client).get_run("first")
    assert (
        record["job_id"]
        is record["repository_location_name"]
        is record["repository_name"]
        is None
    )
