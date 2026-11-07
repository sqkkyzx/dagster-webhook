"""Exercise pinned Dagster validation and GraphQL serialization entirely offline."""

from unittest.mock import Mock

import graphene
import pytest
from dagster import Map, Selector
from dagster._config import validate_config
from dagster._core.remote_origin import (
    RegisteredCodeLocationOrigin,
    RemoteJobOrigin,
    RemoteRepositoryOrigin,
)
from dagster._core.storage.dagster_run import DagsterRun
from dagster_graphql import DagsterGraphQLClientError
from dagster_graphql.schema.pipelines import config

from gateway.dagster_adapter import (
    CONFIG_ERROR_FIELDS,
    DagsterAdapter,
    build_runs_filter,
    encode_job_id,
)
from gateway.discovery import ConfigValidationIssue
from gateway.validation import SUBMISSION_TAG, validate_submission_tags


def native_graphql_errors(config_schema, value):
    """Use real evaluation stacks and real Graphene objects, not invented paths."""
    validation = validate_config(config_schema, value)
    assert not validation.success

    class Query(graphene.ObjectType):
        errors = graphene.List(config.GrapheneConfigValidationError)

        def resolve_errors(self, info):
            return [
                config.GrapheneConfigValidationError.from_dagster_error(
                    lambda key: None, error
                )
                for error in validation.errors
            ]

    schema = graphene.Schema(
        query=Query,
        types=[
            config.GrapheneRuntimeMismatchConfigError,
            config.GrapheneMissingFieldConfigError,
            config.GrapheneMissingFieldsConfigError,
            config.GrapheneFieldNotDefinedConfigError,
            config.GrapheneFieldsNotDefinedConfigError,
            config.GrapheneSelectorTypeConfigError,
        ],
    )
    result = schema.execute("{ errors { " + CONFIG_ERROR_FIELDS + " } }")
    assert not result.errors, result.errors
    return result.data["errors"]


@pytest.mark.parametrize(
    "config_schema,value,paths",
    [
        (
            {"ops": {"extract": {"config": {"count": int}}}},
            {"ops": {"extract": {"config": {"count": "bad"}}}},
            [["ops", "extract", "config", "count"]],
        ),
        ({"items": [int]}, {"items": ["bad"]}, [["items", 0]]),
        (
            {"values": Map(str, int)},
            {"values": {"some-key": "bad"}},
            [["values", "some-key"]],
        ),
        (
            {"values": Map(int, str)},
            {"values": {"not-an-int": "value"}},
            [["values", "not-an-int"]],
        ),
        (
            {"values": Map(int, int)},
            {"values": {3: "bad"}},
            [["values", "3"]],
        ),
        ({"nested": {"count": int}}, {"nested": {}}, [["nested", "count"]]),
        (
            {"nested": {"count": int, "name": str}},
            {"nested": {}},
            [["nested", "count"], ["nested", "name"]],
        ),
        (
            {"nested": {"count": int}},
            {"nested": {"count": 1, "extra": 2}},
            [["nested", "extra"]],
        ),
        (
            {"nested": {"count": int}},
            {"nested": {"count": 1, "first_extra": 2, "second_extra": 3}},
            [["nested", "first_extra"], ["nested", "second_extra"]],
        ),
        (
            {"choice": Selector({"a": int, "b": int})},
            {"choice": {"a": 1, "b": 2}},
            [["choice"]],
        ),
        (Selector({"a": int, "b": int}), {}, [[]]),
    ],
)
def test_native_errors_have_useful_paths_in_preflight_and_submission(
    config_schema, value, paths
):
    errors = native_graphql_errors(config_schema, value)
    client = Mock()
    adapter = DagsterAdapter(client)
    job_id = encode_job_id("location", "repo", "job")
    client._execute.return_value = {
        "isPipelineConfigValid": {
            "__typename": "RunConfigValidationInvalid",
            "errors": errors,
        }
    }
    preflight = adapter.validate_job(job_id, value)
    assert preflight["valid"] is False
    assert {tuple(issue["path"]) for issue in preflight["errors"]} == {
        tuple(path) for path in paths
    }
    for issue in preflight["errors"]:
        ConfigValidationIssue.model_validate(issue)

    client._execute.return_value = {
        "launchRun": {"__typename": "RunConfigValidationInvalid", "errors": errors}
    }
    with pytest.raises(DagsterGraphQLClientError) as caught:
        adapter.submit_run({"job_id": job_id, "run_config": value}, "offline-run")
    assert caught.value.args[0] == "RunConfigValidationInvalid"
    assert caught.value.args[1] == preflight["errors"]

    with pytest.raises(DagsterGraphQLClientError) as legacy_error:
        adapter.submit_legacy_job(
            {
                "job_name": "job",
                "repository_location_name": "location",
                "repository_name": "repo",
                "run_config": value,
            }
        )
    assert legacy_error.value.args[1] == preflight["errors"]


@pytest.mark.parametrize("key", [".dagster/repository", SUBMISSION_TAG])
def test_reserved_metadata_tag_is_rejected_before_upstream_execution(key):
    client = Mock()
    with pytest.raises(ValueError, match="reserved"):
        DagsterAdapter(client).submit_run(
            {
                "job_id": encode_job_id("actual_location", "repo", "job"),
                "tags": {key: "forged-value"},
            },
            "offline-run",
        )
    client._execute.assert_not_called()


def test_allowed_tags_preserve_actual_origin_for_exact_job_filter():
    tags = validate_submission_tags({"source": "test", "dagster/partition": "day"})
    origin = RemoteJobOrigin(
        RemoteRepositoryOrigin(RegisteredCodeLocationOrigin("location"), "repo"), "job"
    )
    run = DagsterRun(job_name="job", remote_job_origin=origin, tags=tags)
    filters = build_runs_filter(job_id=encode_job_id("location", "repo", "job"))
    for tag in filters["tags"]:
        assert run.tags_for_storage()[tag["key"]] == tag["value"]
    assert validate_submission_tags(None) is None
