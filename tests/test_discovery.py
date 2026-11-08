"""Mock-only HTTP resource tests; never connect to a Dagster deployment."""

import json
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.security import HTTPBearer
from fastapi.testclient import TestClient

from gateway.dagster_adapter import DagsterAdapter, encode_job_id
from gateway.discovery import JOB_EXAMPLE, RUN_EXAMPLE, create_discovery_router

AUTH = {"Authorization": "Bearer local-test-token"}
INTERNAL_JOB_ID = encode_job_id("my_code", "my_repository", "daily_job")
INTERNAL_JOB = {**JOB_EXAMPLE, "job_id": INTERNAL_JOB_ID}


def candidate(location="my_code", repository="my_repository", name="daily_job"):
    return {
        **JOB_EXAMPLE,
        "job_id": encode_job_id(location, repository, name),
        "job_name": name,
        "repository_location_name": location,
        "repository_name": repository,
    }


@pytest.fixture
def resource_api(monkeypatch):
    monkeypatch.delenv("DAGSTER_REPOSITORY_NAME", raising=False)
    adapter = Mock(spec=DagsterAdapter)
    adapter.list_jobs.return_value = {"items": [INTERNAL_JOB]}
    adapter.get_job.return_value = {**INTERNAL_JOB, "op_names": ["extract"]}
    adapter.get_config_schema.return_value = {
        "job_id": INTERNAL_JOB_ID,
        "format": "dagster",
        "root_type_key": "Shape.example",
        "default_yaml": "{}\n",
        "types": [],
    }
    adapter.validate_job.return_value = {
        "job_id": INTERNAL_JOB_ID,
        "valid": True,
        "errors": [],
    }
    api = FastAPI()
    api.include_router(create_discovery_router(lambda: adapter, HTTPBearer()))
    with TestClient(api) as client:
        yield client, adapter


def test_discovery_returns_job_names_and_origins_without_internal_ids(resource_api):
    client, adapter = resource_api
    response = client.get(
        "/api/v1/jobs?repository_location_name=my_code&repository_name=my_repository",
        headers=AUTH,
    )
    assert response.status_code == 200
    assert response.json()["items"] == [JOB_EXAMPLE]
    assert "job_id" not in response.text
    adapter.list_jobs.assert_called_once_with("my_code", "my_repository")


def test_discovery_unfiltered_does_not_apply_default_repository(resource_api):
    client, adapter = resource_api
    response = client.get("/api/v1/jobs", headers=AUTH)
    assert response.status_code == 200
    adapter.list_jobs.assert_called_once_with(None, None)


def test_discovery_hidden_location_alias_is_compatible_and_conflicts_are_rejected(
    resource_api,
):
    client, adapter = resource_api
    assert (
        client.get("/api/v1/jobs?location_name=my_code", headers=AUTH).status_code
        == 200
    )
    adapter.list_jobs.assert_called_once_with("my_code", None)
    adapter.list_jobs.reset_mock()
    assert (
        client.get(
            "/api/v1/jobs?location_name=old&repository_location_name=new", headers=AUTH
        ).status_code
        == 422
    )
    adapter.list_jobs.assert_not_called()


def test_job_name_only_resolves_unique_custom_repository(resource_api):
    client, adapter = resource_api
    adapter.list_jobs.return_value = {"items": [candidate(repository="custom_repo")]}
    adapter.get_job.return_value = {
        **candidate(repository="custom_repo"),
        "op_names": ["extract"],
    }
    response = client.get("/api/v1/jobs/daily_job", headers=AUTH)
    assert response.status_code == 200
    assert response.json()["repository_name"] == "custom_repo"
    assert response.json()["job_name"] == "daily_job"
    assert "job_id" not in response.json()
    adapter.get_job.assert_called_once_with(
        encode_job_id("my_code", "custom_repo", "daily_job")
    )


def test_job_location_only_resolves_actual_repository(resource_api):
    client, adapter = resource_api
    adapter.list_jobs.return_value = {"items": [candidate(repository="actual_repo")]}
    response = client.get(
        "/api/v1/jobs/daily_job?repository_location_name=my_code", headers=AUTH
    )
    assert response.status_code == 200
    assert response.json()["repository_name"] == "actual_repo"
    adapter.get_job.assert_called_once_with(
        encode_job_id("my_code", "actual_repo", "daily_job")
    )


def test_job_repository_only_resolves_location(resource_api):
    client, adapter = resource_api
    response = client.get(
        "/api/v1/jobs/daily_job?repository_name=my_repository", headers=AUTH
    )
    assert response.status_code == 200
    assert response.json()["repository_location_name"] == "my_code"
    adapter.get_job.assert_called_once_with(INTERNAL_JOB_ID)


def test_exact_named_job_can_be_requested_without_discovery(resource_api):
    client, adapter = resource_api
    response = client.get(
        "/api/v1/jobs/daily_job?repository_name=my_repository&repository_location_name=my_code",
        headers=AUTH,
    )
    assert response.status_code == 200
    adapter.get_job.assert_called_once_with(INTERNAL_JOB_ID)
    adapter.list_jobs.assert_not_called()


def test_ambiguous_name_returns_candidates_without_internal_ids(resource_api):
    client, adapter = resource_api
    adapter.list_jobs.return_value = {
        "items": [candidate("first"), candidate("second")]
    }
    response = client.get("/api/v1/jobs/daily_job", headers=AUTH)
    assert response.status_code == 409
    assert "job_id" not in response.text
    assert "repository_location_name" in response.text
    adapter.get_job.assert_not_called()


def test_missing_named_job_returns_404(resource_api):
    client, adapter = resource_api
    adapter.list_jobs.return_value = {"items": []}
    assert client.get("/api/v1/jobs/missing", headers=AUTH).status_code == 404
    adapter.get_job.assert_not_called()


def test_config_schema_uses_job_name_and_returns_resolved_reference(resource_api):
    client, adapter = resource_api
    response = client.get("/api/v1/jobs/daily_job/config-schema", headers=AUTH)
    assert response.status_code == 200
    assert response.json() == {
        "job_name": "daily_job",
        "repository_location_name": "my_code",
        "repository_name": "my_repository",
        "format": "dagster",
        "root_type_key": "Shape.example",
        "default_yaml": "{}\n",
        "types": [],
    }
    adapter.get_config_schema.assert_called_once_with(INTERNAL_JOB_ID)


def test_preflight_invalid_configuration_is_a_successful_validation_result(
    resource_api,
):
    client, adapter = resource_api
    adapter.validate_job.return_value = {
        "job_id": INTERNAL_JOB_ID,
        "valid": False,
        "errors": [
            {"message": "invalid", "path": ["ops"], "reason": "MISSING_REQUIRED_FIELD"}
        ],
    }
    response = client.post(
        "/api/v1/jobs/daily_job/validate", headers=AUTH, json={"run_config": {}}
    )
    assert response.status_code == 200
    assert response.json()["valid"] is False
    assert response.json()["job_name"] == "daily_job"
    assert response.json()["repository_name"] == "my_repository"
    assert "job_id" not in response.json()
    adapter.validate_job.assert_called_once_with(
        INTERNAL_JOB_ID, run_config={}, op_selection=None, asset_selection=None
    )


def test_preflight_rejects_unknown_fields(resource_api):
    client, adapter = resource_api
    response = client.post(
        "/api/v1/jobs/daily_job/validate", headers=AUTH, json={"run_confg": {}}
    )
    assert response.status_code == 422
    adapter.validate_job.assert_not_called()


def test_runs_filters_have_unambiguous_repeated_parameter_semantics(resource_api):
    client, adapter = resource_api
    adapter.list_runs.return_value = {
        "items": [RUN_EXAMPLE],
        "next_cursor": "tail",
        "has_more": True,
    }
    response = client.get(
        "/api/v1/runs?statuses=FAILURE&statuses=SUCCESS&tag=source=webhook&tag=value=a=b&limit=1",
        headers=AUTH,
    )
    assert response.status_code == 200
    adapter.list_runs.assert_called_once_with(
        statuses=["FAILURE", "SUCCESS"],
        job_name=None,
        tags={"source": "webhook", "value": "a=b"},
        cursor=None,
        limit=1,
        repository_location_name=None,
        repository_name=None,
    )


def test_run_name_only_filter_is_forwarded_without_discovery(resource_api):
    client, adapter = resource_api
    adapter.list_runs.return_value = {
        "items": [],
        "next_cursor": None,
        "has_more": False,
    }
    response = client.get("/api/v1/runs?job_name=removed_job", headers=AUTH)
    assert response.status_code == 200
    assert adapter.list_runs.call_args.kwargs["job_name"] == "removed_job"
    assert adapter.list_runs.call_args.kwargs["repository_location_name"] is None
    assert adapter.list_runs.call_args.kwargs["repository_name"] is None
    adapter.list_jobs.assert_not_called()


def test_run_location_filter_does_not_apply_repository_default(
    resource_api, monkeypatch
):
    client, adapter = resource_api
    monkeypatch.setenv("DAGSTER_REPOSITORY_NAME", "configured_repo")
    adapter.list_runs.return_value = {
        "items": [],
        "next_cursor": None,
        "has_more": False,
    }
    response = client.get(
        "/api/v1/runs?job_name=job&repository_location_name=place", headers=AUTH
    )
    assert response.status_code == 200
    assert adapter.list_runs.call_args.kwargs["repository_name"] is None
    assert adapter.list_runs.call_args.kwargs["repository_location_name"] == "place"
    adapter.list_jobs.assert_not_called()


@pytest.mark.parametrize(
    "query",
    [
        "limit=0",
        "limit=201",
        "statuses=WRONG",
        "tag=broken",
        "tag==value",
        "tag=a=b&tag=a=c",
        "job_id=old-identifier",
        "job_name=job&repository_location_name=place&repository_name=repo&tag=.dagster/repository=wrong@place",
    ],
)
def test_invalid_run_query_does_not_call_upstream(resource_api, query):
    client, adapter = resource_api
    assert client.get("/api/v1/runs?" + query, headers=AUTH).status_code == 422
    adapter.list_runs.assert_not_called()


def test_event_cursor_available_at_current_tail(resource_api):
    client, adapter = resource_api
    adapter.get_events.return_value = {
        "run_id": "run",
        "items": [],
        "next_cursor": "opaque",
        "has_more": False,
    }
    response = client.get("/api/v1/runs/run/events?cursor=old&limit=20", headers=AUTH)
    assert response.status_code == 200
    assert response.json()["next_cursor"] == "opaque"
    adapter.get_events.assert_called_once_with("run", "old", 20)


def test_new_resources_document_named_jobs_security_models_examples_and_errors(
    resource_api,
):
    client, _ = resource_api
    schema = client.get("/openapi.json").json()
    assert "job_id" not in json.dumps(schema)
    assert "/api/v1/jobs/{job_name}" in schema["paths"]
    assert "location_name" not in {
        parameter["name"]
        for parameter in schema["paths"]["/api/v1/jobs"]["get"].get("parameters", [])
    }
    for methods in schema["paths"].values():
        for operation in methods.values():
            assert (
                operation["summary"]
                and operation["description"]
                and operation["operationId"]
            )
            assert operation["tags"] and operation["security"]
            assert {"401", "404", "422", "502"} <= operation["responses"].keys()
            media = operation["responses"]["200"]["content"]["application/json"]
            assert media.get("example") or media.get("examples")
            assert media["schema"]
            for parameter in operation.get("parameters", []):
                assert parameter.get("description")
    for path in (
        "/api/v1/jobs/{job_name}",
        "/api/v1/jobs/{job_name}/config-schema",
        "/api/v1/jobs/{job_name}/validate",
    ):
        method = "post" if path.endswith("/validate") else "get"
        parameters = {
            item["name"]: item for item in schema["paths"][path][method]["parameters"]
        }
        for key in ("repository_name", "repository_location_name"):
            assert parameters[key]["required"] is False
            assert parameters[key]["schema"].get("default") is None


@pytest.mark.parametrize("non_finite", [float("nan"), float("inf"), float("-inf")])
def test_preflight_rejects_non_finite_json_numbers(non_finite):
    from pydantic import ValidationError

    from gateway.discovery import ValidationRequest

    with pytest.raises(ValidationError):
        ValidationRequest.model_validate({"run_config": {"nested": [non_finite]}})


def test_run_repository_only_filter_is_supported_without_discovery(resource_api):
    client, adapter = resource_api
    adapter.list_runs.return_value = {
        "items": [],
        "next_cursor": "last-scanned",
        "has_more": True,
    }
    response = client.get("/api/v1/runs?repository_name=historic_repo", headers=AUTH)
    assert response.status_code == 200
    assert response.json() == {
        "items": [],
        "next_cursor": "last-scanned",
        "has_more": True,
    }
    assert adapter.list_runs.call_args.kwargs["repository_name"] == "historic_repo"
    assert adapter.list_runs.call_args.kwargs["repository_location_name"] is None
    adapter.list_jobs.assert_not_called()


def test_actual_ambiguous_job_error_reports_only_named_candidates(client, app):
    import main

    adapter = Mock(spec=DagsterAdapter)
    adapter.list_jobs.return_value = {
        "items": [candidate("first", "custom"), candidate("second", "custom")]
    }
    app.dependency_overrides[main.get_adapter] = lambda: adapter
    response = client.get(
        "/api/v1/jobs/daily_job", headers={"Authorization": "Bearer test-api-token"}
    )
    assert response.status_code == 409
    assert response.json()["code"] == "JOB_SELECTION_AMBIGUOUS"
    assert response.json()["details"] == [
        {
            "job_name": "daily_job",
            "repository_location_name": "first",
            "repository_name": "custom",
        },
        {
            "job_name": "daily_job",
            "repository_location_name": "second",
            "repository_name": "custom",
        },
    ]
    assert "job_id" not in response.text
    adapter.get_job.assert_not_called()
