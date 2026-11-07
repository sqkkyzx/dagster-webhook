"""Mock-only HTTP resource tests; never connect to a Dagster deployment."""

from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.security import HTTPBearer
from fastapi.testclient import TestClient

from gateway.dagster_adapter import DagsterAdapter
from gateway.discovery import (
    EXAMPLE_JOB_ID,
    JOB_EXAMPLE,
    RUN_EXAMPLE,
    create_discovery_router,
)


@pytest.fixture
def resource_api():
    adapter = Mock(spec=DagsterAdapter)
    api = FastAPI()
    api.include_router(create_discovery_router(lambda: adapter, HTTPBearer()))
    with TestClient(api) as client:
        yield client, adapter


AUTH = {"Authorization": "Bearer local-test-token"}


def test_discovery_returns_different_job_ids_without_rewriting_them(resource_api):
    client, adapter = resource_api
    adapter.list_jobs.return_value = {"items": [JOB_EXAMPLE]}
    response = client.get(
        "/api/v1/jobs?location_name=my_code&repository_name=my_repository", headers=AUTH
    )
    assert response.status_code == 200
    assert response.json()["items"][0]["job_id"] == EXAMPLE_JOB_ID
    adapter.list_jobs.assert_called_once_with("my_code", "my_repository")


def test_preflight_invalid_configuration_is_a_successful_validation_result(
    resource_api,
):
    client, adapter = resource_api
    adapter.validate_job.return_value = {
        "job_id": EXAMPLE_JOB_ID,
        "valid": False,
        "errors": [
            {"message": "invalid", "path": ["ops"], "reason": "MISSING_REQUIRED_FIELD"}
        ],
    }
    response = client.post(
        f"/api/v1/jobs/{EXAMPLE_JOB_ID}/validate", headers=AUTH, json={"run_config": {}}
    )
    assert response.status_code == 200
    assert response.json()["valid"] is False
    adapter.validate_job.assert_called_once_with(
        EXAMPLE_JOB_ID, run_config={}, op_selection=None, asset_selection=None
    )


def test_preflight_rejects_unknown_fields(resource_api):
    client, adapter = resource_api
    response = client.post(
        f"/api/v1/jobs/{EXAMPLE_JOB_ID}/validate", headers=AUTH, json={"run_confg": {}}
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
        job_id=None,
        tags={"source": "webhook", "value": "a=b"},
        cursor=None,
        limit=1,
    )


@pytest.mark.parametrize(
    "query",
    [
        "limit=0",
        "limit=201",
        "statuses=WRONG",
        "tag=broken",
        "tag==value",
        "tag=a=b&tag=a=c",
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


def test_new_resources_document_security_models_examples_and_errors(resource_api):
    client, _ = resource_api
    schema = client.get("/openapi.json").json()
    for methods in schema["paths"].values():
        for operation in methods.values():
            assert (
                operation["summary"]
                and operation["description"]
                and operation["operationId"]
            )
            assert operation["tags"] and operation["security"]
            assert {"401", "404", "422", "502"} <= operation["responses"].keys()
            assert operation["responses"]["200"]["content"]["application/json"][
                "example"
            ]
            assert operation["responses"]["200"]["content"]["application/json"][
                "schema"
            ]
            for parameter in operation.get("parameters", []):
                assert parameter.get("description")


def test_exact_job_id_filter_is_forwarded(resource_api):
    client, adapter = resource_api
    adapter.list_runs.return_value = {
        "items": [],
        "next_cursor": None,
        "has_more": False,
    }
    response = client.get(
        "/api/v1/runs", params={"job_id": EXAMPLE_JOB_ID}, headers=AUTH
    )
    assert response.status_code == 200
    assert adapter.list_runs.call_args.kwargs["job_id"] == EXAMPLE_JOB_ID


@pytest.mark.parametrize(
    "extra", [{"job_name": "wrong"}, {"tag": ".dagster/repository=wrong@place"}]
)
def test_conflicting_job_id_filters_return_422(resource_api, extra):
    client, adapter = resource_api
    response = client.get(
        "/api/v1/runs", params={"job_id": EXAMPLE_JOB_ID, **extra}, headers=AUTH
    )
    assert response.status_code == 422
    adapter.list_runs.assert_not_called()


@pytest.mark.parametrize("non_finite", [float("nan"), float("inf"), float("-inf")])
def test_preflight_rejects_non_finite_json_numbers(non_finite):
    from pydantic import ValidationError

    from gateway.discovery import ValidationRequest

    with pytest.raises(ValidationError):
        ValidationRequest.model_validate({"run_config": {"nested": [non_finite]}})
