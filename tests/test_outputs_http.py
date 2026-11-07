"""Simple output query HTTP contracts use only a mocked read adapter."""

from unittest.mock import Mock

import pytest
from dagster_graphql import DagsterGraphQLClientError

import main as gateway
from gateway.security import APIKey


@pytest.fixture
def outputs_adapter(app):
    adapter = Mock()
    adapter.get_run_outputs.return_value = {
        "run_id": "example-run",
        "run_status": "SUCCESS",
        "items": [
            {
                "step_key": "example_op",
                "status": "SUCCESS",
                "outputs": [{"output_name": "result", "output_value": None}],
            }
        ],
        "next_cursor": "tail-cursor",
        "has_more": False,
        "complete": True,
    }
    app.dependency_overrides[gateway.get_adapter] = lambda: adapter
    return adapter


def test_outputs_are_readable_with_read_scope_and_forward_query_parameters(
    client, outputs_adapter, monkeypatch
):
    monkeypatch.setattr(
        gateway, "API_KEYS", (APIKey("reader", "output-reader", frozenset({"read"})),)
    )
    response = client.get(
        "/api/v1/runs/example-run/outputs?cursor=old-cursor&max_events=8",
        headers={"Authorization": "Bearer output-reader"},
    )
    assert response.status_code == 200
    outputs_adapter.get_run_outputs.assert_called_once_with(
        "example-run", cursor="old-cursor", max_events=8
    )
    body = response.json()
    assert body["items"][0] == {
        "step_key": "example_op",
        "status": "SUCCESS",
        "outputs": [{"output_name": "result", "output_value": None}],
    }
    assert set(body) == {
        "run_id",
        "run_status",
        "items",
        "next_cursor",
        "has_more",
        "complete",
    }
    assert response.headers["X-Request-ID"]


def test_outputs_require_read_permission(client, outputs_adapter, monkeypatch):
    missing = client.get("/api/v1/runs/example-run/outputs")
    assert missing.status_code == 401
    monkeypatch.setattr(
        gateway,
        "API_KEYS",
        (APIKey("submitter", "output-submit-only", frozenset({"submit"})),),
    )
    forbidden = client.get(
        "/api/v1/runs/example-run/outputs",
        headers={"Authorization": "Bearer output-submit-only"},
    )
    assert forbidden.status_code == 403
    assert forbidden.json()["code"] == "FORBIDDEN"
    outputs_adapter.get_run_outputs.assert_not_called()


@pytest.mark.parametrize(
    "query", ["max_events=0", "max_events=20001", "cursor=", "max_events=not-int"]
)
def test_invalid_output_query_is_rejected(client, outputs_adapter, auth_headers, query):
    response = client.get(
        "/api/v1/runs/example-run/outputs?" + query, headers=auth_headers
    )
    assert response.status_code == 422
    outputs_adapter.get_run_outputs.assert_not_called()


def test_missing_output_run_retains_standard_error_contract(
    client, outputs_adapter, auth_headers
):
    outputs_adapter.get_run_outputs.side_effect = DagsterGraphQLClientError(
        "RunNotFoundError", "No such run"
    )
    response = client.get("/api/v1/runs/missing-run/outputs", headers=auth_headers)
    assert response.status_code == 404
    assert response.json()["code"] == "RESOURCE_NOT_FOUND"
    assert response.json()["request_id"] == response.headers["X-Request-ID"]


def test_output_openapi_describes_mounted_files_and_simple_values(client):
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/api/v1/runs/{run_id}/outputs"]["get"]
    assert operation["x-required-scopes"] == ["read"]
    parameters = {parameter["name"] for parameter in operation["parameters"]}
    assert {"run_id", "cursor", "max_events"} <= parameters
    assert "value_metadata_key" not in parameters
    assert "HANDLED_OUTPUT" in operation["description"]
    assert "RUN_OUTPUT_STORAGE_ROOT" in operation["description"]
    assert "complete" in operation["description"]
    output_model = schema["components"]["schemas"]["RunOutputValue"]
    assert set(output_model["properties"]) == {"output_name", "output_value"}
    media = operation["responses"]["200"]["content"]["application/json"]
    assert {"readable_value", "not_started", "unreadable_value", "partial"} <= media[
        "examples"
    ].keys()
    assert (
        media["examples"]["unreadable_value"]["value"]["items"][0]["outputs"][0][
            "output_value"
        ]
        is None
    )
    assert media["examples"]["partial"]["value"]["complete"] is False
