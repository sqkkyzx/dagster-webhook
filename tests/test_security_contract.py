import json
from unittest.mock import Mock

import pytest
from dagster_graphql import DagsterGraphQLClientError

import main as gateway
from gateway.security import APIKey, Principal, authenticate, parse_api_keys


def test_named_keys_support_rotation_without_changing_identity():
    first = parse_api_keys(
        json.dumps(
            [{"name": "automation", "token": "first-key", "scopes": ["read", "submit"]}]
        )
    )
    second = parse_api_keys(
        json.dumps(
            [
                {
                    "name": "automation",
                    "token": "second-key",
                    "scopes": ["read", "submit"],
                }
            ]
        )
    )
    assert (
        authenticate("first-key", first, None).name
        == authenticate("second-key", second, None).name
    )
    assert authenticate("first-key", second, None) is None
    assert "first-key" not in repr(first)


@pytest.mark.parametrize(
    "value",
    [
        "[]",
        "{}",
        "not-json",
        '[{"name":"a","token":"a b","scopes":["read"]}]',
        '[{"name":"a","token":"key","scopes":["typo"]}]',
        '[{"name":"legacy","token":"key","scopes":["read"]}]',
        '[{"name":"a","token":"key","scopes":["read"]},{"name":"a","token":"key2","scopes":["read"]}]',
    ],
)
def test_malformed_keys_are_rejected(value):
    with pytest.raises(ValueError, match="APP_API_KEYS_JSON"):
        parse_api_keys(value)


def test_scopes_prevent_reader_from_creating_or_cancelling(client, app, monkeypatch):
    monkeypatch.setattr(
        gateway, "API_KEYS", (APIKey("reader", "reader-token", frozenset({"read"})),)
    )
    adapter = Mock()
    adapter.list_jobs.return_value = {"items": []}
    app.dependency_overrides[gateway.get_adapter] = lambda: adapter
    headers = {"Authorization": "Bearer reader-token"}
    assert client.get("/api/v1/jobs", headers=headers).status_code == 200
    for path, payload in [
        ("/api/v1/submit_job", {"job_name": "job"}),
        ("/api/v1/terminate_run", {"run_id": "run"}),
        ("/api/v1/reload_location", {"location_name": "location"}),
    ]:
        response = client.post(path, json=payload, headers=headers)
        assert response.status_code == 403
        assert response.json()["code"] == "FORBIDDEN"


def test_admin_scope_includes_reads(client, app, monkeypatch):
    monkeypatch.setattr(
        gateway, "API_KEYS", (APIKey("operator", "admin-key", frozenset({"admin"})),)
    )
    adapter = Mock()
    adapter.list_jobs.return_value = {"items": []}
    app.dependency_overrides[gateway.get_adapter] = lambda: adapter
    assert (
        client.get(
            "/api/v1/jobs", headers={"Authorization": "Bearer admin-key"}
        ).status_code
        == 200
    )


def test_legacy_token_can_be_disabled(monkeypatch):
    assert authenticate("legacy-token", (), None) is None
    assert authenticate("legacy-token", (), "legacy-token") == Principal(
        "legacy", frozenset({"read", "submit", "cancel", "admin"})
    )


def test_request_identity_and_legacy_validation(client, auth_headers, sdk_client):
    response = client.post(
        "/api/v1/submit_job",
        headers={**auth_headers, "X-Request-ID": "my-request"},
        json={"job_name": "daily", "run_confg": {}},
    )
    assert response.status_code == 422
    assert (
        response.json()["request_id"]
        == response.headers["X-Request-ID"]
        == "my-request"
    )
    assert response.json()["details"][0]["path"] == ["body", "run_confg"]
    sdk_client.submit_job_execution.assert_not_called()


def test_non_finite_json_is_rejected(client, auth_headers, sdk_client):
    response = client.post(
        "/api/v1/submit_job",
        headers={**auth_headers, "Content-Type": "application/json"},
        content='{"job_name":"daily","run_config":{"value":NaN}}',
    )
    assert response.status_code == 422
    sdk_client.submit_job_execution.assert_not_called()


def test_health_and_readiness_have_different_semantics(client, app):
    probe = Mock()
    app.dependency_overrides[gateway.get_readiness_client] = lambda: probe
    probe._execute.return_value = {"__typename": "DagitQuery"}
    assert client.get("/ready").status_code == 200
    probe._execute.side_effect = DagsterGraphQLClientError("Connection failed")
    response = client.get("/ready")
    assert response.status_code == 503
    assert response.json()["code"] == "NOT_READY"
    assert client.get("/health").status_code == 200


def test_error_envelopes_keep_request_ids(client):
    response = client.get("/api/v1/jobs")
    assert response.status_code == 401
    body = response.json()
    assert body["request_id"] == response.headers["X-Request-ID"]
    assert {"code", "details", "retryable", "detail"} <= body.keys()


def test_dagster_configuration_errors_expose_field_paths(
    client, sdk_client, auth_headers
):
    sdk_client._execute.side_effect = DagsterGraphQLClientError(
        "RunConfigValidationInvalid",
        [
            {
                "path": ["ops", "extract"],
                "message": "Missing required field",
                "reason": "MISSING_REQUIRED_FIELD",
            }
        ],
    )
    response = client.post(
        "/api/v1/submit_job",
        json={"job_name": "daily", "repository_location_name": "my_code"},
        headers=auth_headers,
    )
    assert response.status_code == 422
    assert response.json()["details"] == [
        {
            "path": ["ops", "extract"],
            "message": "Missing required field",
            "code": "MISSING_REQUIRED_FIELD",
        }
    ]
