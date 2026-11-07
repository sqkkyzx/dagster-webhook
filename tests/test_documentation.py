from datetime import datetime

import pytest

from project_metadata import DAGSTER_VERSION, VERSION


def test_health_and_openapi_share_project_version(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["version"] == VERSION
    datetime.fromisoformat(response.json()["timestamp"])
    schema = client.get("/openapi.json").json()
    assert schema["info"]["version"] == VERSION
    assert VERSION.startswith(DAGSTER_VERSION + ".")


def test_redoc_and_swagger_are_available_without_credentials(client):
    for path in ("/redoc", "/docs"):
        response = client.get(path)
        assert response.status_code == 200
        assert "/openapi.json" in response.text


def test_openapi_documents_security_responses_and_operations(client):
    schema = client.get("/openapi.json").json()
    assert schema["info"]["description"]
    assert all(tag.get("description") for tag in schema["tags"])
    schemes = schema["components"]["securitySchemes"]
    assert any(
        scheme.get("type") == "http"
        and scheme.get("scheme") == "bearer"
        and scheme.get("description")
        for scheme in schemes.values()
    )
    operation_ids = []
    for path, methods in schema["paths"].items():
        for operation in methods.values():
            assert operation["summary"]
            assert operation["description"]
            assert operation["tags"]
            operation_ids.append(operation["operationId"])
            assert any(
                "content" in response
                for code, response in operation["responses"].items()
                if code in {"200", "201"}
            )
            if path.startswith("/api/v1/"):
                assert operation["security"]
                assert "401" in operation["responses"]
                assert "502" in operation["responses"]
    assert len(operation_ids) == len(set(operation_ids))
    assert schema["paths"]["/api/v1/shutdown_location"]["post"]["deprecated"] is True


def test_submit_model_documents_all_fields_and_examples(client):
    schema = client.get("/openapi.json").json()
    model = schema["components"]["schemas"]["SubmitJobRequest"]
    for property_schema in model["properties"].values():
        assert property_schema.get("description")
    request_content = schema["paths"]["/api/v1/submit_job"]["post"]["requestBody"][
        "content"
    ]["application/json"]
    assert (
        model.get("examples")
        or request_content.get("examples")
        or request_content.get("example")
    )


def test_new_endpoints_document_required_scopes_and_idempotency(client):
    schema = client.get("/openapi.json").json()
    for path, methods in schema["paths"].items():
        for operation in methods.values():
            if path.startswith("/api/"):
                assert operation["x-required-scopes"]
                assert "所需权限" in operation["description"]
    creation = schema["paths"]["/api/v1/runs"]["post"]
    key = next(
        parameter
        for parameter in creation["parameters"]
        if parameter["name"] == "Idempotency-Key"
    )
    assert key["required"] is False
    body = schema["components"]["schemas"]["IdempotentRunSubmissionRequest"]
    assert "idempotency_key" in body["properties"]
    assert "idempotency_key" not in body["required"]
    assert "至少" in key["description"]
    for status in ("200", "201"):
        assert {"Location", "Idempotency-Replayed"} <= creation["responses"][status][
            "headers"
        ].keys()


def test_documented_response_examples_match_their_models(client):
    from fastapi.routing import APIRoute
    from pydantic import TypeAdapter

    import main

    for route in [
        *main.app.routes,
        *main.discovery_router.routes,
        *main.operations_router.routes,
    ]:
        if not isinstance(route, APIRoute):
            continue
        for code, response in route.responses.items():
            model = response.get("model") or (
                route.response_model if code in {200, 201} else None
            )
            if model is None:
                continue
            media = response.get("content", {}).get("application/json", {})
            examples = [media["example"]] if "example" in media else []
            examples += [item["value"] for item in media.get("examples", {}).values()]
            for example in examples:
                TypeAdapter(model).validate_python(example)


def validate_actual_response(path, method, response):
    """Validate handler-produced errors, which bypass FastAPI response validation."""
    from fastapi.routing import APIRoute
    from pydantic import TypeAdapter

    import main

    routes = [
        *main.app.routes,
        *main.discovery_router.routes,
        *main.operations_router.routes,
    ]
    route = next(
        route
        for route in routes
        if isinstance(route, APIRoute)
        and route.path == path
        and method in route.methods
    )
    declared = route.responses.get(response.status_code, {})
    model = declared.get("model") or route.response_model
    assert model is not None
    TypeAdapter(model).validate_python(response.json())
    operation = main.app.openapi()["paths"][path][method.lower()]
    assert str(response.status_code) in operation["responses"]
    assert (
        "schema"
        in operation["responses"][str(response.status_code)]["content"][
            "application/json"
        ]
    )
    if response.status_code >= 400:
        assert response.json()["request_id"] == response.headers["X-Request-ID"]


@pytest.mark.parametrize(
    "path,payload",
    [
        ("/api/v1/submit_job", {"job_name": "job"}),
        ("/api/v1/terminate_run", {"run_id": "run"}),
        ("/api/v1/terminate_runs", {"run_ids": ["run"]}),
        ("/api/v1/reload_location", {"location_name": "location"}),
        ("/api/v1/shutdown_location", {"location_name": "location"}),
    ],
)
def test_actual_scope_rejections_match_declared_models(
    client, monkeypatch, path, payload
):
    import main
    from gateway.security import APIKey

    monkeypatch.setattr(
        main, "API_KEYS", (APIKey("reader", "reader-token", frozenset({"read"})),)
    )
    response = client.post(
        path, json=payload, headers={"Authorization": "Bearer reader-token"}
    )
    assert response.status_code == 403
    validate_actual_response(path, "POST", response)


@pytest.mark.parametrize(
    "error_type,status", [("RunNotFoundError", 404), ("PythonError", 502)]
)
def test_actual_upstream_errors_match_declared_models(
    client, sdk_client, auth_headers, error_type, status
):
    from dagster_graphql import DagsterGraphQLClientError

    sdk_client.get_run_status.side_effect = DagsterGraphQLClientError(
        error_type, "test failure"
    )
    response = client.get("/api/v1/get_run_status?run_id=run", headers=auth_headers)
    assert response.status_code == status
    validate_actual_response("/api/v1/get_run_status", "GET", response)


def test_actual_validation_auth_and_internal_errors_match_models(
    client, sdk_client, auth_headers
):
    path = "/api/v1/terminate_run"
    unauthorized = client.post(path, json={"run_id": "run"})
    assert unauthorized.status_code == 401
    validate_actual_response(path, "POST", unauthorized)
    invalid = client.post(
        path, json={"run_id": "run", "unknown": True}, headers=auth_headers
    )
    assert invalid.status_code == 422
    validate_actual_response(path, "POST", invalid)
    sdk_client.terminate_run.side_effect = RuntimeError("offline test failure")
    failure = client.post(path, json={"run_id": "run"}, headers=auth_headers)
    assert failure.status_code == 500
    validate_actual_response(path, "POST", failure)


def test_actual_location_results_match_models(client, sdk_client, auth_headers):
    from dagster_graphql.client.utils import (
        ReloadRepositoryLocationInfo,
        ReloadRepositoryLocationStatus,
    )

    from main import LocationOperationErrorResponse

    sdk_client.reload_repository_location.return_value = ReloadRepositoryLocationInfo(
        status=ReloadRepositoryLocationStatus.FAILURE,
        failure_type="PythonError",
        message="broken definition",
    )
    path = "/api/v1/reload_location"
    response = client.post(
        path, json={"location_name": "offline"}, headers=auth_headers
    )
    assert response.status_code == 502
    validate_actual_response(path, "POST", response)
    LocationOperationErrorResponse.model_validate(response.json())
