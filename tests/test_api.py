import pytest
from dagster import DagsterRunStatus
from dagster_graphql import DagsterGraphQLClientError
from dagster_graphql.client.utils import (
    ReloadRepositoryLocationInfo,
    ReloadRepositoryLocationStatus,
    ShutdownRepositoryLocationInfo,
    ShutdownRepositoryLocationStatus,
)

BUSINESS_REQUESTS = [
    ("POST", "/api/v1/submit_job", {"job_name": "example_job"}),
    ("GET", "/api/v1/get_run_status?run_id=example-run", None),
    ("POST", "/api/v1/terminate_run", {"run_id": "example-run"}),
    ("POST", "/api/v1/terminate_runs", {"run_ids": ["example-run"]}),
    ("POST", "/api/v1/reload_location", {"location_name": "example_location"}),
    ("POST", "/api/v1/shutdown_location", {"location_name": "example_location"}),
]


@pytest.mark.parametrize("method,path,payload", BUSINESS_REQUESTS)
@pytest.mark.parametrize("authorization", [None, "Bearer wrong-token", "Basic invalid"])
def test_business_routes_require_bearer_token(
    client, method, path, payload, authorization
):
    headers = {} if authorization is None else {"Authorization": authorization}
    response = client.request(method, path, json=payload, headers=headers)
    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"


def test_submit_forwards_configuration_and_selections(client, sdk_client, auth_headers):
    payload = {
        "job_name": "example_job",
        "repository_location_name": "example_location",
        "repository_name": "example_repository",
        "run_config": {"ops": {"example_op": {"config": {"value": 7}}}},
        "tags": {"source": "webhook", "attempt": 1},
        "op_selection": ["example_op"],
        "asset_selection": ["example_asset", ["namespace", "other_asset"]],
    }
    sdk_client._execute.return_value["launchRun"]["run"]["runId"] = "submitted-run"
    response = client.post("/api/v1/submit_job", json=payload, headers=auth_headers)
    assert response.status_code == 200
    assert response.json()["run_id"] == "submitted-run"
    sdk_client._execute.assert_called_once()
    params = sdk_client._execute.call_args.args[1]["params"]
    assert params["selector"] == {
        "repositoryLocationName": "example_location",
        "repositoryName": "example_repository",
        "pipelineName": "example_job",
        "solidSelection": ["example_op"],
        "assetSelection": [
            {"path": ["example_asset"]},
            {"path": ["namespace", "other_asset"]},
        ],
    }
    assert params["runConfigData"] == payload["run_config"]
    assert params["executionMetadata"] == {
        "tags": [
            {"key": "source", "value": "webhook"},
            {"key": "attempt", "value": "1"},
        ]
    }


def test_run_status_serializes_enum(client, sdk_client, auth_headers):
    sdk_client.get_run_status.return_value = DagsterRunStatus.SUCCESS
    response = client.get(
        "/api/v1/get_run_status",
        params={"run_id": "finished-run"},
        headers=auth_headers,
    )
    assert response.status_code == 200
    assert response.json() == {"run_id": "finished-run", "status": "SUCCESS"}
    sdk_client.get_run_status.assert_called_once_with("finished-run")


@pytest.mark.parametrize(
    "route,payload,method,argument",
    [
        ("terminate_run", {"run_id": "running-run"}, "terminate_run", "running-run"),
        (
            "terminate_runs",
            {"run_ids": ["first", "second"]},
            "terminate_runs",
            ["first", "second"],
        ),
    ],
)
def test_termination_calls_sdk(
    client, sdk_client, auth_headers, route, payload, method, argument
):
    response = client.post("/api/v1/" + route, json=payload, headers=auth_headers)
    assert response.status_code == 200
    getattr(sdk_client, method).assert_called_once_with(argument)


@pytest.mark.parametrize(
    "route,method,result,expected_status,error_type",
    [
        (
            "reload_location",
            "reload_repository_location",
            ReloadRepositoryLocationInfo(
                status=ReloadRepositoryLocationStatus.FAILURE,
                failure_type="RepositoryLocationNotFound",
                message="Location not found",
            ),
            404,
            "RepositoryLocationNotFound",
        ),
        (
            "reload_location",
            "reload_repository_location",
            ReloadRepositoryLocationInfo(
                status=ReloadRepositoryLocationStatus.FAILURE,
                failure_type="ReloadNotSupported",
                message="Reload not supported",
            ),
            409,
            "ReloadNotSupported",
        ),
        (
            "reload_location",
            "reload_repository_location",
            ReloadRepositoryLocationInfo(
                status=ReloadRepositoryLocationStatus.FAILURE,
                failure_type="PythonError",
                message="Failed to load code",
            ),
            502,
            "PythonError",
        ),
        (
            "shutdown_location",
            "shutdown_repository_location",
            ShutdownRepositoryLocationInfo(
                status=ShutdownRepositoryLocationStatus.FAILURE,
                message="Shutdown failed",
            ),
            502,
            "PythonError",
        ),
    ],
)
def test_location_failures_are_not_reported_as_success(
    client, sdk_client, auth_headers, route, method, result, expected_status, error_type
):
    getattr(sdk_client, method).return_value = result
    response = client.post(
        "/api/v1/" + route,
        json={"location_name": "example_location"},
        headers=auth_headers,
    )
    assert response.status_code == expected_status
    body = response.json()
    assert body["message"] == "Dagster Client Error"
    assert body["error_type"] == error_type
    assert result.message in body["detail"]
    assert body["location_name"] == "example_location"
    assert body["status"] == "FAILURE"
    assert body["failure_type"] == error_type


@pytest.mark.parametrize(
    "route,method,result",
    [
        (
            "reload_location",
            "reload_repository_location",
            ReloadRepositoryLocationInfo(status=ReloadRepositoryLocationStatus.SUCCESS),
        ),
        (
            "shutdown_location",
            "shutdown_repository_location",
            ShutdownRepositoryLocationInfo(
                status=ShutdownRepositoryLocationStatus.SUCCESS
            ),
        ),
    ],
)
def test_successful_location_operations(
    client, sdk_client, auth_headers, route, method, result
):
    getattr(sdk_client, method).return_value = result
    response = client.post(
        "/api/v1/" + route,
        json={"location_name": "example_location"},
        headers=auth_headers,
    )
    assert response.status_code == 200
    assert response.json()["status"] == "SUCCESS"
    assert response.json()["failure_type"] is None
    getattr(sdk_client, method).assert_called_once_with("example_location")


@pytest.mark.parametrize(
    "route,method",
    [
        ("reload_location", "reload_repository_location"),
        ("shutdown_location", "shutdown_repository_location"),
    ],
)
def test_location_transport_failure_retains_unknown_outcome(
    client, sdk_client, auth_headers, route, method
):
    getattr(sdk_client, method).side_effect = DagsterGraphQLClientError(
        "Connection timed out"
    )
    response = client.post(
        "/api/v1/" + route, headers=auth_headers, json={"location_name": "my_code"}
    )
    assert response.status_code == 502
    body = response.json()
    assert body["location_name"] == "my_code"
    assert body["status"] == "UNKNOWN"
    assert body["failure_type"] == "DagsterGraphQLClientError"
    assert body["retryable"] is False


@pytest.mark.parametrize(
    "error_type,status_code",
    [
        ("RunNotFoundError", 404),
        ("JobNotFoundError", 404),
        ("PipelineNotFoundError", 404),
        ("RunConfigValidationInvalid", 422),
        ("PipelineConfigValidationInvalid", 422),
        ("InvalidStepError", 422),
        ("InvalidOutputError", 422),
        ("RunConflict", 409),
        ("TerminateRunFailure", 409),
        ("TerminateRunsError", 409),
        ("UnauthorizedError", 403),
        ("PythonError", 502),
        ("UnrecognizedUpstreamError", 502),
    ],
)
def test_dagster_errors_have_actionable_http_status(
    client, sdk_client, auth_headers, error_type, status_code
):
    sdk_client.get_run_status.side_effect = DagsterGraphQLClientError(
        error_type, "example failure"
    )
    response = client.get(
        "/api/v1/get_run_status?run_id=example-run", headers=auth_headers
    )
    assert response.status_code == status_code
    assert response.json()["error_type"] == error_type
    assert "example failure" in response.json()["detail"]


def test_invalid_submit_payload_returns_validation_error(
    client, sdk_client, auth_headers
):
    response = client.post(
        "/api/v1/submit_job", json={"job_name": ""}, headers=auth_headers
    )
    assert response.status_code == 422
    sdk_client.submit_job_execution.assert_not_called()
    sdk_client._execute.assert_not_called()


def test_batch_validation_matches_documented_limit(client, sdk_client, auth_headers):
    response = client.post(
        "/api/v1/terminate_runs", json={"run_ids": []}, headers=auth_headers
    )
    assert response.status_code == 422
    schema = client.get("/openapi.json").json()
    limit = schema["components"]["schemas"]["TerminateRunsRequest"]["properties"][
        "run_ids"
    ]["maxItems"]
    response = client.post(
        "/api/v1/terminate_runs",
        json={"run_ids": ["run"] * (limit + 1)},
        headers=auth_headers,
    )
    assert response.status_code == 422
    sdk_client.terminate_runs.assert_not_called()
