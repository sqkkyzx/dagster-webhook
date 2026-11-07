"""HTTP contracts for durable submissions, with only the adapter mocked."""

import sqlite3
from unittest.mock import Mock

import pytest
from dagster_graphql import DagsterGraphQLClientError

import main as gateway
from gateway.dagster_adapter import DagsterAdapter, encode_job_id
from gateway.idempotency import IdempotencyStore
from gateway.security import APIKey

JOB_ID = encode_job_id("example_location", "example_repository", "example_job")
PAYLOAD = {
    "job_id": JOB_ID,
    "run_config": {"ops": {"extract": {"config": {"limit": 5}}}},
}


@pytest.fixture
def operation_adapter(app):
    adapter = Mock(spec=DagsterAdapter)
    adapter.submit_run.side_effect = lambda _payload, run_id: {
        "run_id": run_id,
        "status": "QUEUED",
    }
    app.dependency_overrides[gateway.get_adapter] = lambda: adapter
    return adapter


@pytest.fixture
def operation_store(app, tmp_path):
    store = IdempotencyStore(tmp_path / "operations.sqlite3")
    app.dependency_overrides[gateway.get_idempotency_store] = lambda: store
    return store


@pytest.fixture
def submission_headers(auth_headers):
    return {**auth_headers, "Idempotency-Key": "external-event-123"}


def test_first_submission_creates_and_replay_returns_same_run(
    client, operation_adapter, operation_store, submission_headers
):
    first = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    assert first.status_code == 201
    created = first.json()
    assert created["replayed"] is False
    assert created["status"] == "QUEUED"
    assert created["job_id"] == JOB_ID
    assert first.headers["location"] == created["status_url"]
    assert first.headers["idempotency-replayed"] == "false"
    assert first.headers["x-request-id"]
    replay = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    assert replay.status_code == 200
    body = replay.json()
    assert body["replayed"] is True
    assert body["run_id"] == created["run_id"]
    assert body["submission_id"] == created["submission_id"]
    assert replay.headers["idempotency-replayed"] == "true"
    operation_adapter.submit_run.assert_called_once()
    forwarded, assigned_run_id = operation_adapter.submit_run.call_args.args
    assert assigned_run_id == created["run_id"]
    assert forwarded["run_config"] == PAYLOAD["run_config"]
    operation_adapter.find_submission_run.assert_not_called()


def test_reusing_key_with_changed_payload_returns_structured_conflict(
    client, operation_adapter, operation_store, submission_headers
):
    first = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    changed = {**PAYLOAD, "run_config": {}}
    response = client.post("/api/v1/runs", json=changed, headers=submission_headers)
    assert first.status_code == 201
    assert response.status_code == 409
    body = response.json()
    assert body["code"] == "IDEMPOTENCY_PAYLOAD_MISMATCH"
    assert body["retryable"] is False
    assert body["request_id"] == response.headers["x-request-id"]
    assert body["details"][0]["run_id"] == first.json()["run_id"]
    operation_adapter.submit_run.assert_called_once()


def test_same_external_key_is_independent_for_each_principal(
    client, operation_adapter, operation_store, monkeypatch
):
    monkeypatch.setattr(
        gateway,
        "API_KEYS",
        (
            APIKey("system-a", "system-a-token", frozenset({"submit"})),
            APIKey("system-b", "system-b-token", frozenset({"submit"})),
        ),
    )
    monkeypatch.setattr(gateway, "LEGACY_TOKEN_ENABLED", False)
    responses = [
        client.post(
            "/api/v1/runs",
            json=PAYLOAD,
            headers={
                "Authorization": "Bearer " + token,
                "Idempotency-Key": "same-event",
            },
        )
        for token in ("system-a-token", "system-b-token")
    ]
    assert [response.status_code for response in responses] == [201, 201]
    assert responses[0].json()["run_id"] != responses[1].json()["run_id"]
    assert operation_adapter.submit_run.call_count == 2


def test_explicit_configuration_rejection_remains_422_on_replay(
    client, operation_adapter, operation_store, submission_headers
):
    operation_adapter.submit_run.side_effect = DagsterGraphQLClientError(
        "RunConfigValidationInvalid", "configuration does not match the Job"
    )
    first = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    replay = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    assert first.status_code == replay.status_code == 422
    assert (
        first.json()["error_type"]
        == replay.json()["error_type"]
        == "RunConfigValidationInvalid"
    )
    assert first.json()["retryable"] is False
    assert replay.json()["request_id"] == replay.headers["x-request-id"]
    operation_adapter.submit_run.assert_called_once()
    operation_adapter.find_submission_run.assert_not_called()


def test_lost_submission_response_recovers_existing_run_without_second_create(
    client, operation_adapter, operation_store, submission_headers
):
    created = {}

    def lost_response(_payload, run_id):
        created[run_id] = {"run_id": run_id, "status": "STARTED", "job_id": JOB_ID}
        raise DagsterGraphQLClientError("Exception occurred during execution of query")

    operation_adapter.submit_run.side_effect = lost_response
    operation_adapter.find_submission_run.side_effect = created.get
    first = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    assert first.status_code == 409
    assert first.json()["code"] == "SUBMISSION_OUTCOME_UNKNOWN"
    assert first.json()["details"][0]["run_id"] is None
    assert first.json()["details"][0]["submission_id"] in created
    assert first.json()["details"][0]["submission_id"]
    assert first.json()["retryable"] is False
    replay = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    assert replay.status_code == 200
    assert replay.json()["replayed"] is True
    assert replay.json()["status"] == "STARTED"
    assert replay.json()["run_id"] in created
    operation_adapter.submit_run.assert_called_once()
    operation_adapter.find_submission_run.assert_called_once_with(
        replay.json()["run_id"]
    )


def test_unresolved_submission_returns_409_without_resubmitting(
    client, operation_adapter, operation_store, submission_headers
):
    operation_adapter.submit_run.side_effect = DagsterGraphQLClientError(
        "Upstream response lost"
    )
    operation_adapter.find_submission_run.side_effect = DagsterGraphQLClientError(
        "RunNotFoundError", "Not found"
    )
    first = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    assert first.status_code == 409
    assert first.json()["code"] == "SUBMISSION_OUTCOME_UNKNOWN"
    for _attempt in range(2):
        replay = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
        assert replay.status_code == 409
        assert replay.json()["code"] == "SUBMISSION_OUTCOME_UNKNOWN"
        assert replay.json()["retryable"] is False
        assert replay.json()["details"][0]["run_id"] is None
        assert replay.json()["details"][0]["submission_id"]
    operation_adapter.submit_run.assert_called_once()


def test_created_but_not_submitted_run_remains_unknown_until_dagster_accepts(
    client, operation_adapter, operation_store, submission_headers
):
    created = {}

    def response_lost(_payload, run_id):
        created[run_id] = {"run_id": run_id, "status": "NOT_STARTED", "job_id": JOB_ID}
        raise DagsterGraphQLClientError("Submission response lost")

    operation_adapter.submit_run.side_effect = response_lost
    operation_adapter.find_submission_run.side_effect = created.get
    first = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    assert first.status_code == 409
    incomplete = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    assert incomplete.status_code == 409
    assert incomplete.json()["code"] == "SUBMISSION_OUTCOME_UNKNOWN"
    run_id = next(iter(created))
    assert incomplete.json()["details"][0]["run_id"] == run_id
    operation_adapter.submit_run.assert_called_once()

    created[run_id]["status"] = "QUEUED"
    recovered = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    assert recovered.status_code == 200
    assert recovered.json()["run_id"] == run_id
    assert recovered.json()["status"] == "QUEUED"
    assert recovered.json()["replayed"] is True
    operation_adapter.submit_run.assert_called_once()


@pytest.mark.parametrize("header", [None, "contains spaces", "slash/value"])
def test_idempotency_key_is_required_and_validated(
    client, operation_adapter, operation_store, auth_headers, header
):
    headers = (
        auth_headers if header is None else {**auth_headers, "Idempotency-Key": header}
    )
    response = client.post("/api/v1/runs", json=PAYLOAD, headers=headers)
    assert response.status_code == 422
    operation_adapter.submit_run.assert_not_called()


def test_misspelled_submission_field_is_rejected_without_creating_run(
    client, operation_adapter, operation_store, submission_headers
):
    response = client.post(
        "/api/v1/runs",
        json={"job_id": JOB_ID, "run_confg": {}},
        headers=submission_headers,
    )
    assert response.status_code == 422
    assert any("run_confg" in issue["loc"] for issue in response.json()["detail"])
    operation_adapter.submit_run.assert_not_called()


def test_invalid_job_identity_is_rejected_before_claiming_key(
    client, operation_adapter, operation_store, submission_headers
):
    bad = client.post(
        "/api/v1/runs", json={"job_id": "not-a-job-id"}, headers=submission_headers
    )
    assert bad.status_code == 422
    good = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    assert good.status_code == 201
    operation_adapter.submit_run.assert_called_once()


def test_mixed_batch_termination_retains_each_result(
    client, operation_adapter, auth_headers
):
    operation_adapter.terminate_runs.return_value = {
        "results": [
            {"run_id": "first", "status": "accepted"},
            {
                "run_id": "second",
                "status": "failed",
                "error_type": "RunNotFoundError",
                "detail": "Not found",
            },
        ],
        "succeeded": 1,
        "failed": 1,
    }
    response = client.post(
        "/api/v1/runs/terminate-batch",
        json={"run_ids": ["first", "second"]},
        headers=auth_headers,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["succeeded"] == body["failed"] == 1
    assert [item["run_id"] for item in body["results"]] == ["first", "second"]
    assert body["results"][1]["error_type"] == "RunNotFoundError"
    operation_adapter.terminate_runs.assert_called_once_with(["first", "second"])


def test_batch_communication_failure_is_not_reported_as_per_item_success(
    client, operation_adapter, auth_headers
):
    operation_adapter.terminate_runs.side_effect = DagsterGraphQLClientError(
        "PythonError", "Unavailable"
    )
    response = client.post(
        "/api/v1/runs/terminate-batch",
        json={"run_ids": ["first"]},
        headers=auth_headers,
    )
    assert response.status_code == 502
    assert response.json()["error_type"] == "PythonError"


@pytest.mark.parametrize(
    "payload",
    [
        {"run_ids": []},
        {"run_ids": ["same", "same"]},
        {"run_ids": ["first"], "unknown": True},
    ],
)
def test_batch_rejects_invalid_inputs(client, operation_adapter, auth_headers, payload):
    response = client.post(
        "/api/v1/runs/terminate-batch", json=payload, headers=auth_headers
    )
    assert response.status_code == 422
    operation_adapter.terminate_runs.assert_not_called()


def test_redoc_documents_required_key_and_create_replay_responses(client):
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/api/v1/runs"]["post"]
    key = next(
        parameter
        for parameter in operation["parameters"]
        if parameter["name"] == "Idempotency-Key"
    )
    assert key["required"] is True
    assert key["description"]
    assert "200" in operation["responses"] and "201" in operation["responses"]
    assert (
        operation["responses"]["201"]["content"]["application/json"]["example"][
            "replayed"
        ]
        is False
    )
    assert operation["security"]


def test_claim_storage_failure_returns_503_before_contacting_dagster(
    client, operation_adapter, operation_store, submission_headers, monkeypatch
):
    monkeypatch.setattr(
        operation_store,
        "submit",
        Mock(side_effect=sqlite3.OperationalError("storage unavailable")),
    )
    response = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    assert response.status_code == 503
    assert response.json()["code"] == "PERSISTENCE_UNAVAILABLE"
    assert response.json()["retryable"] is False
    assert response.json()["request_id"] == response.headers["x-request-id"]
    operation_adapter.submit_run.assert_not_called()
    operation_adapter.find_submission_run.assert_not_called()


def test_response_storage_failure_preserves_unknown_claim_and_recovers_after_lease(
    client, operation_adapter, operation_store, submission_headers, monkeypatch
):
    clock, created = [1000.0], {}
    monkeypatch.setattr(operation_store, "_now", lambda: clock[0])

    def create(_payload, run_id):
        created[run_id] = {"run_id": run_id, "status": "QUEUED", "job_id": JOB_ID}
        return created[run_id]

    def failed_persistence(*_args):
        raise sqlite3.OperationalError("failed to persist response after submission")

    operation_adapter.submit_run.side_effect = create
    operation_adapter.find_submission_run.side_effect = created.get
    with monkeypatch.context() as patcher:
        patcher.setattr(operation_store, "_accepted", failed_persistence)
        first = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    assert first.status_code == 409
    assert first.json()["code"] == "SUBMISSION_OUTCOME_UNKNOWN"
    assert first.json()["retryable"] is False
    assert first.json()["details"][0]["run_id"] is None
    assert first.json()["details"][0]["submission_id"] in created
    assert first.json()["details"][0]["submission_id"]

    clock[0] += 31
    recovered = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    assert recovered.status_code == 200
    assert recovered.json()["replayed"] is True
    assert recovered.json()["run_id"] in created
    assert recovered.json()["status"] == "QUEUED"
    operation_adapter.submit_run.assert_called_once()


def test_non_sdk_failure_after_claim_preserves_ids_and_does_not_resubmit(
    client, operation_adapter, operation_store, submission_headers
):
    created = {}

    def failed_adapter(_payload, run_id):
        created[run_id] = {"run_id": run_id, "status": "QUEUED", "job_id": JOB_ID}
        raise ValueError("unexpected adapter response")

    operation_adapter.submit_run.side_effect = failed_adapter
    operation_adapter.find_submission_run.side_effect = created.get
    first = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    assert first.status_code == 409
    assert first.json()["code"] == "SUBMISSION_OUTCOME_UNKNOWN"
    assert first.json()["details"][0]["run_id"] is None
    assert first.json()["details"][0]["submission_id"] in created
    assert first.json()["details"][0]["submission_id"]
    recovered = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    assert recovered.status_code == 200
    assert recovered.json()["run_id"] in created
    operation_adapter.submit_run.assert_called_once()


def test_reserved_origin_tag_rejected_before_idempotent_claim(
    client, operation_adapter, operation_store, submission_headers
):
    response = client.post(
        "/api/v1/runs",
        headers=submission_headers,
        json={
            **PAYLOAD,
            "tags": {".dagster/repository": "fake@location"},
        },
    )
    assert response.status_code == 422
    assert "tags" in response.json()["detail"][0]["loc"]
    operation_adapter.submit_run.assert_not_called()
    with sqlite3.connect(operation_store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM submissions").fetchone()[0] == 0


def test_submission_get_never_builds_adapter_or_contacts_dagster(
    client, app, operation_adapter, operation_store, submission_headers
):
    first = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers).json()

    def forbidden_adapter():
        raise AssertionError("GET submission must not create an upstream dependency")

    app.dependency_overrides[gateway.get_adapter] = forbidden_adapter
    response = client.get(first["submission_url"], headers=submission_headers)
    assert response.status_code == 200
    assert response.json()["state"] == "ACCEPTED"
    assert response.json()["run"]["run_id"] == first["run_id"]
    operation_adapter.find_submission_run.assert_not_called()


@pytest.mark.parametrize("other_scopes", [frozenset({"submit"}), frozenset({"admin"})])
def test_submission_routes_are_isolated_even_for_admin(
    client, operation_adapter, operation_store, monkeypatch, other_scopes
):
    monkeypatch.setattr(
        gateway,
        "API_KEYS",
        (
            APIKey("owner", "owner-token", frozenset({"submit"})),
            APIKey("other", "other-token", other_scopes),
        ),
    )
    monkeypatch.setattr(gateway, "LEGACY_TOKEN_ENABLED", False)
    owner = {"Authorization": "Bearer owner-token", "Idempotency-Key": "shared-key"}
    other = {"Authorization": "Bearer other-token"}
    first = client.post("/api/v1/runs", json=PAYLOAD, headers=owner).json()
    url = first["submission_url"]
    for method, path in ((client.get, url), (client.post, url + "/reconcile")):
        assert method(path, headers=other).status_code == 404
        assert method(path, headers=owner).status_code == 200
    operation_adapter.find_submission_run.assert_not_called()
    operation_adapter.submit_run.assert_called_once()


@pytest.mark.parametrize(
    "outcome", ["NOT_FOUND", "NOT_STARTED", "UPSTREAM_ERROR", "ACCEPTED"]
)
def test_http_reconcile_returns_documented_state_without_resubmitting(
    client, operation_adapter, operation_store, submission_headers, outcome
):
    from gateway.operations import SubmissionResponse

    operation_adapter.submit_run.side_effect = DagsterGraphQLClientError(
        "response lost"
    )
    first = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    identifiers = first.json()["details"][0]
    run_id = "real-dagster-run"
    if outcome == "NOT_FOUND":
        operation_adapter.find_submission_run.side_effect = DagsterGraphQLClientError(
            "RunNotFoundError", "missing"
        )
    elif outcome == "UPSTREAM_ERROR":
        operation_adapter.find_submission_run.side_effect = DagsterGraphQLClientError(
            "transport error"
        )
    else:
        operation_adapter.find_submission_run.return_value = {
            "run_id": run_id,
            "status": "QUEUED" if outcome == "ACCEPTED" else outcome,
            "job_id": JOB_ID,
        }
    url = f"/api/v1/submissions/{identifiers['submission_id']}"
    before = client.get(url, headers=submission_headers)
    assert before.json()["state"] == "UNKNOWN"
    operation_adapter.find_submission_run.assert_not_called()
    response = client.post(url + "/reconcile", headers=submission_headers)
    assert response.status_code == 200
    body = response.json()
    SubmissionResponse.model_validate(body)
    assert body["state"] == ("ACCEPTED" if outcome == "ACCEPTED" else "UNKNOWN")
    assert body["last_check_outcome"] == outcome
    assert body["last_checked_at"]
    assert body["run_id"] == (
        run_id if outcome in {"ACCEPTED", "NOT_STARTED"} else None
    )
    operation_adapter.submit_run.assert_called_once()
    operation_adapter.find_submission_run.assert_called_once_with(
        identifiers["submission_id"]
    )


@pytest.mark.parametrize("reconcile", [False, True])
def test_http_recovery_disk_failure_retains_409_and_ids(
    client,
    operation_adapter,
    operation_store,
    submission_headers,
    monkeypatch,
    reconcile,
):
    operation_adapter.submit_run.side_effect = DagsterGraphQLClientError(
        "response lost"
    )
    operation_adapter.find_submission_run.side_effect = DagsterGraphQLClientError(
        "RunNotFoundError", "missing"
    )
    initial = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    identifiers = initial.json()["details"][0]
    monkeypatch.setattr(
        operation_store,
        "_unknown",
        Mock(side_effect=sqlite3.OperationalError("disk full")),
    )
    if reconcile:
        response = client.post(
            f"/api/v1/submissions/{identifiers['submission_id']}/reconcile",
            headers=submission_headers,
        )
    else:
        response = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    assert response.status_code == 409
    assert response.json()["code"] == "SUBMISSION_OUTCOME_UNKNOWN"
    assert response.json()["details"][0]["run_id"] == identifiers["run_id"]
    assert (
        response.json()["details"][0]["submission_id"] == identifiers["submission_id"]
    )
    operation_adapter.submit_run.assert_called_once()


def test_submission_readers_cannot_query_or_reconcile_without_submit_scope(
    client, operation_adapter, operation_store, submission_headers, monkeypatch
):
    first = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers).json()
    monkeypatch.setattr(
        gateway, "API_KEYS", (APIKey("reader", "reader-token", frozenset({"read"})),)
    )
    for method, path in (
        (client.get, first["submission_url"]),
        (client.post, first["submission_url"] + "/reconcile"),
    ):
        response = method(path, headers={"Authorization": "Bearer reader-token"})
        assert response.status_code == 403
    operation_adapter.find_submission_run.assert_not_called()


def test_rejected_submission_ids_allow_query_and_reconcile_without_lookup(
    client, operation_adapter, operation_store, submission_headers
):
    operation_adapter.submit_run.side_effect = DagsterGraphQLClientError(
        "RunConfigValidationInvalid", "invalid"
    )
    first = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    assert first.status_code == 422
    ids = next(item for item in first.json()["details"] if "submission_id" in item)
    url = f"/api/v1/submissions/{ids['submission_id']}"
    for method, path in ((client.get, url), (client.post, url + "/reconcile")):
        response = method(path, headers=submission_headers)
        assert response.status_code == 200
        assert response.json()["state"] == "REJECTED"
        assert response.json()["error_type"] == "RunConfigValidationInvalid"
    replay = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    replay_ids = next(
        item for item in replay.json()["details"] if "submission_id" in item
    )
    assert replay_ids["submission_id"] == ids["submission_id"]
    operation_adapter.find_submission_run.assert_not_called()
    operation_adapter.submit_run.assert_called_once()


def test_reconcile_http_active_lease_and_local_expiry_contract(
    client, operation_adapter, operation_store, submission_headers, monkeypatch
):
    now = [1000.0]
    monkeypatch.setattr(operation_store, "_now", lambda: now[0])
    operation_adapter.submit_run.side_effect = DagsterGraphQLClientError(
        "response lost"
    )
    first = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    ids = first.json()["details"][0]
    url = f"/api/v1/submissions/{ids['submission_id']}"
    with sqlite3.connect(operation_store.path) as connection:
        connection.execute(
            "UPDATE submissions SET state = 'REQUESTING', lease_expires_at = 1010"
        )
    response = client.post(url + "/reconcile", headers=submission_headers)
    assert response.status_code == 409
    assert response.headers["retry-after"] == "10"
    assert response.json()["code"] == "IDEMPOTENCY_IN_PROGRESS"
    operation_adapter.find_submission_run.assert_not_called()
    now[0] = 1010
    assert client.get(url, headers=submission_headers).json()["state"] == "UNKNOWN"
    operation_adapter.find_submission_run.return_value = {
        "run_id": "real-dagster-run",
        "status": "QUEUED",
        "job_id": JOB_ID,
    }
    assert (
        client.post(url + "/reconcile", headers=submission_headers).json()["state"]
        == "ACCEPTED"
    )
    now[0] += operation_store.retention_seconds
    assert client.get(url, headers=submission_headers).status_code == 404
    assert (
        client.post(url + "/reconcile", headers=submission_headers).status_code == 404
    )
    operation_adapter.find_submission_run.assert_called_once()


def test_http_marker_match_to_other_job_stays_unknown(
    client, operation_adapter, operation_store, submission_headers
):
    operation_adapter.submit_run.side_effect = DagsterGraphQLClientError(
        "response lost"
    )
    first = client.post("/api/v1/runs", json=PAYLOAD, headers=submission_headers)
    ids = first.json()["details"][0]
    assert ids["run_id"] is None
    operation_adapter.find_submission_run.return_value = {
        "run_id": "unrelated",
        "status": "SUCCESS",
        "job_id": encode_job_id("other", "repo", "job"),
    }
    response = client.post(
        f"/api/v1/submissions/{ids['submission_id']}/reconcile",
        headers=submission_headers,
    )
    assert response.status_code == 200
    assert response.json()["state"] == "UNKNOWN"
    assert response.json()["last_check_outcome"] == "MISMATCH"
    assert response.json()["run_id"] is response.json()["status_url"] is None
    operation_adapter.submit_run.assert_called_once()


def test_submission_marker_cannot_be_supplied_by_caller(
    client, operation_adapter, operation_store, submission_headers
):
    response = client.post(
        "/api/v1/runs",
        headers=submission_headers,
        json={
            **PAYLOAD,
            "tags": {"dagster-webhook/submission_id": "someone-else"},
        },
    )
    assert response.status_code == 422
    operation_adapter.submit_run.assert_not_called()
    with sqlite3.connect(operation_store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM submissions").fetchone()[0] == 0
