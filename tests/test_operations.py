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
    "job_name": "example_job",
    "repository_location_name": "example_location",
    "repository_name": "example_repository",
    "run_config": {"ops": {"extract": {"config": {"limit": 5}}}},
}


@pytest.fixture
def operation_adapter(app):
    adapter = Mock(spec=DagsterAdapter)
    adapter.list_jobs.return_value = {
        "items": [{key: value for key, value in PAYLOAD.items() if key != "run_config"}]
    }
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
    assert created["job_name"] == "example_job"
    assert created["repository_location_name"] == "example_location"
    assert created["repository_name"] == "example_repository"
    assert "job_id" not in created
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


@pytest.mark.parametrize("path", ["/api/v1/runs", "/api/v1/runs/submit"])
def test_job_name_alone_infers_custom_repository_without_a_default_restriction(
    client, operation_adapter, operation_store, submission_headers, monkeypatch, path
):
    monkeypatch.setenv("DAGSTER_REPOSITORY_NAME", "unrelated_default")
    operation_adapter.submit_direct_run.return_value = {
        "run_id": "direct-run",
        "status": "QUEUED",
    }
    response = client.post(
        path, json={"job_name": "example_job"}, headers=submission_headers
    )
    assert response.status_code == 201
    body = response.json()
    assert body["repository_name"] == "example_repository"
    assert body["repository_location_name"] == "example_location"
    assert "job_id" not in body
    operation_adapter.list_jobs.assert_called_once_with(
        location_name=None, repository_name=None
    )
    submit = (
        operation_adapter.submit_run
        if path.endswith("/runs")
        else operation_adapter.submit_direct_run
    )
    assert submit.call_args.args[0]["job_id"] == JOB_ID


def test_named_idempotent_replay_never_rediscovers_a_moved_or_unavailable_job(
    client, operation_adapter, operation_store, submission_headers
):
    request = {"job_name": "example_job"}
    first = client.post("/api/v1/runs", json=request, headers=submission_headers)
    assert first.status_code == 201
    operation_adapter.list_jobs.side_effect = AssertionError("Workspace is unavailable")
    replay = client.post("/api/v1/runs", json=request, headers=submission_headers)
    assert replay.status_code == 200
    assert replay.json()["run_id"] == first.json()["run_id"]
    assert replay.json()["repository_name"] == "example_repository"
    operation_adapter.submit_run.assert_called_once()
    operation_adapter.list_jobs.assert_called_once()


def test_ambiguous_name_does_not_launch_or_keep_a_key_and_location_is_enough(
    client, operation_adapter, operation_store, submission_headers
):
    first, second = (
        {
            "job_name": "example_job",
            "repository_location_name": location,
            "repository_name": "custom_repository",
        }
        for location in ("first", "second")
    )
    operation_adapter.list_jobs.return_value = {"items": [first, second]}
    response = client.post(
        "/api/v1/runs", json={"job_name": "example_job"}, headers=submission_headers
    )
    assert response.status_code == 409
    assert response.json()["code"] == "JOB_SELECTION_AMBIGUOUS"
    assert "job_id" not in response.text
    operation_adapter.submit_run.assert_not_called()
    with sqlite3.connect(operation_store.path) as connection:
        assert connection.execute("SELECT count(*) FROM submissions").fetchone()[0] == 0
    selected = client.post(
        "/api/v1/runs",
        json={"job_name": "example_job", "repository_location_name": "second"},
        headers=submission_headers,
    )
    assert selected.status_code == 201
    assert selected.json()["repository_name"] == "custom_repository"
    assert selected.json()["repository_location_name"] == "second"


def test_discovery_failure_is_not_reported_as_an_uncertain_launch(
    client, operation_adapter, operation_store, submission_headers
):
    operation_adapter.list_jobs.side_effect = DagsterGraphQLClientError("upstream down")
    response = client.post(
        "/api/v1/runs", json={"job_name": "example_job"}, headers=submission_headers
    )
    assert response.status_code == 502
    assert response.json()["code"] == "DAGSTER_UNAVAILABLE"
    operation_adapter.submit_run.assert_not_called()
    with sqlite3.connect(operation_store.path) as connection:
        assert connection.execute("SELECT count(*) FROM submissions").fetchone()[0] == 0


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
        json={"job_name": "example_job", "run_confg": {}},
        headers=submission_headers,
    )
    assert response.status_code == 422
    assert any("run_confg" in issue["loc"] for issue in response.json()["detail"])
    operation_adapter.submit_run.assert_not_called()


def test_invalid_job_identity_is_rejected_before_claiming_key(
    client, operation_adapter, operation_store, submission_headers
):
    bad = client.post(
        "/api/v1/runs", json={"job_name": "not-a-job"}, headers=submission_headers
    )
    assert bad.status_code == 404
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


def test_redoc_documents_alternative_keys_and_create_replay_responses(client):
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/api/v1/runs"]["post"]
    key = next(
        parameter
        for parameter in operation["parameters"]
        if parameter["name"] == "Idempotency-Key"
    )
    assert key["required"] is False
    assert "二选一" in key["description"]
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


@pytest.mark.parametrize("header_first", [True, False])
def test_body_and_header_keys_replay_the_same_submission(
    client, operation_adapter, operation_store, auth_headers, header_first
):
    header_request = {
        "json": PAYLOAD,
        "headers": {**auth_headers, "Idempotency-Key": "switch-key"},
    }
    body_request = {
        "json": {**PAYLOAD, "idempotency_key": "switch-key"},
        "headers": auth_headers,
    }
    first_request, replay_request = (
        (header_request, body_request)
        if header_first
        else (body_request, header_request)
    )
    first = client.post("/api/v1/runs", **first_request)
    replay = client.post("/api/v1/runs", **replay_request)
    assert first.status_code == 201
    assert replay.status_code == 200
    assert replay.json()["run_id"] == first.json()["run_id"]
    assert replay.json()["submission_id"] == first.json()["submission_id"]
    operation_adapter.submit_run.assert_called_once()
    assert "idempotency_key" not in operation_adapter.submit_run.call_args.args[0]


def test_same_key_in_both_places_is_accepted_and_cross_source_payload_conflicts(
    client, operation_adapter, operation_store, auth_headers
):
    headers = {**auth_headers, "Idempotency-Key": "same-key"}
    first = client.post(
        "/api/v1/runs", headers=headers, json={**PAYLOAD, "idempotency_key": "same-key"}
    )
    assert first.status_code == 201
    replay = client.post(
        "/api/v1/runs",
        headers=auth_headers,
        json={**PAYLOAD, "idempotency_key": "same-key"},
    )
    assert replay.status_code == 200
    conflict = client.post(
        "/api/v1/runs",
        headers=auth_headers,
        json={**PAYLOAD, "run_config": {}, "idempotency_key": "same-key"},
    )
    assert conflict.status_code == 409
    assert conflict.json()["code"] == "IDEMPOTENCY_PAYLOAD_MISMATCH"
    operation_adapter.submit_run.assert_called_once()


def test_different_header_and_body_keys_are_rejected_before_claim(
    client, operation_adapter, operation_store, auth_headers
):
    response = client.post(
        "/api/v1/runs",
        headers={**auth_headers, "Idempotency-Key": "header-key"},
        json={**PAYLOAD, "idempotency_key": "body-key"},
    )
    assert response.status_code == 422
    assert response.json()["code"] == "IDEMPOTENCY_KEY_MISMATCH"
    operation_adapter.submit_run.assert_not_called()
    with operation_store._connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM submissions").fetchone()[0] == 0


@pytest.mark.parametrize(
    "key", ["", "with spaces", "slash/value", "k" * 201, 123, False, [], {}]
)
def test_invalid_body_key_is_not_hidden_by_valid_header(
    client, operation_adapter, operation_store, auth_headers, key
):
    response = client.post(
        "/api/v1/runs",
        headers={**auth_headers, "Idempotency-Key": "valid-header"},
        json={**PAYLOAD, "idempotency_key": key},
    )
    assert response.status_code == 422
    assert response.json()["details"][0]["path"] == ["body", "idempotency_key"]
    operation_adapter.submit_run.assert_not_called()


@pytest.mark.parametrize("key", ["", "has spaces", "k" * 201])
def test_invalid_header_key_is_not_hidden_by_valid_body(
    client, operation_adapter, operation_store, auth_headers, key
):
    response = client.post(
        "/api/v1/runs",
        headers={**auth_headers, "Idempotency-Key": key},
        json={**PAYLOAD, "idempotency_key": "valid-body"},
    )
    assert response.status_code == 422
    operation_adapter.submit_run.assert_not_called()


def test_null_body_key_uses_header_but_does_not_allow_missing_key(
    client, operation_adapter, operation_store, auth_headers
):
    missing = client.post(
        "/api/v1/runs", json={**PAYLOAD, "idempotency_key": None}, headers=auth_headers
    )
    assert missing.status_code == 422
    assert operation_adapter.submit_run.call_count == 0
    provided = client.post(
        "/api/v1/runs",
        json={**PAYLOAD, "idempotency_key": None},
        headers={**auth_headers, "Idempotency-Key": "provided"},
    )
    assert provided.status_code == 201


def test_old_payload_fingerprint_is_preserved_with_body_key(
    client, operation_adapter, operation_store, auth_headers
):
    old_payload = {**PAYLOAD, "tags": {}, "op_selection": None, "asset_selection": None}
    old_result = operation_store.submit(
        "legacy",
        "existing-event",
        old_payload,
        create=lambda marker: {"run_id": "existing-real-run", "status": "QUEUED"},
        lookup=Mock(),
    )
    response = client.post(
        "/api/v1/runs",
        headers=auth_headers,
        json={**PAYLOAD, "idempotency_key": "existing-event"},
    )
    assert response.status_code == 200
    assert response.json()["run_id"] == "existing-real-run"
    assert response.json()["submission_id"] == old_result.submission_id
    operation_adapter.submit_run.assert_not_called()


@pytest.mark.parametrize("include_key_header", [False, True])
def test_direct_run_always_creates_and_has_no_store_dependency(
    client, app, operation_adapter, auth_headers, include_key_header
):
    def unavailable_store():
        raise AssertionError("Non-idempotent submission must not open its store")

    app.dependency_overrides[gateway.get_idempotency_store] = unavailable_store
    operation_adapter.submit_direct_run.side_effect = [
        {"run_id": "first-direct-run", "status": "QUEUED"},
        {"run_id": "second-direct-run", "status": "QUEUED"},
    ]
    headers = {
        **auth_headers,
        **(
            {"Idempotency-Key": "ignored-for-direct-submit"}
            if include_key_header
            else {}
        ),
    }
    responses = [
        client.post("/api/v1/runs/submit", headers=headers, json=PAYLOAD)
        for _ in range(2)
    ]
    assert [response.status_code for response in responses] == [201, 201]
    assert responses[0].json()["run_id"] != responses[1].json()["run_id"]
    for response in responses:
        body = response.json()
        assert set(body) == {
            "run_id",
            "status",
            "job_name",
            "repository_location_name",
            "repository_name",
            "status_url",
        }
        assert response.headers["Location"] == body["status_url"]
        assert "Idempotency-Replayed" not in response.headers
    assert operation_adapter.submit_direct_run.call_count == 2
    operation_adapter.submit_run.assert_not_called()


def test_direct_submit_permissions_invalid_body_and_uncertain_transport(
    client, operation_adapter, auth_headers, monkeypatch
):
    monkeypatch.setattr(
        gateway, "API_KEYS", (APIKey("reader", "read-only", frozenset({"read"})),)
    )
    reader = client.post(
        "/api/v1/runs/submit",
        headers={"Authorization": "Bearer read-only"},
        json=PAYLOAD,
    )
    assert reader.status_code == 403
    invalid = client.post(
        "/api/v1/runs/submit",
        headers=auth_headers,
        json={**PAYLOAD, "idempotency_key": "not-supported"},
    )
    assert invalid.status_code == 422
    operation_adapter.submit_direct_run.assert_not_called()
    operation_adapter.submit_direct_run.side_effect = DagsterGraphQLClientError(
        "Connection timed out"
    )
    uncertain = client.post("/api/v1/runs/submit", headers=auth_headers, json=PAYLOAD)
    assert uncertain.status_code == 502
    assert uncertain.json()["retryable"] is False


def test_direct_submit_is_documented_separately_from_idempotent_submit(client):
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/api/v1/runs/submit"]["post"]
    assert "非幂等" in operation["summary"]
    assert operation["x-required-scopes"] == ["submit"]
    assert "201" in operation["responses"]
    assert "Location" in operation["responses"]["201"]["headers"]
    assert (
        "idempotency_key"
        not in schema["components"]["schemas"]["RunSubmissionRequest"]["properties"]
    )
    for model in ("RunSubmissionRequest", "IdempotentRunSubmissionRequest"):
        request = schema["components"]["schemas"][model]
        assert request["required"] == ["job_name"]
        assert "job_id" not in request["properties"]
        for optional in ("repository_location_name", "repository_name"):
            assert request["properties"][optional].get("default") is None
            assert {"type": "null"} in request["properties"][optional]["anyOf"]
