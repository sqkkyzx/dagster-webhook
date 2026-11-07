"""Exercise durable claims without contacting Dagster or any external service."""

import multiprocessing
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from unittest.mock import Mock

import pytest

from gateway.idempotency import (
    IdempotencyConflict,
    IdempotencyStore,
    StoredSubmissionRejection,
    SubmissionUncertain,
)


@pytest.fixture
def store(tmp_path):
    return IdempotencyStore(
        tmp_path / "claims.sqlite3", retention_seconds=60, lease_seconds=10
    )


@pytest.fixture
def clock(monkeypatch):
    current = [1000.0]
    monkeypatch.setattr(IdempotencyStore, "_now", staticmethod(lambda: current[0]))
    return current


def accepted(run_id):
    return {"run_id": run_id, "status": "QUEUED"}


def test_identical_request_replays_after_store_recreation(store):
    create = Mock(side_effect=accepted)
    lookup = Mock()
    first = store.submit("caller", "request", {"b": 2, "a": 1}, create, lookup)
    restarted = IdempotencyStore(store.path)
    second = restarted.submit("caller", "request", {"a": 1, "b": 2}, create, lookup)
    assert not first.replayed
    assert second.replayed
    assert second.run == first.run
    assert second.submission_id == first.submission_id
    create.assert_called_once_with(first.run["run_id"])
    lookup.assert_not_called()


def test_key_is_scoped_to_caller(store):
    first = store.submit("first", "request", {}, accepted, Mock())
    second = store.submit("second", "request", {}, accepted, Mock())
    assert first.run["run_id"] != second.run["run_id"]


def test_same_key_with_different_payload_is_rejected(store):
    create = Mock(side_effect=accepted)
    first = store.submit("caller", "request", {"value": 1}, create, Mock())
    with pytest.raises(IdempotencyConflict) as caught:
        store.submit("caller", "request", {"value": 2}, create, Mock())
    assert caught.value.code == "IDEMPOTENCY_PAYLOAD_MISMATCH"
    assert caught.value.run_id == first.run["run_id"]
    assert create.call_count == 1


def test_concurrent_store_instances_create_only_once_without_holding_db_lock(store):
    entered, release = Event(), Event()
    create = Mock()

    def slow_create(run_id):
        entered.set()
        assert release.wait(timeout=5)
        return accepted(run_id)

    create.side_effect = slow_create
    competitors = [IdempotencyStore(store.path) for _ in range(7)]

    def compete(other):
        try:
            return other.submit("caller", "request", {}, create, Mock())
        except IdempotencyConflict as conflict:
            return conflict

    with ThreadPoolExecutor(max_workers=8) as executor:
        first_future = executor.submit(
            store.submit, "caller", "request", {}, create, Mock()
        )
        assert entered.wait(timeout=5)
        try:
            conflicts = list(executor.map(compete, competitors))
            assert all(isinstance(item, IdempotencyConflict) for item in conflicts)
            assert all(item.code == "IDEMPOTENCY_IN_PROGRESS" for item in conflicts)
            assert all(item.retry_after > 0 for item in conflicts)
            # Another key can resolve while the first remote call is blocked.
            unrelated = competitors[0].submit(
                "caller", "different", {}, accepted, Mock()
            )
            assert not unrelated.replayed
        finally:
            release.set()
        first = first_future.result(timeout=5)
    assert create.call_count == 1
    assert all(
        item.run_id is None and item.submission_id == first.submission_id
        for item in conflicts
    )


def _process_submit(path, queue, entered, release):
    store = IdempotencyStore(path)

    def create(run_id):
        entered.set()
        if not release.wait(timeout=10):
            raise TimeoutError("test release timed out")
        return accepted(run_id)

    try:
        result = store.submit(
            "caller", "request", {}, create, lambda _kind, _token: None
        )
        queue.put(("accepted", result.submission_id))
    except IdempotencyConflict as conflict:
        queue.put((conflict.code, conflict.submission_id))


def test_processes_share_atomic_claims(store):
    context = multiprocessing.get_context("spawn")
    queue, entered, release = context.Queue(), context.Event(), context.Event()
    first = context.Process(
        target=_process_submit, args=(str(store.path), queue, entered, release)
    )
    second = context.Process(
        target=_process_submit, args=(str(store.path), queue, entered, release)
    )
    first.start()
    try:
        assert entered.wait(timeout=10)
        second.start()
        conflict = queue.get(timeout=10)
        assert conflict[0] == "IDEMPOTENCY_IN_PROGRESS"
        second.join(timeout=10)
        assert second.exitcode == 0
        release.set()
        success = queue.get(timeout=10)
        assert success == ("accepted", conflict[1])
        first.join(timeout=10)
        assert first.exitcode == 0
    finally:
        release.set()
        for process in (first, second):
            if process.pid is not None:
                if process.is_alive():
                    process.terminate()
                process.join(timeout=5)
        queue.close()


def test_response_timeout_recovers_preallocated_run_without_resubmitting(store):
    created = {}

    def create(run_id):
        created[run_id] = {"run_id": run_id, "status": "STARTED"}
        raise TimeoutError("response lost after Dagster accepted")

    create_spy = Mock(side_effect=create)
    with pytest.raises(TimeoutError) as failure:
        store.submit(
            "caller", "request", {}, create_spy, lambda _kind, token: created.get(token)
        )
    assert failure.value.gateway_run_id is None
    assert failure.value.gateway_submission_id in created
    assert failure.value.gateway_submission_id
    result = IdempotencyStore(store.path).submit(
        "caller", "request", {}, create_spy, lambda _kind, token: created.get(token)
    )
    assert result.replayed
    assert result.run["status"] == "STARTED"
    assert result.run["run_id"] in created
    assert create_spy.call_count == 1


def test_unknown_outcome_never_expires_or_creates_again(store, clock):
    create = Mock(side_effect=TimeoutError("unknown remote result"))
    with pytest.raises(TimeoutError):
        store.submit("caller", "request", {}, create, lambda _kind, _token: None)
    for advance in (1, 1000, 100000):
        clock[0] += advance
        with pytest.raises(SubmissionUncertain) as caught:
            store.submit("caller", "request", {}, create, lambda _kind, _token: None)
        assert caught.value.code == "SUBMISSION_OUTCOME_UNKNOWN"
        assert caught.value.run_id is None
        assert caught.value.submission_id
    assert create.call_count == 1


def test_process_crash_leaves_claim_and_recovers_after_lease(store, clock):
    created = {}

    def crash(run_id):
        created[run_id] = accepted(run_id)
        raise SystemExit("simulate a process exiting before storing its response")

    with pytest.raises(SystemExit):
        store.submit(
            "caller", "request", {}, crash, lambda _kind, token: created.get(token)
        )
    restarted = IdempotencyStore(store.path)
    with pytest.raises(IdempotencyConflict) as caught:
        restarted.submit(
            "caller", "request", {}, Mock(), lambda _kind, token: created.get(token)
        )
    assert caught.value.code == "IDEMPOTENCY_IN_PROGRESS"
    clock[0] += 31
    create = Mock()
    recovered = restarted.submit(
        "caller", "request", {}, create, lambda _kind, token: created.get(token)
    )
    assert recovered.replayed
    assert recovered.run["run_id"] in created
    create.assert_not_called()


def test_accepted_key_can_be_reused_only_after_retention(store, clock):
    first = store.submit("caller", "request", {"value": 1}, accepted, Mock())
    clock[0] += 59
    replay = store.submit("caller", "request", {"value": 1}, accepted, Mock())
    assert replay.run["run_id"] == first.run["run_id"]
    clock[0] += 1
    replacement = store.submit("caller", "request", {"value": 2}, accepted, Mock())
    assert not replacement.replayed
    assert replacement.run["run_id"] != first.run["run_id"]


def test_explicit_rejection_replays_type_without_persisting_error_details(store, clock):
    secret = "private-password-in-upstream-validation-error"
    error = ValueError("RunConfigValidationInvalid", secret)
    create = Mock(side_effect=error)
    with pytest.raises(ValueError) as first:
        store.submit(
            "caller", "request", {}, create, Mock(), is_rejection=lambda _exc: True
        )
    assert first.value is error
    with pytest.raises(StoredSubmissionRejection) as replay:
        store.submit("caller", "request", {}, create, Mock())
    assert replay.value.error_type == "RunConfigValidationInvalid"
    assert create.call_count == 1
    assert secret.encode() not in Path(store.path).read_bytes()
    clock[0] += 60
    retry = store.submit("caller", "request", {"corrected": True}, accepted, Mock())
    assert not retry.replayed


def test_lookup_error_keeps_outcome_unknown(store):
    create = Mock(side_effect=TimeoutError("initial result unknown"))
    with pytest.raises(TimeoutError):
        store.submit("caller", "request", {}, create, Mock())
    lookup = Mock(side_effect=ConnectionError("Dagster unavailable"))
    with pytest.raises(SubmissionUncertain):
        store.submit("caller", "request", {}, create, lookup)
    recovered = store.submit(
        "caller", "request", {}, create, lambda _kind, token: accepted(token)
    )
    assert recovered.replayed
    assert create.call_count == 1


def test_response_persistence_failure_recovers_without_new_create(
    store, clock, monkeypatch
):
    created = {}

    def create(run_id):
        created[run_id] = accepted(run_id)
        return created[run_id]

    def failed_write(*_args):
        raise sqlite3.OperationalError("simulated write failure")

    monkeypatch.setattr(store, "_accepted", failed_write)
    with pytest.raises(sqlite3.OperationalError):
        store.submit(
            "caller", "request", {}, create, lambda _kind, token: created.get(token)
        )
    clock[0] += 31
    create_again = Mock()
    recovered = IdempotencyStore(store.path).submit(
        "caller", "request", {}, create_again, lambda _kind, token: created.get(token)
    )
    assert recovered.replayed
    assert recovered.run["run_id"] in created
    create_again.assert_not_called()


def test_storage_contains_only_hashes_and_safe_run_snapshot(store):
    principal, key, secret = (
        "private-caller-token",
        "private-request-key",
        "private-run-config-secret",
    )

    def create(run_id):
        return {
            "run_id": run_id,
            "status": "QUEUED",
            "run_config": {"password": secret},
            "tags": {"api_key": secret},
            "failure_summary": secret,
        }

    result = store.submit(
        principal, key, {"run_config": {"password": secret}}, create, Mock()
    )
    assert result.run == {"run_id": result.run["run_id"], "status": "QUEUED"}
    disk = Path(store.path).read_bytes()
    assert all(value.encode() not in disk for value in (principal, key, secret))


def test_stale_unknown_transition_cannot_overwrite_accepted_result(store, clock):
    entered, finish = Event(), Event()
    recovery_entered, finish_recovery = Event(), Event()

    def create(run_id):
        entered.set()
        assert finish.wait(timeout=5)
        return accepted(run_id)

    def lookup(_kind, _token):
        recovery_entered.set()
        assert finish_recovery.wait(timeout=5)

    with ThreadPoolExecutor(max_workers=2) as executor:
        original = executor.submit(
            store.submit, "caller", "request", {}, create, lookup
        )
        assert entered.wait(timeout=5)
        clock[0] += 11
        recovery = executor.submit(
            store.submit, "caller", "request", {}, Mock(), lookup
        )
        assert recovery_entered.wait(timeout=5)
        try:
            finish.set()
            first = original.result(timeout=5)
        finally:
            finish_recovery.set()
            finish.set()
        with pytest.raises(SubmissionUncertain):
            recovery.result(timeout=5)
    replay = store.submit("caller", "request", {}, Mock(), Mock())
    assert replay.replayed
    assert replay.run == first.run


@pytest.mark.parametrize("payload", [{"invalid": float("nan")}, {"invalid": object()}])
def test_non_json_payload_is_rejected_before_claim(store, payload):
    create = Mock()
    with pytest.raises((ValueError, TypeError)):
        store.submit("caller", "request", payload, create, Mock())
    create.assert_not_called()
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM submissions").fetchone()[0] == 0


def test_memory_database_is_not_accepted():
    with pytest.raises(ValueError, match="persistent"):
        IdempotencyStore(":memory:")


def test_claim_cleanup_removes_expired_resolved_rows_but_preserves_unknown(
    store, clock
):
    store.submit("caller", "accepted", {}, accepted, Mock())
    rejected = Mock(side_effect=ValueError("RunConfigValidationInvalid", "invalid"))
    with pytest.raises(ValueError):
        store.submit(
            "caller", "rejected", {}, rejected, Mock(), is_rejection=lambda _exc: True
        )
    uncertain = Mock(side_effect=TimeoutError("unknown"))
    with pytest.raises(TimeoutError):
        store.submit("caller", "unknown", {}, uncertain, Mock())
    clock[0] += 61
    store.submit("caller", "new-key", {}, accepted, Mock())
    with sqlite3.connect(store.path) as connection:
        states = connection.execute(
            "SELECT state FROM submissions ORDER BY state"
        ).fetchall()
    assert states == [("ACCEPTED",), ("UNKNOWN",)]
    with pytest.raises(SubmissionUncertain):
        store.submit("caller", "unknown", {}, uncertain, lambda _kind, _token: None)
    assert uncertain.call_count == 1


def test_expired_row_cleanup_is_bounded_per_claim(store, clock):
    for index in range(101):
        store.submit("caller", f"old-key-{index}", {}, accepted, Mock())
    clock[0] += 61
    store.submit("caller", "new-key", {}, accepted, Mock())
    with sqlite3.connect(store.path) as connection:
        remaining = connection.execute("SELECT COUNT(*) FROM submissions").fetchone()[0]
    assert remaining == 2


def test_get_submission_is_owned_retained_snapshot_without_upstream(store, clock):
    from gateway.idempotency import SubmissionNotFound

    first = store.submit("caller", "request", {}, accepted, Mock())
    record = store.get("caller", first.submission_id)
    assert record["state"] == "ACCEPTED"
    assert record["run"]["run_id"] == first.run["run_id"]
    assert record["last_checked_at"] is None
    assert record["expires_at"]
    assert (
        not {"principal_hash", "key_hash", "payload_hash", "result_json"}
        & record.keys()
    )
    with pytest.raises(SubmissionNotFound):
        store.get("another-caller", first.submission_id)
    clock[0] += 60
    with pytest.raises(SubmissionNotFound):
        store.get("caller", first.submission_id)
    # GET did not perform maintenance writes; expiry hides the retained row.
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM submissions").fetchone()[0] == 1


@pytest.mark.parametrize("outcome", ["NOT_FOUND", "NOT_STARTED", "UPSTREAM_ERROR"])
def test_explicit_reconcile_records_unknown_reason_without_resubmitting(
    store, clock, outcome
):
    create = Mock(side_effect=TimeoutError("response lost"))
    with pytest.raises(TimeoutError) as initial:
        store.submit("caller", "request", {}, create, Mock())
    submission_id = initial.value.gateway_submission_id
    if outcome == "UPSTREAM_ERROR":
        lookup = Mock(side_effect=ConnectionError("secret-upstream-error"))
    elif outcome == "NOT_STARTED":
        lookup = Mock(
            side_effect=lambda _kind, token: {"run_id": token, "status": "NOT_STARTED"}
        )
    else:
        lookup = Mock(return_value=None)
    clock[0] += 5000
    result = store.reconcile("caller", submission_id, lookup)
    assert result["state"] == "UNKNOWN"
    assert result["last_check_outcome"] == outcome
    assert result["last_checked_at"]
    assert result["expires_at"] is None
    assert result["run"] is None
    assert create.call_count == 1
    assert b"secret-upstream-error" not in Path(store.path).read_bytes()


def test_reconcile_recovers_and_terminal_reconcile_does_not_extend_retention(
    store, clock
):
    with pytest.raises(TimeoutError) as initial:
        store.submit("caller", "request", {}, Mock(side_effect=TimeoutError()), Mock())
    submission_id = initial.value.gateway_submission_id
    run_id = "actual-dagster-run"
    lookup = Mock(
        return_value={
            "run_id": run_id,
            "status": "SUCCESS",
            "created_at": "2026-11-08T00:00:00Z",
            "started_at": "2026-11-08T00:00:01Z",
            "ended_at": "2026-11-08T00:00:02Z",
        }
    )
    recovered = store.reconcile("caller", submission_id, lookup)
    assert recovered["state"] == "ACCEPTED"
    assert recovered["last_check_outcome"] == "ACCEPTED"
    assert recovered["run"]["ended_at"] == "2026-11-08T00:00:02Z"
    clock[0] += 30
    replay = store.reconcile("caller", submission_id, lookup)
    assert replay == recovered
    lookup.assert_called_once_with("submission_tag", submission_id)


def test_reconcile_active_claim_returns_lease_then_expired_get_is_unknown(store, clock):
    def crashed(_run_id):
        raise SystemExit

    with pytest.raises(SystemExit):
        store.submit("caller", "request", {}, crashed, Mock())
    with sqlite3.connect(store.path) as connection:
        submission_id = connection.execute(
            "SELECT submission_id FROM submissions"
        ).fetchone()[0]
    assert store.get("caller", submission_id)["state"] == "REQUESTING"
    lookup = Mock(return_value=None)
    with pytest.raises(IdempotencyConflict) as conflict:
        store.reconcile("caller", submission_id, lookup)
    assert conflict.value.retry_after == 10
    lookup.assert_not_called()
    clock[0] += 10
    assert store.get("caller", submission_id)["state"] == "UNKNOWN"
    assert store.reconcile("caller", submission_id, lookup)["state"] == "UNKNOWN"
    lookup.assert_called_once()


@pytest.mark.parametrize("operation", ["replay", "reconcile"])
@pytest.mark.parametrize("lookup_fails", [False, True])
def test_recovery_storage_failure_keeps_uncertain_identifiers(
    store, monkeypatch, operation, lookup_fails
):
    with pytest.raises(TimeoutError) as initial:
        store.submit("caller", "request", {}, Mock(side_effect=TimeoutError()), Mock())
    lookup = (
        Mock(side_effect=ConnectionError()) if lookup_fails else Mock(return_value=None)
    )
    monkeypatch.setattr(
        store, "_unknown", Mock(side_effect=sqlite3.OperationalError("disk full"))
    )
    with pytest.raises(SubmissionUncertain) as result:
        if operation == "replay":
            store.submit("caller", "request", {}, Mock(), lookup)
        else:
            store.reconcile("caller", initial.value.gateway_submission_id, lookup)
    assert result.value.run_id == initial.value.gateway_run_id
    assert result.value.submission_id == initial.value.gateway_submission_id


def test_migration_of_legacy_database_is_concurrent_and_preserves_rows(tmp_path):
    import json

    path = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("""CREATE TABLE submissions (
            principal_hash TEXT NOT NULL, key_hash TEXT NOT NULL, payload_hash TEXT NOT NULL,
            submission_id TEXT NOT NULL UNIQUE, run_id TEXT NOT NULL UNIQUE,
            state TEXT NOT NULL, created_at REAL NOT NULL, lease_expires_at REAL NOT NULL,
            expires_at REAL, result_json TEXT, error_type TEXT,
            PRIMARY KEY (principal_hash, key_hash))""")
        connection.execute(
            "INSERT INTO submissions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                IdempotencyStore._hash("caller"),
                IdempotencyStore._hash("key"),
                "payload-hash",
                "submission-legacy",
                "run-legacy",
                "ACCEPTED",
                1000,
                0,
                9999999999,
                json.dumps({"run_id": "run-legacy", "status": "QUEUED"}),
                None,
            ),
        )
    with ThreadPoolExecutor(max_workers=6) as executor:
        stores = list(executor.map(lambda _: IdempotencyStore(path), range(6)))
    for reopened in stores:
        row = reopened.get("caller", "submission-legacy")
        assert row["state"] == "ACCEPTED"
        assert row["run"]["run_id"] == "run-legacy"
        assert row["last_checked_at"] is row["last_check_outcome"] is None
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM submissions").fetchone()[0] == 1


def test_new_claim_distinguishes_marker_from_actual_run_id(store):
    create = Mock(return_value={"run_id": "dagster-generated-id", "status": "QUEUED"})
    result = store.submit(
        "caller", "request", {"job_id": "expected-job"}, create, Mock()
    )
    create.assert_called_once_with(result.submission_id)
    assert result.run["run_id"] == "dagster-generated-id"
    assert store.get("caller", result.submission_id)["run_id"] == "dagster-generated-id"
    with sqlite3.connect(store.path) as connection:
        row = connection.execute(
            "SELECT run_id, actual_run_id, recovery_kind, expected_job_id FROM submissions"
        ).fetchone()
    assert row == (
        result.submission_id,
        "dagster-generated-id",
        "submission_tag",
        "expected-job",
    )


def test_tag_match_with_wrong_job_remains_unknown_without_claiming_run(store):
    create = Mock(side_effect=TimeoutError("response lost"))
    with pytest.raises(TimeoutError) as initial:
        store.submit("caller", "request", {"job_id": "expected"}, create, Mock())
    lookup = Mock(
        return_value={"run_id": "unrelated-run", "status": "SUCCESS", "job_id": "other"}
    )
    result = store.reconcile("caller", initial.value.gateway_submission_id, lookup)
    assert result["state"] == "UNKNOWN"
    assert result["last_check_outcome"] == "MISMATCH"
    assert result["run_id"] is result["run"] is None
    create.assert_called_once()


def test_historical_unknown_claim_uses_legacy_lookup_and_confirms_actual_id(tmp_path):
    path = tmp_path / "legacy-unknown.sqlite3"
    store = IdempotencyStore(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """INSERT INTO submissions
            (principal_hash,key_hash,payload_hash,submission_id,run_id,state,created_at,lease_expires_at,recovery_kind)
            VALUES (?, ?, 'hash', 'legacy-submission', 'legacy-candidate', 'UNKNOWN', 1000, 0, 'legacy_run_id')""",
            (store._hash("caller"), store._hash("legacy-key")),
        )
    assert store.get("caller", "legacy-submission")["run_id"] is None
    lookup = Mock(return_value={"run_id": "legacy-candidate", "status": "QUEUED"})
    result = store.reconcile("caller", "legacy-submission", lookup)
    lookup.assert_called_once_with("legacy_run_id", "legacy-candidate")
    assert result["state"] == "ACCEPTED"
    assert result["run_id"] == "legacy-candidate"


def test_concurrent_reconciliation_uses_single_lookup_lease(store):
    with pytest.raises(TimeoutError) as initial:
        store.submit("caller", "request", {}, Mock(side_effect=TimeoutError()), Mock())
    submission_id = initial.value.gateway_submission_id
    entered, release = Event(), Event()

    def slow_lookup(kind, token):
        entered.set()
        assert release.wait(timeout=5)
        return {"run_id": "actual-run", "status": "SUCCESS"}

    lookup = Mock(side_effect=slow_lookup)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(store.reconcile, "caller", submission_id, lookup)
        assert entered.wait(timeout=5)
        try:
            with pytest.raises(IdempotencyConflict) as conflict:
                IdempotencyStore(store.path).reconcile("caller", submission_id, lookup)
            assert conflict.value.code == "IDEMPOTENCY_IN_PROGRESS"
        finally:
            release.set()
        assert first.result(timeout=5)["state"] == "ACCEPTED"
    lookup.assert_called_once()
