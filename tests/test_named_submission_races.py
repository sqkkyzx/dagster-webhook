"""Name resolution never weakens durable submission ownership or recovery."""

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import Mock

import pytest

from gateway.idempotency import IdempotencyStore, SubmissionUncertain

PAYLOAD = {"job_name": "daily_job", "run_config": {}}
RESOLVED_JOB = "resolved-location-repository-job"
RUN = {"run_id": "dagster-created-run", "status": "QUEUED", "job_id": RESOLVED_JOB}


@pytest.fixture
def store(tmp_path):
    return IdempotencyStore(tmp_path / "named.sqlite3", lease_seconds=5)


def stored_claim(store):
    with sqlite3.connect(store.path) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute("SELECT * FROM submissions").fetchone()
    return dict(row) if row is not None else None


def test_expired_resolution_lease_does_not_accept_unbound_stale_recovery(
    store, monkeypatch
):
    """A retry that read the pre-resolution claim cannot skip identity checks."""
    clock = [1000.0]
    monkeypatch.setattr(IdempotencyStore, "_now", staticmethod(lambda: clock[0]))
    competitor = IdempotencyStore(store.path, lease_seconds=5)
    preparing, finish_prepare = Event(), Event()
    recovery_read, finish_recovery = Event(), Event()
    check_existing = IdempotencyStore._check_existing

    def prepare():
        preparing.set()
        assert finish_prepare.wait(timeout=5)
        return RESOLVED_JOB

    def delayed_check(self, record, lookup):
        # Preserve the retry's snapshot while the original binds its identity.
        recovery_read.set()
        assert finish_recovery.wait(timeout=5)
        return check_existing(self, record, lookup)

    monkeypatch.setattr(IdempotencyStore, "_check_existing", delayed_check)
    create = Mock(side_effect=TimeoutError("upstream accepted; response lost"))
    wrong_lookup = Mock(
        return_value={"run_id": "wrong-run", "status": "SUCCESS", "job_id": "other-job"}
    )
    retry_prepare = Mock(side_effect=AssertionError("A retry must not rediscover"))
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(
            store.submit, "caller", "event", PAYLOAD, create, Mock(), prepare=prepare
        )
        retry = None
        try:
            assert preparing.wait(timeout=5)
            clock[0] += 6
            retry = executor.submit(
                competitor.submit,
                "caller",
                "event",
                PAYLOAD,
                create,
                wrong_lookup,
                prepare=retry_prepare,
            )
            assert recovery_read.wait(timeout=5)
            finish_prepare.set()
            with pytest.raises(TimeoutError):
                first.result(timeout=5)
            assert stored_claim(store)["expected_job_id"] == RESOLVED_JOB
            finish_recovery.set()
            with pytest.raises(SubmissionUncertain):
                retry.result(timeout=5)
        finally:
            finish_prepare.set()
            finish_recovery.set()
            # Executor exit waits for both workers even when an assertion fails.
    create.assert_called_once()
    wrong_lookup.assert_not_called()
    retry_prepare.assert_not_called()
    assert stored_claim(store)["state"] == "UNKNOWN"
    assert stored_claim(store)["actual_run_id"] is None

    # A later retry has the persisted identity: wrong-source matches remain
    # unknown, and the exact-source result can recover without another launch.
    monkeypatch.setattr(IdempotencyStore, "_check_existing", check_existing)
    with pytest.raises(SubmissionUncertain):
        competitor.submit(
            "caller", "event", PAYLOAD, create, wrong_lookup, prepare=retry_prepare
        )
    assert stored_claim(store)["last_check_outcome"] == "MISMATCH"
    recovered = competitor.submit(
        "caller",
        "event",
        PAYLOAD,
        create,
        Mock(return_value=RUN),
        prepare=retry_prepare,
    )
    assert recovered.replayed is True and recovered.run == RUN
    create.assert_called_once()
    retry_prepare.assert_not_called()


def test_failed_read_only_preparation_releases_key_without_launch(store):
    create, lookup = Mock(return_value=RUN), Mock()
    failed_prepare = Mock(side_effect=RuntimeError("workspace discovery failed"))
    with pytest.raises(RuntimeError, match="workspace discovery failed"):
        store.submit("caller", "event", PAYLOAD, create, lookup, prepare=failed_prepare)
    create.assert_not_called()
    lookup.assert_not_called()
    assert stored_claim(store) is None

    # Retrying the same business key after fixing discovery can submit once.
    result = store.submit(
        "caller", "event", PAYLOAD, create, lookup, prepare=lambda: RESOLVED_JOB
    )
    assert result.replayed is False and result.run == RUN
    create.assert_called_once()
    lookup.assert_not_called()


def test_accepted_replay_does_not_resolve_current_workspace(store):
    create, lookup = Mock(return_value=RUN), Mock()
    first = store.submit(
        "caller", "event", PAYLOAD, create, lookup, prepare=lambda: RESOLVED_JOB
    )
    restarted = IdempotencyStore(store.path)
    unavailable_workspace = Mock(
        side_effect=RuntimeError("Job removed or upstream down")
    )
    second = restarted.submit(
        "caller", "event", PAYLOAD, create, lookup, prepare=unavailable_workspace
    )
    assert second.replayed is True
    assert second.run == first.run and second.submission_id == first.submission_id
    unavailable_workspace.assert_not_called()
    create.assert_called_once()
    lookup.assert_not_called()


def test_resolved_identity_is_durable_before_first_launch(store):
    def create(token):
        claim = stored_claim(store)
        assert claim["submission_id"] == token
        assert claim["expected_job_id"] == RESOLVED_JOB
        assert claim["state"] == "REQUESTING"
        # A second database reader sees the committed value before contact.
        reopened = IdempotencyStore(store.path)
        assert stored_claim(reopened)["expected_job_id"] == RESOLVED_JOB
        return RUN

    result = store.submit(
        "caller", "event", PAYLOAD, create, Mock(), prepare=lambda: RESOLVED_JOB
    )
    assert result.run == RUN
    assert stored_claim(store)["state"] == "ACCEPTED"
