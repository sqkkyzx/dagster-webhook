"""Durable submission claims for a single-host SQLite deployment.

Each key is scoped to its caller and bound to a canonical payload hash. A run ID
is learned from a response or a verified recovery lookup. A protected submission marker is
allocated before contact, and uncertain submissions are only looked up by that marker; this service never automatically submits them again.

SQLite may be shared by processes on one host using a persistent local volume.
It is not a database for replicas on separate hosts or a network filesystem.
"""

import datetime
import hashlib
import json
import math
import sqlite3
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SubmissionResult:
    run: dict[str, Any]
    replayed: bool
    submission_id: str


class IdempotencyConflict(Exception):
    """A key cannot currently accept this submission (HTTP 409)."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "IDEMPOTENCY_CONFLICT",
        retry_after: int | None = None,
        run_id: str | None = None,
        submission_id: str | None = None,
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.retry_after = retry_after
        self.run_id = run_id
        self.submission_id = submission_id


class SubmissionUncertain(IdempotencyConflict):
    """Submission may have reached Dagster; a duplicate must not be created."""

    def __init__(self, run_id: str | None, submission_id: str):
        super().__init__(
            "Submission outcome is unknown; retry this key to check the existing run, "
            "rather than submitting with a new key.",
            code="SUBMISSION_OUTCOME_UNKNOWN",
            run_id=run_id,
            submission_id=submission_id,
        )


class SubmissionNotFound(Exception):
    """No retained submission belongs to this caller and identifier."""


class StoredSubmissionRejection(Exception):
    """An earlier request with this key was explicitly rejected by Dagster."""

    def __init__(self, error_type: str, submission_id: str, run_id: str | None):
        super().__init__(error_type)
        self.error_type = error_type
        self.submission_id = submission_id
        self.run_id = run_id


# No request configuration, credentials, arbitrary metadata, or error text is
# persisted. Status is a submission-time snapshot, not a live status query.
_STORED_RUN_FIELDS = frozenset(
    {
        "run_id",
        "status",
        "job_id",
        "job_name",
        "repository_location_name",
        "repository_name",
        "created_at",
        "started_at",
        "ended_at",
        "parent_run_id",
        "root_run_id",
    }
)


class IdempotencyStore:
    """Claim keys atomically and preserve unknown outcomes across restarts.

    Successful and explicitly rejected submissions reserve their key until
    ``retention_seconds`` after resolution. Once that window expires the same
    key can create a new run. Unknown outcomes never expire automatically.
    A live request owns a short lease; after it expires, another process may
    look up its protected submission marker but may not call ``create`` again.
    Historical claims retain their original run-ID recovery method.
    """

    def __init__(
        self,
        path: str | Path,
        retention_seconds: int = 86400,
        lease_seconds: int = 30,
    ):
        if str(path) == ":memory:":
            raise ValueError("Idempotency requires a persistent SQLite file")
        if retention_seconds <= 0 or lease_seconds <= 0:
            raise ValueError("Retention and lease must be positive")
        self.path = Path(path).expanduser()
        self.retention_seconds = retention_seconds
        self.lease_seconds = lease_seconds
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as connection:
            # Serialize schema migration so old databases can be opened safely
            # by multiple workers after an upgrade.
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    """CREATE TABLE IF NOT EXISTS submissions (
                        principal_hash TEXT NOT NULL,
                        key_hash TEXT NOT NULL,
                        payload_hash TEXT NOT NULL,
                        submission_id TEXT NOT NULL UNIQUE,
                        run_id TEXT NOT NULL UNIQUE,
                        state TEXT NOT NULL,
                        created_at REAL NOT NULL,
                        lease_expires_at REAL NOT NULL,
                        expires_at REAL,
                        result_json TEXT,
                        error_type TEXT,
                        last_checked_at REAL,
                        last_check_outcome TEXT,
                        recovery_kind TEXT NOT NULL DEFAULT 'legacy_run_id',
                        actual_run_id TEXT,
                        expected_job_id TEXT,
                        PRIMARY KEY (principal_hash, key_hash)
                    )"""
                )
                columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(submissions)")
                }
                for name, kind in (
                    ("last_checked_at", "REAL"),
                    ("last_check_outcome", "TEXT"),
                    ("recovery_kind", "TEXT NOT NULL DEFAULT 'legacy_run_id'"),
                    ("actual_run_id", "TEXT"),
                    ("expected_job_id", "TEXT"),
                ):
                    if name not in columns:
                        connection.execute(
                            f"ALTER TABLE submissions ADD COLUMN {name} {kind}"
                        )
                connection.execute(
                    """UPDATE submissions SET actual_run_id = run_id
                    WHERE recovery_kind = 'legacy_run_id' AND state = 'ACCEPTED' AND actual_run_id IS NULL"""
                )
                connection.execute(
                    """CREATE INDEX IF NOT EXISTS submissions_expiry ON submissions(expires_at)
                    WHERE state IN ('ACCEPTED', 'REJECTED')"""
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(str(self.path), timeout=10, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
        finally:
            connection.close()

    @staticmethod
    def _now() -> float:
        return time.time()

    @staticmethod
    def _hash(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _claim(
        self,
        principal_hash: str,
        key_hash: str,
        payload_hash: str,
        expected_job_id: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        now = self._now()
        record = None
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                # Bound maintenance work on each claim. Unresolved submissions
                # must remain available even when their creation time is old.
                connection.execute(
                    """DELETE FROM submissions WHERE rowid IN (
                        SELECT rowid FROM submissions
                        WHERE state IN ('ACCEPTED', 'REJECTED') AND expires_at <= ?
                        ORDER BY expires_at LIMIT 100
                    )""",
                    (now,),
                )
                row = connection.execute(
                    "SELECT * FROM submissions WHERE principal_hash = ? AND key_hash = ?",
                    (principal_hash, key_hash),
                ).fetchone()
                if (
                    row is not None
                    and row["state"] in {"ACCEPTED", "REJECTED"}
                    and row["expires_at"] <= now
                ):
                    connection.execute(
                        "DELETE FROM submissions WHERE submission_id = ?",
                        (row["submission_id"],),
                    )
                    row = None
                if row is None:
                    submission_id = str(uuid.uuid4())
                    # The historical run_id column now retains the recovery
                    # token, not an invented public Dagster Run ID.
                    run_id = submission_id
                    connection.execute(
                        """INSERT INTO submissions
                        (principal_hash, key_hash, payload_hash, submission_id, run_id,
                         state, created_at, lease_expires_at, recovery_kind, expected_job_id)
                        VALUES (?, ?, ?, ?, ?, 'REQUESTING', ?, ?, 'submission_tag', ?)""",
                        (
                            principal_hash,
                            key_hash,
                            payload_hash,
                            submission_id,
                            run_id,
                            now,
                            now + self.lease_seconds,
                            expected_job_id,
                        ),
                    )
                    row = connection.execute(
                        "SELECT * FROM submissions WHERE submission_id = ?",
                        (submission_id,),
                    ).fetchone()
                    connection.commit()
                    return dict(row), True
                record = dict(row)
                if record["payload_hash"] != payload_hash:
                    raise IdempotencyConflict(
                        "This idempotency key is already bound to a different request.",
                        code="IDEMPOTENCY_PAYLOAD_MISMATCH",
                        run_id=self._known_run_id(record),
                        submission_id=record["submission_id"],
                    )
                if record["state"] == "REQUESTING":
                    remaining = record["lease_expires_at"] - now
                    if remaining > 0:
                        raise IdempotencyConflict(
                            "A request with this idempotency key is still in progress.",
                            code="IDEMPOTENCY_IN_PROGRESS",
                            retry_after=max(1, math.ceil(remaining)),
                            run_id=self._known_run_id(record),
                            submission_id=record["submission_id"],
                        )
                if record["state"] in {"UNKNOWN", "REQUESTING"}:
                    # Lease a recovery lookup, never another submission.
                    connection.execute(
                        """UPDATE submissions SET state = 'REQUESTING', lease_expires_at = ?
                        WHERE submission_id = ?""",
                        (now + self.lease_seconds, record["submission_id"]),
                    )
                connection.commit()
                return record, False
            except BaseException as exception:
                connection.rollback()
                if record is not None and isinstance(exception, sqlite3.Error):
                    self._annotate_failure(exception, record)
                raise

    def _unknown(
        self,
        record: dict[str, Any],
        checked_at: float | None = None,
        outcome: str | None = None,
    ) -> None:
        with self._connection() as connection:
            connection.execute(
                """UPDATE submissions SET state = 'UNKNOWN', lease_expires_at = 0,
                    last_checked_at = COALESCE(?, last_checked_at),
                    last_check_outcome = COALESCE(?, last_check_outcome),
                    actual_run_id = COALESCE(?, actual_run_id)
                WHERE submission_id = ? AND state NOT IN ('ACCEPTED', 'REJECTED')""",
                (
                    checked_at,
                    outcome,
                    record.get("actual_run_id"),
                    record["submission_id"],
                ),
            )

    def _preserve_unknown(self, record, checked_at=None, outcome=None) -> None:
        # A failed recovery write cannot turn an already claimed request into a
        # pre-submission storage error or hide its recovery identifiers.
        try:
            self._unknown(record, checked_at, outcome)
        except sqlite3.Error:
            pass

    def _accepted(
        self,
        record: dict[str, Any],
        run: dict[str, Any],
        checked_at: float | None = None,
    ) -> SubmissionResult:
        if (
            not isinstance(run, dict)
            or not isinstance(run.get("run_id"), str)
            or not run["run_id"]
        ):
            raise ValueError("Dagster returned no valid run ID")
        if (
            record["recovery_kind"] == "legacy_run_id"
            and run["run_id"] != record["run_id"]
        ):
            raise ValueError("Dagster returned an unexpected run ID")
        record["actual_run_id"] = run["run_id"]
        snapshot = {
            key: value for key, value in run.items() if key in _STORED_RUN_FIELDS
        }
        serialized = json.dumps(snapshot, sort_keys=True, allow_nan=False)
        with self._connection() as connection:
            connection.execute(
                """UPDATE submissions SET state = 'ACCEPTED', result_json = ?, expires_at = ?,
                   lease_expires_at = 0,
                   last_checked_at = COALESCE(?, last_checked_at),
                   last_check_outcome = CASE WHEN ? IS NULL THEN last_check_outcome ELSE 'ACCEPTED' END,
                   actual_run_id = ?
                WHERE submission_id = ? AND state NOT IN ('ACCEPTED', 'REJECTED')""",
                (
                    serialized,
                    self._now() + self.retention_seconds,
                    checked_at,
                    checked_at,
                    run["run_id"],
                    record["submission_id"],
                ),
            )
        return SubmissionResult(
            run=snapshot, replayed=False, submission_id=record["submission_id"]
        )

    def _rejected(self, record: dict[str, Any], exception: Exception) -> None:
        error_type = str(exception.args[0]) if exception.args else "SubmissionRejected"
        if not error_type.isidentifier() or len(error_type) > 128:
            error_type = "SubmissionRejected"
        with self._connection() as connection:
            connection.execute(
                """UPDATE submissions SET state = 'REJECTED', error_type = ?, expires_at = ?,
                   lease_expires_at = 0
                WHERE submission_id = ? AND state NOT IN ('ACCEPTED', 'REJECTED')""",
                (
                    error_type,
                    self._now() + self.retention_seconds,
                    record["submission_id"],
                ),
            )

    @staticmethod
    def _known_run_id(record):
        return record.get("actual_run_id")

    @staticmethod
    def _annotate_failure(exception: Exception, record: dict[str, Any]) -> None:
        # Preserve the original exception type, message and traceback while
        # exposing the durable identifiers to the HTTP error handler.
        for attribute, value in (
            ("gateway_submission_id", record["submission_id"]),
            ("gateway_run_id", IdempotencyStore._known_run_id(record)),
        ):
            try:
                setattr(exception, attribute, value)
            except Exception:  # noqa: BLE001, S110 -- Preserve the original exception.
                pass

    def _check_existing(self, record, lookup) -> SubmissionResult | None:
        try:
            existing = lookup(record["recovery_kind"], record["run_id"])
        except Exception:  # noqa: BLE001 -- Any failed lookup leaves the outcome unknown.
            checked_at = self._now()
            try:
                self._unknown(record, checked_at, "UPSTREAM_ERROR")
            except Exception as storage_error:
                raise SubmissionUncertain(
                    self._known_run_id(record), record["submission_id"]
                ) from storage_error
            return None
        checked_at = self._now()
        try:
            if existing is None:
                self._unknown(record, checked_at, "NOT_FOUND")
                return None
            if (
                record.get("expected_job_id")
                and existing.get("job_id") != record["expected_job_id"]
            ):
                self._unknown(record, checked_at, "MISMATCH")
                return None
            record["actual_run_id"] = existing.get("run_id")
            if existing.get("status") == "NOT_STARTED":
                self._unknown(record, checked_at, "NOT_STARTED")
                return None
            return self._accepted(record, existing, checked_at)
        except Exception as exception:
            self._preserve_unknown(record, checked_at, "UPSTREAM_ERROR")
            raise SubmissionUncertain(
                self._known_run_id(record), record["submission_id"]
            ) from exception

    def _owned_record(self, connection, principal: str, submission_id: str) -> dict:
        row = connection.execute(
            "SELECT * FROM submissions WHERE principal_hash = ? AND submission_id = ?",
            (self._hash(principal), submission_id),
        ).fetchone()
        if row is None or (
            row["state"] in {"ACCEPTED", "REJECTED"}
            and row["expires_at"] <= self._now()
        ):
            raise SubmissionNotFound
        return dict(row)

    def _public_record(self, record: dict) -> dict:
        def timestamp(value):
            return (
                datetime.datetime.fromtimestamp(value, datetime.UTC).isoformat()
                if value is not None
                else None
            )

        state = record["state"]
        lease = record["lease_expires_at"]
        if state == "REQUESTING" and lease <= self._now():
            state = "UNKNOWN"
        return {
            "submission_id": record["submission_id"],
            "run_id": self._known_run_id(record),
            "state": state,
            "created_at": timestamp(record["created_at"]),
            "expires_at": timestamp(record["expires_at"]),
            "lease_expires_at": timestamp(lease) if state == "REQUESTING" else None,
            "last_checked_at": timestamp(record["last_checked_at"]),
            "last_check_outcome": record["last_check_outcome"],
            "error_type": record["error_type"],
            "run": json.loads(record["result_json"]) if record["result_json"] else None,
        }

    def get(self, principal: str, submission_id: str) -> dict:
        """Read retained local state; never query Dagster or extend retention."""
        with self._connection() as connection:
            return self._public_record(
                self._owned_record(connection, principal, submission_id)
            )

    def reconcile(self, principal: str, submission_id: str, lookup) -> dict:
        """Lease a read-only marker lookup; never create a run or release the key."""
        record = None
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                record = self._owned_record(connection, principal, submission_id)
                if record["state"] in {"ACCEPTED", "REJECTED"}:
                    connection.commit()
                    return self._public_record(record)
                remaining = record["lease_expires_at"] - self._now()
                if record["state"] == "REQUESTING" and remaining > 0:
                    raise IdempotencyConflict(
                        "This submission is still being processed.",
                        code="IDEMPOTENCY_IN_PROGRESS",
                        retry_after=max(1, math.ceil(remaining)),
                        run_id=self._known_run_id(record),
                        submission_id=record["submission_id"],
                    )
                connection.execute(
                    "UPDATE submissions SET state = 'REQUESTING', lease_expires_at = ? WHERE submission_id = ?",
                    (self._now() + self.lease_seconds, submission_id),
                )
                connection.commit()
            except BaseException as exception:
                connection.rollback()
                if record is not None and isinstance(exception, sqlite3.Error):
                    raise SubmissionUncertain(
                        self._known_run_id(record), record["submission_id"]
                    ) from exception
                raise
        self._check_existing(record, lookup)
        try:
            return self.get(principal, submission_id)
        except sqlite3.Error as exception:
            raise SubmissionUncertain(
                self._known_run_id(record), record["submission_id"]
            ) from exception

    def submit(
        self,
        principal: str,
        key: str,
        payload: dict[str, Any],
        create: Callable[[str], dict[str, Any]],
        lookup: Callable[[str, str], dict[str, Any] | None],
        is_rejection: Callable[[Exception], bool] | None = None,
    ) -> SubmissionResult:
        if not principal or not isinstance(principal, str):
            raise ValueError("A non-empty caller identity is required")
        if not isinstance(key, str) or not key.strip() or len(key) > 255:
            raise ValueError("An idempotency key must contain 1 to 255 characters")
        if not isinstance(payload, dict):
            raise TypeError("A submission payload must be an object")
        canonical = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        record, fresh = self._claim(
            self._hash(principal),
            self._hash(key),
            self._hash(canonical),
            payload.get("job_id"),
        )
        if not fresh:
            if record["state"] == "ACCEPTED":
                return SubmissionResult(
                    run=json.loads(record["result_json"]),
                    replayed=True,
                    submission_id=record["submission_id"],
                )
            if record["state"] == "REJECTED":
                raise StoredSubmissionRejection(
                    record["error_type"],
                    record["submission_id"],
                    self._known_run_id(record),
                )
            accepted = self._check_existing(record, lookup)
            if accepted is None:
                raise SubmissionUncertain(
                    self._known_run_id(record), record["submission_id"]
                )
            return SubmissionResult(
                run=accepted.run, replayed=True, submission_id=accepted.submission_id
            )
        try:
            run = create(record["run_id"])
        except Exception as exception:
            self._annotate_failure(exception, record)
            try:
                if is_rejection is not None and is_rejection(exception):
                    self._rejected(record, exception)
                else:
                    self._unknown(record)
            except sqlite3.Error:
                # The original claim still exists and can be recovered after its
                # lease expires; preserve the original upstream exception.
                pass
            raise
        try:
            return self._accepted(record, run)
        except Exception as exception:
            self._annotate_failure(exception, record)
            raise
