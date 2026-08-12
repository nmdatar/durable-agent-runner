"""SQLite persistence for workflow state and event history."""

import json
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class RunSnapshot:
    id: str
    workflow_name: str
    status: str


@dataclass(frozen=True)
class StepSnapshot:
    name: str
    position: int
    status: str
    output: Any | None
    attempt_count: int
    next_attempt_at: str | None
    last_error: str | None


@dataclass(frozen=True)
class StoredEvent:
    id: int
    event_type: str
    step_name: str | None
    payload: dict[str, Any]
    created_at: str


@dataclass(frozen=True)
class ClaimedStep:
    run_id: str
    workflow_name: str
    step_name: str
    worker_id: str
    lease_expires_at: str
    attempt_count: int


@dataclass(frozen=True)
class OperationSnapshot:
    idempotency_key: str
    status: str
    result: Any | None


@dataclass(frozen=True)
class ArtifactSnapshot:
    id: str
    run_id: str
    step_name: str
    name: str
    path: str
    sha256: str
    size_bytes: int
    media_type: str


class SQLiteStore:
    """Store current state and append-only events in the same database."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    def initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY
                );

                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    workflow_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS steps (
                    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                    name TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    output_json TEXT,
                    started_at TEXT,
                    completed_at TEXT,
                    lease_owner TEXT,
                    lease_expires_at TEXT,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT,
                    last_error TEXT,
                    PRIMARY KEY (run_id, name),
                    UNIQUE (run_id, position)
                );

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                    step_name TEXT,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS operations (
                    idempotency_key TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                    step_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    result_json TEXT,
                    created_at TEXT NOT NULL,
                    completed_at TEXT
                );

                -- This table emulates a durable external publishing service.
                CREATE TABLE IF NOT EXISTS publications (
                    idempotency_key TEXT PRIMARY KEY,
                    publication_id TEXT NOT NULL UNIQUE,
                    report TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS artifacts (
                    id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                    step_name TEXT NOT NULL,
                    name TEXT NOT NULL,
                    path TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    media_type TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(run_id, step_name, name)
                );
                """
            )
            columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(steps)").fetchall()
            }
            if "lease_owner" not in columns:
                connection.execute("ALTER TABLE steps ADD COLUMN lease_owner TEXT")
            if "lease_expires_at" not in columns:
                connection.execute("ALTER TABLE steps ADD COLUMN lease_expires_at TEXT")
            if "attempt_count" not in columns:
                connection.execute(
                    "ALTER TABLE steps ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0"
                )
            if "next_attempt_at" not in columns:
                connection.execute("ALTER TABLE steps ADD COLUMN next_attempt_at TEXT")
            if "last_error" not in columns:
                connection.execute("ALTER TABLE steps ADD COLUMN last_error TEXT")
            connection.execute("DELETE FROM schema_version")
            connection.execute("INSERT INTO schema_version(version) VALUES (5)")
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_steps_claimable
                ON steps(status, lease_expires_at, run_id, position)
                """
            )

    def create_run(self, workflow_name: str, step_names: Sequence[str]) -> str:
        run_id = str(uuid4())
        now = utc_now()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO runs(id, workflow_name, status, created_at, updated_at)
                VALUES (?, ?, 'pending', ?, ?)
                """,
                (run_id, workflow_name, now, now),
            )
            connection.executemany(
                """
                INSERT INTO steps(run_id, name, position, status)
                VALUES (?, ?, ?, 'pending')
                """,
                [
                    (run_id, step_name, position)
                    for position, step_name in enumerate(step_names)
                ],
            )
            self._insert_event(connection, run_id, "run_created")
        return run_id

    def get_run(self, run_id: str) -> RunSnapshot:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT id, workflow_name, status FROM runs WHERE id = ?",
                (run_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown run: {run_id}")
        return RunSnapshot(row["id"], row["workflow_name"], row["status"])

    def get_steps(self, run_id: str) -> list[StepSnapshot]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT name, position, status, output_json,
                       attempt_count, next_attempt_at, last_error
                FROM steps
                WHERE run_id = ?
                ORDER BY position
                """,
                (run_id,),
            ).fetchall()
        return [
            StepSnapshot(
                name=row["name"],
                position=row["position"],
                status=row["status"],
                output=json.loads(row["output_json"])
                if row["output_json"] is not None
                else None,
                attempt_count=row["attempt_count"],
                next_attempt_at=row["next_attempt_at"],
                last_error=row["last_error"],
            )
            for row in rows
        ]

    def get_events(self, run_id: str) -> list[StoredEvent]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, event_type, step_name, payload_json, created_at
                FROM events
                WHERE run_id = ?
                ORDER BY id
                """,
                (run_id,),
            ).fetchall()
        return [
            StoredEvent(
                id=row["id"],
                event_type=row["event_type"],
                step_name=row["step_name"],
                payload=json.loads(row["payload_json"]),
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def get_operation(self, idempotency_key: str) -> OperationSnapshot | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT idempotency_key, status, result_json
                FROM operations
                WHERE idempotency_key = ?
                """,
                (idempotency_key,),
            ).fetchone()
        if row is None:
            return None
        return OperationSnapshot(
            idempotency_key=row["idempotency_key"],
            status=row["status"],
            result=json.loads(row["result_json"])
            if row["result_json"] is not None
            else None,
        )

    def begin_operation(self, run_id: str, step_name: str, idempotency_key: str) -> None:
        """Record intent before contacting an external system."""
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO operations(
                    idempotency_key, run_id, step_name, status, created_at
                ) VALUES (?, ?, ?, 'started', ?)
                """,
                (idempotency_key, run_id, step_name, utc_now()),
            )

    def complete_operation(self, idempotency_key: str, result: Any) -> None:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE operations
                SET status = 'completed', result_json = ?, completed_at = ?
                WHERE idempotency_key = ?
                """,
                (json.dumps(result), utc_now(), idempotency_key),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"unknown operation: {idempotency_key}")

    def publish_once(self, idempotency_key: str, report: str) -> str:
        """Emulate an external API that honors an idempotency key."""
        with self._connect() as connection:
            existing = connection.execute(
                """
                SELECT publication_id, report
                FROM publications
                WHERE idempotency_key = ?
                """,
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                if existing["report"] != report:
                    raise ValueError("idempotency key was reused with different input")
                return existing["publication_id"]

            publication_id = f"publication-{uuid4()}"
            connection.execute(
                """
                INSERT INTO publications(
                    idempotency_key, publication_id, report, created_at
                ) VALUES (?, ?, ?, ?)
                """,
                (idempotency_key, publication_id, report, utc_now()),
            )
            return publication_id

    def count_publications(self, idempotency_key: str | None = None) -> int:
        with self._connect() as connection:
            if idempotency_key is None:
                return connection.execute(
                    "SELECT COUNT(*) FROM publications"
                ).fetchone()[0]
            return connection.execute(
                "SELECT COUNT(*) FROM publications WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()[0]

    def save_artifact(
        self,
        *,
        run_id: str,
        step_name: str,
        name: str,
        path: str,
        sha256: str,
        size_bytes: int,
        media_type: str,
        repaired: bool = False,
    ) -> ArtifactSnapshot:
        """Insert or refresh metadata after artifact bytes are durable."""
        now = utc_now()
        with self._connect() as connection:
            existing = connection.execute(
                """
                SELECT id FROM artifacts
                WHERE run_id = ? AND step_name = ? AND name = ?
                """,
                (run_id, step_name, name),
            ).fetchone()
            artifact_id = existing["id"] if existing else str(uuid4())
            connection.execute(
                """
                INSERT INTO artifacts(
                    id, run_id, step_name, name, path, sha256,
                    size_bytes, media_type, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, step_name, name) DO UPDATE SET
                    path = excluded.path,
                    sha256 = excluded.sha256,
                    size_bytes = excluded.size_bytes,
                    media_type = excluded.media_type,
                    updated_at = excluded.updated_at
                """,
                (
                    artifact_id,
                    run_id,
                    step_name,
                    name,
                    path,
                    sha256,
                    size_bytes,
                    media_type,
                    now,
                    now,
                ),
            )
            self._insert_event(
                connection,
                run_id,
                "artifact_repaired" if repaired else "artifact_saved",
                step_name=step_name,
                payload={
                    "artifact_id": artifact_id,
                    "name": name,
                    "path": path,
                    "sha256": sha256,
                    "size_bytes": size_bytes,
                },
            )
        return self.get_artifact(artifact_id)

    def get_artifact(self, artifact_id: str) -> ArtifactSnapshot:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT id, run_id, step_name, name, path, sha256,
                       size_bytes, media_type
                FROM artifacts WHERE id = ?
                """,
                (artifact_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown artifact: {artifact_id}")
        return ArtifactSnapshot(
            id=row["id"],
            run_id=row["run_id"],
            step_name=row["step_name"],
            name=row["name"],
            path=row["path"],
            sha256=row["sha256"],
            size_bytes=row["size_bytes"],
            media_type=row["media_type"],
        )

    def list_artifacts(self, run_id: str) -> list[ArtifactSnapshot]:
        with self._connect() as connection:
            ids = [
                row["id"]
                for row in connection.execute(
                    "SELECT id FROM artifacts WHERE run_id = ? ORDER BY created_at",
                    (run_id,),
                ).fetchall()
            ]
        return [self.get_artifact(artifact_id) for artifact_id in ids]

    def validate_workflow(self, run_id: str, workflow_name: str, step_names: Sequence[str]) -> None:
        run = self.get_run(run_id)
        stored_names = [step.name for step in self.get_steps(run_id)]
        if run.workflow_name != workflow_name or stored_names != list(step_names):
            raise ValueError("the supplied workflow does not match the stored run")

    def claim_next_step(
        self,
        worker_id: str,
        *,
        lease_duration: timedelta,
        run_id: str | None = None,
        now: datetime | None = None,
    ) -> ClaimedStep | None:
        """Atomically give one eligible step to a worker for a limited time."""
        claimed_at = now or datetime.now(UTC)
        claimed_at_text = claimed_at.isoformat()
        expires_at = (claimed_at + lease_duration).isoformat()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT
                    s.run_id,
                    s.name,
                    s.status,
                    s.lease_owner,
                    s.attempt_count,
                    r.workflow_name,
                    r.status AS run_status
                FROM steps AS s
                JOIN runs AS r ON r.id = s.run_id
                WHERE r.status IN ('pending', 'running')
                  AND (
                      s.status = 'pending'
                      OR (
                          s.status = 'waiting_retry'
                          AND s.next_attempt_at <= ?
                      )
                      OR (
                          s.status = 'running'
                          AND s.lease_expires_at <= ?
                      )
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM steps AS earlier
                      WHERE earlier.run_id = s.run_id
                        AND earlier.position < s.position
                        AND earlier.status != 'succeeded'
                  )
                  AND (? IS NULL OR s.run_id = ?)
                ORDER BY r.created_at, s.position
                LIMIT 1
                """,
                (claimed_at_text, claimed_at_text, run_id, run_id),
            ).fetchone()
            if row is None:
                connection.commit()
                return None

            reclaimed = row["status"] == "running"
            if reclaimed:
                self._insert_event(
                    connection,
                    row["run_id"],
                    "step_lease_expired",
                    step_name=row["name"],
                    payload={"previous_worker_id": row["lease_owner"]},
                )

            connection.execute(
                """
                UPDATE steps
                SET status = 'running',
                    started_at = ?,
                    lease_owner = ?,
                    lease_expires_at = ?,
                    attempt_count = attempt_count + 1,
                    next_attempt_at = NULL
                WHERE run_id = ? AND name = ?
                """,
                (
                    claimed_at_text,
                    worker_id,
                    expires_at,
                    row["run_id"],
                    row["name"],
                ),
            )
            if row["run_status"] == "pending":
                connection.execute(
                    "UPDATE runs SET status = 'running', updated_at = ? WHERE id = ?",
                    (claimed_at_text, row["run_id"]),
                )
                self._insert_event(connection, row["run_id"], "run_started")
            self._insert_event(
                connection,
                row["run_id"],
                "step_claimed",
                step_name=row["name"],
                payload={
                    "worker_id": worker_id,
                    "lease_expires_at": expires_at,
                    "reclaimed": reclaimed,
                    "attempt_count": row["attempt_count"] + 1,
                },
            )
            connection.commit()
            return ClaimedStep(
                run_id=row["run_id"],
                workflow_name=row["workflow_name"],
                step_name=row["name"],
                worker_id=worker_id,
                lease_expires_at=expires_at,
                attempt_count=row["attempt_count"] + 1,
            )
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def renew_lease(
        self,
        claim: ClaimedStep,
        *,
        lease_duration: timedelta,
        now: datetime | None = None,
    ) -> ClaimedStep:
        """Extend a live lease, provided the caller still owns it."""
        renewed_at = now or datetime.now(UTC)
        renewed_at_text = renewed_at.isoformat()
        expires_at = (renewed_at + lease_duration).isoformat()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE steps
                SET lease_expires_at = ?
                WHERE run_id = ?
                  AND name = ?
                  AND status = 'running'
                  AND lease_owner = ?
                  AND lease_expires_at > ?
                """,
                (
                    expires_at,
                    claim.run_id,
                    claim.step_name,
                    claim.worker_id,
                    renewed_at_text,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("cannot renew a lease that is expired or not owned")
            self._insert_event(
                connection,
                claim.run_id,
                "step_heartbeat",
                step_name=claim.step_name,
                payload={
                    "worker_id": claim.worker_id,
                    "lease_expires_at": expires_at,
                },
            )
        return ClaimedStep(
            run_id=claim.run_id,
            workflow_name=claim.workflow_name,
            step_name=claim.step_name,
            worker_id=claim.worker_id,
            lease_expires_at=expires_at,
            attempt_count=claim.attempt_count,
        )

    def fail_claim(
        self,
        claim: ClaimedStep,
        error: Exception,
        *,
        retryable: bool,
        max_attempts: int,
        retry_at: datetime | None,
        now: datetime | None = None,
    ) -> str:
        """Persist a failed attempt and either schedule retry or fail the run."""
        failed_at = now or datetime.now(UTC)
        failed_at_text = failed_at.isoformat()
        will_retry = retryable and claim.attempt_count < max_attempts
        status = "waiting_retry" if will_retry else "failed"
        next_attempt_at = retry_at.isoformat() if will_retry and retry_at else None
        error_text = f"{type(error).__name__}: {error}"

        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE steps
                SET status = ?,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    next_attempt_at = ?,
                    last_error = ?
                WHERE run_id = ?
                  AND name = ?
                  AND status = 'running'
                  AND lease_owner = ?
                  AND lease_expires_at > ?
                """,
                (
                    status,
                    next_attempt_at,
                    error_text,
                    claim.run_id,
                    claim.step_name,
                    claim.worker_id,
                    failed_at_text,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("cannot fail a lease that is expired or not owned")
            event_type = "step_retry_scheduled" if will_retry else "step_failed"
            self._insert_event(
                connection,
                claim.run_id,
                event_type,
                step_name=claim.step_name,
                payload={
                    "worker_id": claim.worker_id,
                    "attempt_count": claim.attempt_count,
                    "error": error_text,
                    "next_attempt_at": next_attempt_at,
                },
            )
            if not will_retry:
                connection.execute(
                    "UPDATE runs SET status = 'failed', updated_at = ? WHERE id = ?",
                    (failed_at_text, claim.run_id),
                )
                self._insert_event(
                    connection,
                    claim.run_id,
                    "run_failed",
                    payload={"step_name": claim.step_name, "error": error_text},
                )
        return status

    def complete_claim(
        self,
        claim: ClaimedStep,
        output: Any,
        *,
        now: datetime | None = None,
    ) -> bool:
        """Commit a step only if its worker still holds an unexpired lease."""
        completed_at = now or datetime.now(UTC)
        completed_at_text = completed_at.isoformat()
        output_json = json.dumps(output)
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE steps
                SET status = 'succeeded',
                    output_json = ?,
                    completed_at = ?,
                    lease_owner = NULL,
                    lease_expires_at = NULL
                WHERE run_id = ?
                  AND name = ?
                  AND status = 'running'
                  AND lease_owner = ?
                  AND lease_expires_at > ?
                """,
                (
                    output_json,
                    completed_at_text,
                    claim.run_id,
                    claim.step_name,
                    claim.worker_id,
                    completed_at_text,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("cannot complete a lease that is expired or not owned")
            self._insert_event(
                connection,
                claim.run_id,
                "step_completed",
                step_name=claim.step_name,
                payload={"output": output, "worker_id": claim.worker_id},
            )
            unfinished = connection.execute(
                """
                SELECT COUNT(*) FROM steps
                WHERE run_id = ? AND status != 'succeeded'
                """,
                (claim.run_id,),
            ).fetchone()[0]
            run_completed = unfinished == 0
            if run_completed:
                connection.execute(
                    "UPDATE runs SET status = 'completed', updated_at = ? WHERE id = ?",
                    (completed_at_text, claim.run_id),
                )
                self._insert_event(connection, claim.run_id, "run_completed")
        return run_completed

    def _insert_event(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        event_type: str,
        *,
        step_name: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO events(run_id, step_name, event_type, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run_id, step_name, event_type, json.dumps(payload or {}), utc_now()),
        )
