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
            connection.execute("DELETE FROM schema_version")
            connection.execute("INSERT INTO schema_version(version) VALUES (2)")
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
                SELECT name, position, status, output_json
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
                    r.workflow_name,
                    r.status AS run_status
                FROM steps AS s
                JOIN runs AS r ON r.id = s.run_id
                WHERE r.status IN ('pending', 'running')
                  AND (
                      s.status = 'pending'
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
                (claimed_at_text, run_id, run_id),
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
                    lease_expires_at = ?
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
                },
            )
            connection.commit()
            return ClaimedStep(
                run_id=row["run_id"],
                workflow_name=row["workflow_name"],
                step_name=row["name"],
                worker_id=worker_id,
                lease_expires_at=expires_at,
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
        )

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
