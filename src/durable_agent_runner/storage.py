"""SQLite persistence for workflow state and event history."""

import json
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
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

                INSERT OR IGNORE INTO schema_version(version) VALUES (1);

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

    def begin_or_resume_run(self, run_id: str) -> list[str]:
        """Mark interrupted steps pending and return their names."""
        now = utc_now()
        with self._connect() as connection:
            interrupted = [
                row["name"]
                for row in connection.execute(
                    "SELECT name FROM steps WHERE run_id = ? AND status = 'running'",
                    (run_id,),
                ).fetchall()
            ]
            for step_name in interrupted:
                connection.execute(
                    """
                    UPDATE steps
                    SET status = 'pending', started_at = NULL
                    WHERE run_id = ? AND name = ?
                    """,
                    (run_id, step_name),
                )
                self._insert_event(
                    connection,
                    run_id,
                    "step_recovered",
                    step_name=step_name,
                )
            connection.execute(
                "UPDATE runs SET status = 'running', updated_at = ? WHERE id = ?",
                (now, run_id),
            )
            self._insert_event(connection, run_id, "run_started")
        return interrupted

    def start_step(self, run_id: str, step_name: str) -> None:
        now = utc_now()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE steps
                SET status = 'running', started_at = ?
                WHERE run_id = ? AND name = ? AND status = 'pending'
                """,
                (now, run_id, step_name),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(f"step is not pending: {step_name}")
            self._insert_event(
                connection,
                run_id,
                "step_started",
                step_name=step_name,
            )

    def complete_step(self, run_id: str, step_name: str, output: Any) -> None:
        now = utc_now()
        output_json = json.dumps(output)
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE steps
                SET status = 'succeeded', output_json = ?, completed_at = ?
                WHERE run_id = ? AND name = ? AND status = 'running'
                """,
                (output_json, now, run_id, step_name),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(f"step is not running: {step_name}")
            self._insert_event(
                connection,
                run_id,
                "step_completed",
                step_name=step_name,
                payload={"output": output},
            )

    def complete_run(self, run_id: str) -> None:
        now = utc_now()
        with self._connect() as connection:
            incomplete_count = connection.execute(
                """
                SELECT COUNT(*) FROM steps
                WHERE run_id = ? AND status != 'succeeded'
                """,
                (run_id,),
            ).fetchone()[0]
            if incomplete_count:
                raise RuntimeError("cannot complete a run with unfinished steps")
            connection.execute(
                "UPDATE runs SET status = 'completed', updated_at = ? WHERE id = ?",
                (now, run_id),
            )
            self._insert_event(connection, run_id, "run_completed")

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

