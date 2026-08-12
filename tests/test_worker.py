from datetime import UTC, datetime, timedelta
import sqlite3

import pytest

from durable_agent_runner.demo import DemoServices, build_demo_workflow
from durable_agent_runner.durable_runner import DurableRunner
from durable_agent_runner.storage import SQLiteStore
from durable_agent_runner.worker import Worker


def make_store(tmp_path) -> SQLiteStore:
    store = SQLiteStore(tmp_path / "runner.db")
    store.initialize()
    return store


def test_database_initialization_is_safe_to_repeat(tmp_path) -> None:
    store = make_store(tmp_path)

    store.initialize()

    with sqlite3.connect(store.path) as connection:
        versions = connection.execute(
            "SELECT version FROM schema_version"
        ).fetchall()
    assert versions == [(3,)]


def test_second_worker_cannot_claim_a_step_with_a_live_lease(tmp_path) -> None:
    store = make_store(tmp_path)
    workflow = build_demo_workflow("Test task", DemoServices())
    run_id = DurableRunner(store).create_run(workflow)
    now = datetime(2026, 1, 1, tzinfo=UTC)

    first_claim = store.claim_next_step(
        "worker-a",
        run_id=run_id,
        lease_duration=timedelta(seconds=30),
        now=now,
    )
    second_claim = store.claim_next_step(
        "worker-b",
        run_id=run_id,
        lease_duration=timedelta(seconds=30),
        now=now,
    )

    assert first_claim is not None
    assert first_claim.step_name == "plan"
    assert second_claim is None


def test_heartbeat_extends_a_lease_then_another_worker_reclaims_it(tmp_path) -> None:
    store = make_store(tmp_path)
    workflow = build_demo_workflow("Test task", DemoServices())
    run_id = DurableRunner(store).create_run(workflow)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    duration = timedelta(seconds=10)
    first_claim = store.claim_next_step(
        "worker-a",
        run_id=run_id,
        lease_duration=duration,
        now=start,
    )
    assert first_claim is not None

    renewed_claim = store.renew_lease(
        first_claim,
        lease_duration=duration,
        now=start + timedelta(seconds=5),
    )
    too_early = store.claim_next_step(
        "worker-b",
        run_id=run_id,
        lease_duration=duration,
        now=start + timedelta(seconds=11),
    )
    reclaimed = store.claim_next_step(
        "worker-b",
        run_id=run_id,
        lease_duration=duration,
        now=start + timedelta(seconds=16),
    )

    assert renewed_claim.lease_expires_at.endswith("00:00:15+00:00")
    assert too_early is None
    assert reclaimed is not None
    assert reclaimed.step_name == "plan"

    with pytest.raises(RuntimeError, match="expired or not owned"):
        store.complete_claim(
            first_claim,
            ["stale output"],
            now=start + timedelta(seconds=17),
        )

    store.complete_claim(
        reclaimed,
        ["durability", "idempotency"],
        now=start + timedelta(seconds=17),
    )
    event_types = [event.event_type for event in store.get_events(run_id)]
    assert "step_heartbeat" in event_types
    assert "step_lease_expired" in event_types


def test_separate_workers_can_finish_one_workflow(tmp_path) -> None:
    store = make_store(tmp_path)
    services = DemoServices()
    workflow = build_demo_workflow("Test task", services)
    run_id = DurableRunner(store).create_run(workflow)

    def resolve_workflow(_name: str):
        return workflow

    results = [
        Worker(store, resolve_workflow, worker_id=f"worker-{index}").run_once(
            run_id=run_id
        )
        for index in range(4)
    ]

    assert [result.step_name for result in results if result] == [
        "plan",
        "collect",
        "write_report",
        "publish",
    ]
    assert results[-1] is not None
    assert results[-1].run_completed is True
    assert store.get_run(run_id).status == "completed"
