from datetime import UTC, datetime, timedelta

import pytest

from durable_agent_runner.demo import DemoServices, build_demo_workflow
from durable_agent_runner.durable_runner import DurableRunner
from durable_agent_runner.operations import IdempotentPublisher
from durable_agent_runner.runner import SimulatedCrash
from durable_agent_runner.storage import SQLiteStore
from durable_agent_runner.worker import Worker


class ManualClock:
    def __init__(self) -> None:
        self.now = datetime(2026, 1, 1, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now


def test_publish_is_not_duplicated_after_ambiguous_crash(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "runner.db")
    store.initialize()
    initial_workflow = build_demo_workflow("Test task", DemoServices())
    run_id = DurableRunner(store).create_run(initial_workflow)
    clock = ManualClock()

    def resolve(claim, *, crash_after_effect=False):
        return build_demo_workflow(
            "Test task",
            DemoServices(
                publisher=IdempotentPublisher(
                    store,
                    claim.run_id,
                    crash_after_effect=crash_after_effect,
                )
            ),
        )

    worker = Worker(store, resolve, worker_id="setup-worker", clock=clock)
    for _ in range(3):
        worker.run_once(run_id=run_id)

    crashing_worker = Worker(
        store,
        lambda claim: resolve(claim, crash_after_effect=True),
        worker_id="crashing-worker",
        clock=clock,
    )
    with pytest.raises(SimulatedCrash):
        crashing_worker.run_once(run_id=run_id, lease_duration=timedelta(seconds=5))

    key = f"{run_id}:publish:v1"
    assert store.count_publications(key) == 1
    assert store.get_operation(key).status == "started"

    clock.now += timedelta(seconds=6)
    recovering_worker = Worker(
        store,
        resolve,
        worker_id="recovering-worker",
        clock=clock,
    )
    result = recovering_worker.run_once(
        run_id=run_id,
        lease_duration=timedelta(seconds=5),
    )

    assert result is not None and result.run_completed
    assert store.count_publications(key) == 1
    operation = store.get_operation(key)
    assert operation is not None and operation.status == "completed"
    assert store.get_steps(run_id)[-1].attempt_count == 2


def test_idempotency_key_rejects_different_input(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "runner.db")
    store.initialize()
    workflow = build_demo_workflow("Test task", DemoServices())
    run_id = DurableRunner(store).create_run(workflow)
    key = f"{run_id}:publish:v1"

    store.publish_once(key, "first report")

    with pytest.raises(ValueError, match="different input"):
        store.publish_once(key, "different report")
