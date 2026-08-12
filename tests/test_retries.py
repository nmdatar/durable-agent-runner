from datetime import UTC, datetime, timedelta

from durable_agent_runner.durable_runner import DurableRunner
from durable_agent_runner.errors import RetryableStepError, TerminalStepError
from durable_agent_runner.runner import RetryPolicy, Step, Workflow
from durable_agent_runner.storage import SQLiteStore
from durable_agent_runner.worker import Worker


class ManualClock:
    def __init__(self) -> None:
        self.now = datetime(2026, 1, 1, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def make_runner(tmp_path, action, policy=None):
    store = SQLiteStore(tmp_path / "runner.db")
    store.initialize()
    workflow = Workflow(
        "retry-test",
        (Step("unstable", action, policy or RetryPolicy()),),
    )
    run_id = DurableRunner(store).create_run(workflow)
    clock = ManualClock()
    worker = Worker(
        store,
        lambda _claim: workflow,
        worker_id="retry-worker",
        clock=clock,
    )
    return store, run_id, worker, clock


def test_retryable_failure_waits_then_succeeds(tmp_path) -> None:
    calls = 0

    def flaky(_outputs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RetryableStepError("service unavailable")
        return "ok"

    policy = RetryPolicy(max_attempts=3, initial_delay_seconds=5)
    store, run_id, worker, clock = make_runner(tmp_path, flaky, policy)

    first = worker.run_once(run_id=run_id)
    too_early = worker.run_once(run_id=run_id)
    clock.advance(5)
    second = worker.run_once(run_id=run_id)

    assert first is not None and first.status == "waiting_retry"
    assert too_early is None
    assert second is not None and second.status == "succeeded"
    step = store.get_steps(run_id)[0]
    assert step.attempt_count == 2
    assert step.last_error == "RetryableStepError: service unavailable"
    assert store.get_run(run_id).status == "completed"


def test_retry_exhaustion_fails_the_run(tmp_path) -> None:
    def always_fails(_outputs):
        raise RetryableStepError("still unavailable")

    policy = RetryPolicy(max_attempts=2, initial_delay_seconds=5)
    store, run_id, worker, clock = make_runner(tmp_path, always_fails, policy)

    first = worker.run_once(run_id=run_id)
    clock.advance(5)
    second = worker.run_once(run_id=run_id)

    assert first is not None and first.status == "waiting_retry"
    assert second is not None and second.status == "failed"
    assert store.get_run(run_id).status == "failed"
    assert store.get_steps(run_id)[0].attempt_count == 2


def test_terminal_failure_does_not_retry(tmp_path) -> None:
    def invalid_input(_outputs):
        raise TerminalStepError("invalid input")

    store, run_id, worker, _clock = make_runner(tmp_path, invalid_input)

    result = worker.run_once(run_id=run_id)

    assert result is not None and result.status == "failed"
    assert store.get_run(run_id).status == "failed"
    assert worker.run_once(run_id=run_id) is None
