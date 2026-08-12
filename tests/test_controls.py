from durable_agent_runner.demo import DemoServices, build_demo_workflow
from durable_agent_runner.durable_runner import DurableRunner
from durable_agent_runner.storage import RunBudget, SQLiteStore
from durable_agent_runner.worker import Worker


def make_run(tmp_path, budget=RunBudget()):
    store = SQLiteStore(tmp_path / "runner.db")
    store.initialize()
    workflow = build_demo_workflow("Test task", DemoServices())
    run_id = DurableRunner(store).create_run(workflow, budget)
    worker = Worker(store, lambda _claim: workflow, worker_id="control-worker")
    return store, run_id, worker


def test_cancelled_run_cannot_claim_more_work(tmp_path) -> None:
    store, run_id, worker = make_run(tmp_path)

    assert store.cancel_run(run_id, "user requested stop") is True
    assert worker.run_once(run_id=run_id) is None

    run = store.get_run(run_id)
    assert run.status == "cancelled"
    assert run.terminal_reason == "user requested stop"
    assert [step.status for step in store.get_steps(run_id)] == ["pending"] * 4
    assert store.cancel_run(run_id) is False


def test_token_budget_is_checked_before_step_execution(tmp_path) -> None:
    store, run_id, worker = make_run(tmp_path, RunBudget(max_tokens=99))

    result = worker.run_once(run_id=run_id)

    assert result is not None and result.status == "failed"
    run = store.get_run(run_id)
    assert run.status == "failed"
    assert run.terminal_reason == "budget exceeded: max_tokens"
    assert run.used_tokens == 0
    assert store.get_steps(run_id)[0].status == "failed"


def test_step_budget_stops_before_third_distinct_step(tmp_path) -> None:
    store, run_id, worker = make_run(tmp_path, RunBudget(max_steps=2))

    worker.run_once(run_id=run_id)
    worker.run_once(run_id=run_id)
    result = worker.run_once(run_id=run_id)

    assert result is not None and result.status == "failed"
    run = store.get_run(run_id)
    assert run.used_steps == 2
    assert run.status == "failed"
    assert store.get_steps(run_id)[2].status == "failed"


def test_usage_accumulates_across_workers(tmp_path) -> None:
    store, run_id, worker = make_run(tmp_path)

    for _ in range(4):
        worker.run_once(run_id=run_id)

    run = store.get_run(run_id)
    assert run.status == "completed"
    assert run.used_attempts == 4
    assert run.used_steps == 4
    assert run.used_tokens == 150
    assert run.used_tool_calls == 2
    assert run.used_cost_micros == 2_250
