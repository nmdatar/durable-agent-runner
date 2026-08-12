import pytest

from durable_agent_runner.demo import DemoServices, build_demo_workflow
from durable_agent_runner.durable_runner import DurableRunner
from durable_agent_runner.runner import RunnerEvent, SimulatedCrash
from durable_agent_runner.storage import SQLiteStore


def make_store(tmp_path) -> SQLiteStore:
    store = SQLiteStore(tmp_path / "runner.db")
    store.initialize()
    return store


def test_new_process_resumes_after_last_completed_step(tmp_path) -> None:
    store = make_store(tmp_path)
    workflow = build_demo_workflow("Test task", DemoServices())
    run_id = DurableRunner(store).create_run(workflow)

    with pytest.raises(SimulatedCrash):
        DurableRunner(store).run(run_id, workflow, crash_after="collect")

    events: list[RunnerEvent] = []
    result = DurableRunner(store, observer=events.append).run(run_id, workflow)

    assert events[0] == RunnerEvent("claimed", "write_report")
    assert result.outputs["publish"] == "publication-1"
    assert store.get_run(run_id).status == "completed"
    assert [step.status for step in store.get_steps(run_id)] == ["succeeded"] * 4


def test_state_changes_have_matching_durable_events(tmp_path) -> None:
    store = make_store(tmp_path)
    workflow = build_demo_workflow("Test task", DemoServices())
    runner = DurableRunner(store)
    run_id = runner.create_run(workflow)

    runner.run(run_id, workflow)

    event_types = [event.event_type for event in store.get_events(run_id)]
    assert event_types == [
        "run_created",
        "run_started",
        "step_claimed",
        "budget_reserved",
        "step_completed",
        "step_claimed",
        "budget_reserved",
        "step_completed",
        "step_claimed",
        "budget_reserved",
        "step_completed",
        "step_claimed",
        "budget_reserved",
        "step_completed",
        "run_completed",
    ]
