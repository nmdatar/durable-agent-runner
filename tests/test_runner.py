import pytest

from durable_agent_runner.demo import DemoServices, build_demo_workflow
from durable_agent_runner.runner import InMemoryRunner, RunnerEvent, SimulatedCrash


def test_runner_completes_every_step_in_order() -> None:
    events: list[RunnerEvent] = []
    services = DemoServices()
    workflow = build_demo_workflow("Test task", services)

    result = InMemoryRunner(observer=events.append).run(workflow)

    assert list(result.outputs) == ["plan", "collect", "write_report", "publish"]
    assert result.outputs["publish"] == "publication-1"
    assert [event.kind for event in events] == ["started", "completed"] * 4
    assert len(services.publications) == 1


def test_a_new_runner_restarts_from_the_beginning_after_a_crash() -> None:
    first_events: list[RunnerEvent] = []
    services = DemoServices()
    workflow = build_demo_workflow("Test task", services)
    first_runner = InMemoryRunner(observer=first_events.append)

    with pytest.raises(SimulatedCrash):
        first_runner.run(workflow, crash_after="collect")

    assert list(first_runner.outputs) == ["plan", "collect"]

    second_events: list[RunnerEvent] = []
    InMemoryRunner(observer=second_events.append).run(workflow)

    assert second_events[0] == RunnerEvent("started", "plan")


def test_restarting_after_an_unrecorded_publish_duplicates_the_side_effect() -> None:
    services = DemoServices()
    workflow = build_demo_workflow("Test task", services)

    with pytest.raises(SimulatedCrash):
        InMemoryRunner().run(workflow, crash_after="publish")

    InMemoryRunner().run(workflow)

    assert len(services.publications) == 2

