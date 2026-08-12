"""Deterministic crash campaign for validating recovery guarantees."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from durable_agent_runner.artifacts import ArtifactManager
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

    def advance_past(self, duration: timedelta) -> None:
        self.now += duration + timedelta(milliseconds=1)


@dataclass(frozen=True)
class ChaosReport:
    run_id: str
    status: str
    injected_crashes: tuple[str, ...]
    publish_attempts: int
    publication_count: int
    event_types: tuple[str, ...]


def run_chaos_campaign(store: SQLiteStore, artifact_root: str | Path) -> ChaosReport:
    """Crash at four boundaries, recover, and verify one correct result."""
    clock = ManualClock()
    lease_duration = timedelta(seconds=5)
    initial_workflow = build_demo_workflow("Chaos test", DemoServices())
    run_id = DurableRunner(store).create_run(initial_workflow)
    artifacts = ArtifactManager(store, artifact_root)
    injected: list[str] = []

    def worker(*, crash_after_effect: bool = False) -> Worker:
        def resolve(claim):
            return build_demo_workflow(
                "Chaos test",
                DemoServices(
                    publisher=IdempotentPublisher(
                        store,
                        claim.run_id,
                        crash_after_effect=crash_after_effect,
                    ),
                    artifacts=artifacts,
                    run_id=claim.run_id,
                ),
            )

        return Worker(
            store,
            resolve,
            worker_id=f"chaos-worker-{len(injected) + 1}",
            clock=clock,
        )

    def expect_crash(label: str, action) -> None:
        try:
            action()
        except SimulatedCrash:
            injected.append(label)
            return
        raise AssertionError(f"expected injected crash: {label}")

    expect_crash(
        "plan:after_claim",
        lambda: worker().run_once(
            run_id=run_id,
            lease_duration=lease_duration,
            crash_after_claim="plan",
        ),
    )
    clock.advance_past(lease_duration)
    worker().run_once(run_id=run_id, lease_duration=lease_duration)

    expect_crash(
        "collect:before_commit",
        lambda: worker().run_once(
            run_id=run_id,
            lease_duration=lease_duration,
            crash_before_commit="collect",
        ),
    )
    clock.advance_past(lease_duration)
    worker().run_once(run_id=run_id, lease_duration=lease_duration)

    expect_crash(
        "write_report:after_commit",
        lambda: worker().run_once(
            run_id=run_id,
            lease_duration=lease_duration,
            crash_after_commit="write_report",
        ),
    )

    expect_crash(
        "publish:after_side_effect",
        lambda: worker(crash_after_effect=True).run_once(
            run_id=run_id,
            lease_duration=lease_duration,
        ),
    )
    clock.advance_past(lease_duration)
    result = worker().run_once(run_id=run_id, lease_duration=lease_duration)

    run = store.get_run(run_id)
    publish_step = next(
        step for step in store.get_steps(run_id) if step.name == "publish"
    )
    publication_count = store.count_publications(f"{run_id}:publish:v1")
    if result is None or not result.run_completed or run.status != "completed":
        raise AssertionError("chaos campaign did not converge to completion")
    if publication_count != 1:
        raise AssertionError("chaos campaign created duplicate publications")
    if any(step.status != "succeeded" for step in store.get_steps(run_id)):
        raise AssertionError("chaos campaign left unfinished steps")

    return ChaosReport(
        run_id=run_id,
        status=run.status,
        injected_crashes=tuple(injected),
        publish_attempts=publish_step.attempt_count,
        publication_count=publication_count,
        event_types=tuple(event.event_type for event in store.get_events(run_id)),
    )
