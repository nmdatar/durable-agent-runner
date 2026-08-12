"""Worker execution backed by expiring SQLite leases."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from threading import Event, Thread
from uuid import uuid4

from durable_agent_runner.runner import EventObserver, RunnerEvent, SimulatedCrash, Workflow
from durable_agent_runner.storage import ClaimedStep, SQLiteStore

WorkflowResolver = Callable[[str], Workflow]


@dataclass(frozen=True)
class WorkResult:
    run_id: str
    step_name: str
    run_completed: bool


class LeaseHeartbeat:
    """Renew a lease in the background while a step is executing."""

    def __init__(
        self,
        store: SQLiteStore,
        claim: ClaimedStep,
        lease_duration: timedelta,
    ) -> None:
        self._store = store
        self._claim = claim
        self._lease_duration = lease_duration
        self._stopped = Event()
        self._lost = Event()
        self._thread = Thread(target=self._run, daemon=True)

    def __enter__(self) -> "LeaseHeartbeat":
        self._thread.start()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self._stopped.set()
        self._thread.join()
        if self._lost.is_set() and _type is None:
            raise RuntimeError("worker lost its lease while executing the step")

    def _run(self) -> None:
        interval = max(self._lease_duration.total_seconds() / 3, 0.01)
        while not self._stopped.wait(interval):
            try:
                self._claim = self._store.renew_lease(
                    self._claim,
                    lease_duration=self._lease_duration,
                )
            except RuntimeError:
                self._lost.set()
                return


class Worker:
    """Claim and execute at most one workflow step at a time."""

    def __init__(
        self,
        store: SQLiteStore,
        resolve_workflow: WorkflowResolver,
        *,
        worker_id: str | None = None,
        observer: EventObserver | None = None,
    ) -> None:
        self._store = store
        self._resolve_workflow = resolve_workflow
        self.worker_id = worker_id or f"worker-{uuid4()}"
        self._observer = observer or (lambda _event: None)

    def run_once(
        self,
        *,
        run_id: str | None = None,
        lease_duration: timedelta = timedelta(seconds=30),
        crash_before_commit: str | None = None,
    ) -> WorkResult | None:
        """Claim one eligible step, execute it, and commit its output."""
        claim = self._store.claim_next_step(
            self.worker_id,
            lease_duration=lease_duration,
            run_id=run_id,
        )
        if claim is None:
            return None

        workflow = self._resolve_workflow(claim.workflow_name)
        self._store.validate_workflow(
            claim.run_id,
            workflow.name,
            [step.name for step in workflow.steps],
        )
        step = next(step for step in workflow.steps if step.name == claim.step_name)
        outputs = {
            stored.name: stored.output
            for stored in self._store.get_steps(claim.run_id)
            if stored.status == "succeeded"
        }

        self._observer(RunnerEvent("claimed", step.name))
        with LeaseHeartbeat(self._store, claim, lease_duration):
            output = step.execute(outputs)
        if step.name == crash_before_commit:
            self._observer(RunnerEvent("crashed", step.name))
            raise SimulatedCrash(step.name)

        run_completed = self._store.complete_claim(claim, output)
        self._observer(RunnerEvent("completed", step.name))
        return WorkResult(claim.run_id, step.name, run_completed)

