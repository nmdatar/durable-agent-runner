"""Worker execution backed by expiring SQLite leases."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Event, Thread
from uuid import uuid4

from durable_agent_runner.errors import RetryableStepError
from durable_agent_runner.runner import EventObserver, RunnerEvent, SimulatedCrash, Workflow
from durable_agent_runner.storage import ClaimedStep, SQLiteStore

WorkflowResolver = Callable[[ClaimedStep], Workflow]
Clock = Callable[[], datetime]


@dataclass(frozen=True)
class WorkResult:
    run_id: str
    step_name: str
    run_completed: bool
    status: str
    attempt_count: int


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
        clock: Clock | None = None,
    ) -> None:
        self._store = store
        self._resolve_workflow = resolve_workflow
        self.worker_id = worker_id or f"worker-{uuid4()}"
        self._observer = observer or (lambda _event: None)
        self._clock = clock or (lambda: datetime.now(UTC))

    def run_once(
        self,
        *,
        run_id: str | None = None,
        lease_duration: timedelta = timedelta(seconds=30),
        crash_after_claim: str | None = None,
        crash_before_commit: str | None = None,
        crash_after_commit: str | None = None,
    ) -> WorkResult | None:
        """Claim one eligible step, execute it, and commit its output."""
        claim = self._store.claim_next_step(
            self.worker_id,
            lease_duration=lease_duration,
            run_id=run_id,
            now=self._clock(),
        )
        if claim is None:
            return None
        if claim.step_name == crash_after_claim:
            self._observer(RunnerEvent("crashed_after_claim", claim.step_name))
            raise SimulatedCrash(f"{claim.step_name}:after_claim")

        workflow = self._resolve_workflow(claim)
        self._store.validate_workflow(
            claim.run_id,
            workflow.name,
            [step.name for step in workflow.steps],
        )
        step = next(step for step in workflow.steps if step.name == claim.step_name)
        budget_error = self._store.reserve_budget(
            claim,
            tokens=step.usage.tokens,
            tool_calls=step.usage.tool_calls,
            cost_micros=step.usage.cost_micros,
            now=self._clock(),
        )
        if budget_error is not None:
            self._observer(RunnerEvent("budget_exceeded", step.name))
            return WorkResult(
                claim.run_id,
                step.name,
                run_completed=False,
                status="failed",
                attempt_count=claim.attempt_count,
            )
        outputs = {
            stored.name: stored.output
            for stored in self._store.get_steps(claim.run_id)
            if stored.status == "succeeded"
        }

        self._observer(RunnerEvent("claimed", step.name))
        try:
            with LeaseHeartbeat(self._store, claim, lease_duration):
                output = step.execute(outputs)
        except SimulatedCrash:
            raise
        except Exception as error:
            now = self._clock()
            policy = step.retry_policy
            retry_at = now + timedelta(
                seconds=policy.delay_after(claim.attempt_count)
            )
            status = self._store.fail_claim(
                claim,
                error,
                retryable=isinstance(error, RetryableStepError),
                max_attempts=policy.max_attempts,
                retry_at=retry_at,
                now=now,
            )
            self._observer(RunnerEvent(status, step.name))
            return WorkResult(
                claim.run_id,
                step.name,
                run_completed=False,
                status=status,
                attempt_count=claim.attempt_count,
            )
        if step.name == crash_before_commit:
            self._observer(RunnerEvent("crashed", step.name))
            raise SimulatedCrash(step.name)

        run_completed = self._store.complete_claim(claim, output, now=self._clock())
        self._observer(RunnerEvent("completed", step.name))
        if step.name == crash_after_commit:
            self._observer(RunnerEvent("crashed_after_commit", step.name))
            raise SimulatedCrash(f"{step.name}:after_commit")
        return WorkResult(
            claim.run_id,
            step.name,
            run_completed,
            status="succeeded",
            attempt_count=claim.attempt_count,
        )
