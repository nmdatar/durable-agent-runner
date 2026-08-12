"""Convenience facade for creating and locally draining durable runs."""

from uuid import uuid4

from durable_agent_runner.runner import (
    EventObserver,
    RunResult,
    SimulatedCrash,
    Workflow,
)
from durable_agent_runner.storage import RunBudget, SQLiteStore
from durable_agent_runner.worker import Worker


class DurableRunner:
    """Execute a workflow while persisting every completed step."""

    def __init__(
        self,
        store: SQLiteStore,
        observer: EventObserver | None = None,
        worker_id: str | None = None,
    ) -> None:
        self._store = store
        self._observer = observer or (lambda _event: None)
        self._worker_id = worker_id or f"local-runner-{uuid4()}"

    def create_run(
        self,
        workflow: Workflow,
        budget: RunBudget | None = None,
    ) -> str:
        return self._store.create_run(
            workflow.name,
            [step.name for step in workflow.steps],
            budget,
        )

    def run(
        self,
        run_id: str,
        workflow: Workflow,
        *,
        crash_after: str | None = None,
    ) -> RunResult:
        """Drain one run locally while still obeying worker leases."""
        step_names = [step.name for step in workflow.steps]
        self._store.validate_workflow(run_id, workflow.name, step_names)

        def resolve_workflow(claim) -> Workflow:
            if claim.workflow_name != workflow.name:
                raise ValueError(f"unknown workflow: {claim.workflow_name}")
            return workflow

        worker = Worker(
            self._store,
            resolve_workflow,
            worker_id=self._worker_id,
            observer=self._observer,
        )

        while self._store.get_run(run_id).status != "completed":
            work = worker.run_once(run_id=run_id)
            if work is None:
                raise RuntimeError("run has work, but no step is currently claimable")
            if work.step_name == crash_after:
                raise SimulatedCrash(work.step_name)

        outputs = {
            step.name: step.output
            for step in self._store.get_steps(run_id)
            if step.status == "succeeded"
        }
        return RunResult(workflow.name, outputs)
