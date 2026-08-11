"""A runner that resumes from step boundaries stored in SQLite."""

from durable_agent_runner.runner import (
    EventObserver,
    RunResult,
    RunnerEvent,
    SimulatedCrash,
    Workflow,
)
from durable_agent_runner.storage import SQLiteStore


class DurableRunner:
    """Execute a workflow while persisting every completed step."""

    def __init__(
        self,
        store: SQLiteStore,
        observer: EventObserver | None = None,
    ) -> None:
        self._store = store
        self._observer = observer or (lambda _event: None)

    def create_run(self, workflow: Workflow) -> str:
        return self._store.create_run(
            workflow.name,
            [step.name for step in workflow.steps],
        )

    def run(
        self,
        run_id: str,
        workflow: Workflow,
        *,
        crash_after: str | None = None,
    ) -> RunResult:
        """Resume a stored run and execute only unfinished steps."""
        step_names = [step.name for step in workflow.steps]
        self._store.validate_workflow(run_id, workflow.name, step_names)
        self._store.begin_or_resume_run(run_id)

        stored_steps = self._store.get_steps(run_id)
        outputs = {
            step.name: step.output
            for step in stored_steps
            if step.status == "succeeded"
        }

        for step in workflow.steps:
            if step.name in outputs:
                self._observer(RunnerEvent("skipped", step.name))
                continue

            self._store.start_step(run_id, step.name)
            self._observer(RunnerEvent("started", step.name))
            output = step.execute(outputs)
            self._store.complete_step(run_id, step.name, output)
            outputs[step.name] = output
            self._observer(RunnerEvent("completed", step.name))

            if step.name == crash_after:
                self._observer(RunnerEvent("crashed", step.name))
                raise SimulatedCrash(step.name)

        self._store.complete_run(run_id)
        return RunResult(workflow.name, outputs)

