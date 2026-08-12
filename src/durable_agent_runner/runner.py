"""The intentionally non-durable runner used as our baseline."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

StepAction = Callable[[Mapping[str, Any]], Any]
EventObserver = Callable[["RunnerEvent"], None]


@dataclass(frozen=True)
class RetryPolicy:
    """Bounded exponential-backoff settings for one step."""

    max_attempts: int = 3
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0

    def delay_after(self, attempt_count: int) -> float:
        delay = self.initial_delay_seconds * (2 ** (attempt_count - 1))
        return min(delay, self.max_delay_seconds)


@dataclass(frozen=True)
class StepUsage:
    """Estimated resources reserved before executing one step attempt."""

    tokens: int = 0
    tool_calls: int = 0
    cost_micros: int = 0


@dataclass(frozen=True)
class Step:
    """One named unit of work in a workflow."""

    name: str
    execute: StepAction
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    usage: StepUsage = field(default_factory=StepUsage)


@dataclass(frozen=True)
class Workflow:
    """An ordered collection of steps."""

    name: str
    steps: Sequence[Step]


@dataclass(frozen=True)
class RunnerEvent:
    """An observation emitted while the in-memory runner is alive."""

    kind: str
    step_name: str


@dataclass(frozen=True)
class RunResult:
    """The outputs produced by a completed workflow."""

    workflow_name: str
    outputs: Mapping[str, Any]


class SimulatedCrash(RuntimeError):
    """Raised after a selected step to imitate abrupt process loss."""

    def __init__(self, step_name: str) -> None:
        super().__init__(f"simulated crash after step: {step_name}")
        self.step_name = step_name


class InMemoryRunner:
    """Execute every workflow step in order and retain state only in memory."""

    def __init__(self, observer: EventObserver | None = None) -> None:
        self._observer = observer or (lambda _event: None)
        self._outputs: dict[str, Any] = {}

    @property
    def outputs(self) -> Mapping[str, Any]:
        """Return a snapshot of the progress held by this process."""
        return dict(self._outputs)

    def run(
        self,
        workflow: Workflow,
        *,
        crash_after: str | None = None,
    ) -> RunResult:
        """Run from the first step; there is no persisted resume point."""
        self._outputs = {}

        for step in workflow.steps:
            self._observer(RunnerEvent("started", step.name))
            self._outputs[step.name] = step.execute(self._outputs)
            self._observer(RunnerEvent("completed", step.name))

            if step.name == crash_after:
                self._observer(RunnerEvent("crashed", step.name))
                raise SimulatedCrash(step.name)

        return RunResult(workflow.name, dict(self._outputs))
