"""Deterministic, agent-shaped work for exercising the runner."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from durable_agent_runner.artifacts import ArtifactIntegrityError, ArtifactManager
from durable_agent_runner.errors import RetryableStepError, TerminalStepError
from durable_agent_runner.operations import IdempotentPublisher
from durable_agent_runner.runner import Step, Workflow


@dataclass
class DemoServices:
    """Predictable stand-ins for a model, search tool, and external publisher."""

    publications: list[str] = field(default_factory=list)
    failure_step: str | None = None
    failure_kind: str | None = None
    publisher: IdempotentPublisher | None = None
    artifacts: ArtifactManager | None = None
    run_id: str | None = None

    def _maybe_fail(self, step_name: str) -> None:
        if self.failure_step != step_name:
            return
        if self.failure_kind == "retryable":
            raise RetryableStepError(f"temporary failure in {step_name}")
        if self.failure_kind == "terminal":
            raise TerminalStepError(f"permanent failure in {step_name}")

    def plan(self, task: str) -> list[str]:
        self._maybe_fail("plan")
        return ["durability", "idempotency"]

    def collect(self, topics: list[str]) -> list[str]:
        self._maybe_fail("collect")
        facts = {
            "durability": "Durable progress survives process failure.",
            "idempotency": "An idempotent operation can be safely repeated.",
        }
        return [facts[topic] for topic in topics]

    def publish(self, report: str) -> str:
        self._maybe_fail("publish")
        if self.publisher is not None:
            return self.publisher.publish(report)
        publication_id = f"publication-{len(self.publications) + 1}"
        self.publications.append(report)
        return publication_id


def build_demo_workflow(task: str, services: DemoServices) -> Workflow:
    """Create a tiny research workflow with one external side effect."""

    def plan(_outputs: Mapping[str, Any]) -> list[str]:
        return services.plan(task)

    def collect(outputs: Mapping[str, Any]) -> list[str]:
        return services.collect(outputs["plan"])

    def render_report(outputs: Mapping[str, Any]) -> str:
        return f"# {task}\n\n" + "\n".join(outputs["collect"])

    def write_report(outputs: Mapping[str, Any]) -> Any:
        services._maybe_fail("write_report")
        report = render_report(outputs)
        if services.artifacts is None or services.run_id is None:
            return report
        return services.artifacts.write_text(
            run_id=services.run_id,
            step_name="write_report",
            name="report.md",
            content=report,
            media_type="text/markdown",
        )

    def publish(outputs: Mapping[str, Any]) -> str:
        report_output = outputs["write_report"]
        if services.artifacts is None or not isinstance(report_output, dict):
            return services.publish(report_output)
        try:
            report = services.artifacts.read_text(report_output)
        except ArtifactIntegrityError:
            if services.run_id is None:
                raise
            services.artifacts.write_text(
                run_id=services.run_id,
                step_name="write_report",
                name="report.md",
                content=render_report(outputs),
                media_type="text/markdown",
                repaired=True,
            )
            report = services.artifacts.read_text(report_output)
        return services.publish(report)

    return Workflow(
        name="demo-research",
        steps=(
            Step("plan", plan),
            Step("collect", collect),
            Step("write_report", write_report),
            Step("publish", publish),
        ),
    )
