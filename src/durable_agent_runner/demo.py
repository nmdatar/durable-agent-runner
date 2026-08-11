"""Deterministic, agent-shaped work for exercising the runner."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from durable_agent_runner.runner import Step, Workflow


@dataclass
class DemoServices:
    """Predictable stand-ins for a model, search tool, and external publisher."""

    publications: list[str] = field(default_factory=list)

    def plan(self, task: str) -> list[str]:
        return ["durability", "idempotency"]

    def collect(self, topics: list[str]) -> list[str]:
        facts = {
            "durability": "Durable progress survives process failure.",
            "idempotency": "An idempotent operation can be safely repeated.",
        }
        return [facts[topic] for topic in topics]

    def publish(self, report: str) -> str:
        publication_id = f"publication-{len(self.publications) + 1}"
        self.publications.append(report)
        return publication_id


def build_demo_workflow(task: str, services: DemoServices) -> Workflow:
    """Create a tiny research workflow with one external side effect."""

    def plan(_outputs: Mapping[str, Any]) -> list[str]:
        return services.plan(task)

    def collect(outputs: Mapping[str, Any]) -> list[str]:
        return services.collect(outputs["plan"])

    def write_report(outputs: Mapping[str, Any]) -> str:
        return f"# {task}\n\n" + "\n".join(outputs["collect"])

    def publish(outputs: Mapping[str, Any]) -> str:
        return services.publish(outputs["write_report"])

    return Workflow(
        name="demo-research",
        steps=(
            Step("plan", plan),
            Step("collect", collect),
            Step("write_report", write_report),
            Step("publish", publish),
        ),
    )

