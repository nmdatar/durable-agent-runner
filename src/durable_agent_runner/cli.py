"""Command-line entry point for the durable agent runner."""

import argparse
from collections.abc import Sequence

from durable_agent_runner.demo import DemoServices, build_demo_workflow
from durable_agent_runner.runner import InMemoryRunner, RunnerEvent, SimulatedCrash

STEP_NAMES = ("plan", "collect", "write_report", "publish")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    demo = subparsers.add_parser("demo", help="run the in-memory demo workflow")
    demo.add_argument(
        "--crash-after",
        choices=STEP_NAMES,
        help="simulate process failure immediately after this step",
    )
    return parser


def print_event(event: RunnerEvent) -> None:
    print(f"{event.kind.upper():9} {event.step_name}")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the selected command."""
    args = build_parser().parse_args(argv)

    if args.command == "demo":
        services = DemoServices()
        workflow = build_demo_workflow("Why durability matters", services)
        runner = InMemoryRunner(observer=print_event)

        try:
            result = runner.run(workflow, crash_after=args.crash_after)
        except SimulatedCrash as error:
            print(f"\n{error}")
            print("Progress existed only in memory and cannot be resumed.")
            return 1

        print(f"\nREPORT\n{result.outputs['write_report']}")
        print(f"\nPUBLISHED {result.outputs['publish']}")
        return 0

    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
