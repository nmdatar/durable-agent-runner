"""Command-line entry point for the durable agent runner."""

import argparse
from collections.abc import Sequence
from pathlib import Path

from durable_agent_runner.demo import DemoServices, build_demo_workflow
from durable_agent_runner.durable_runner import DurableRunner
from durable_agent_runner.runner import InMemoryRunner, RunnerEvent, SimulatedCrash
from durable_agent_runner.storage import SQLiteStore

STEP_NAMES = ("plan", "collect", "write_report", "publish")
DEFAULT_DB = Path(".runner/runner.db")


def add_database_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help=f"SQLite database path (default: {DEFAULT_DB})",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    demo = subparsers.add_parser("demo", help="run the in-memory demo workflow")
    demo.add_argument(
        "--crash-after",
        choices=STEP_NAMES,
        help="simulate process failure immediately after this step",
    )

    init = subparsers.add_parser("init", help="initialize the SQLite database")
    add_database_argument(init)

    start = subparsers.add_parser("start", help="create and execute a durable run")
    start.add_argument("workflow", choices=("demo",))
    start.add_argument("--crash-after", choices=STEP_NAMES)
    add_database_argument(start)

    resume = subparsers.add_parser("resume", help="resume a durable run")
    resume.add_argument("run_id")
    resume.add_argument("--crash-after", choices=STEP_NAMES)
    add_database_argument(resume)

    inspect = subparsers.add_parser("inspect", help="show current run state")
    inspect.add_argument("run_id")
    add_database_argument(inspect)

    events = subparsers.add_parser("events", help="show durable event history")
    events.add_argument("run_id")
    add_database_argument(events)
    return parser


def print_event(event: RunnerEvent) -> None:
    print(f"{event.kind.upper():9} {event.step_name}")


def open_store(path: Path) -> SQLiteStore:
    store = SQLiteStore(path)
    store.initialize()
    return store


def run_durable(
    store: SQLiteStore,
    run_id: str,
    *,
    crash_after: str | None,
) -> int:
    services = DemoServices()
    workflow = build_demo_workflow("Why durability matters", services)
    runner = DurableRunner(store, observer=print_event)
    try:
        result = runner.run(run_id, workflow, crash_after=crash_after)
    except SimulatedCrash as error:
        print(f"\n{error}")
        print(f"Resume with: runner resume {run_id}")
        return 1
    print(f"\nPUBLISHED {result.outputs['publish']}")
    return 0


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

    if args.command == "init":
        open_store(args.db)
        print(f"Initialized {args.db}")
        return 0

    if args.command == "start":
        store = open_store(args.db)
        workflow = build_demo_workflow("Why durability matters", DemoServices())
        run_id = DurableRunner(store).create_run(workflow)
        print(f"RUN_ID {run_id}")
        return run_durable(store, run_id, crash_after=args.crash_after)

    if args.command == "resume":
        store = open_store(args.db)
        return run_durable(store, args.run_id, crash_after=args.crash_after)

    if args.command == "inspect":
        store = open_store(args.db)
        run = store.get_run(args.run_id)
        print(f"RUN {run.id} {run.workflow_name} {run.status}")
        for step in store.get_steps(args.run_id):
            print(f"{step.position:02} {step.status:9} {step.name}")
        return 0

    if args.command == "events":
        store = open_store(args.db)
        for event in store.get_events(args.run_id):
            step = f" {event.step_name}" if event.step_name else ""
            print(f"{event.id:04} {event.event_type}{step}")
        return 0

    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
