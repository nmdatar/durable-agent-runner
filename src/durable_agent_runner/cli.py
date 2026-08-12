"""Command-line entry point for the durable agent runner."""

import argparse
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

from durable_agent_runner.demo import DemoServices, build_demo_workflow
from durable_agent_runner.durable_runner import DurableRunner
from durable_agent_runner.runner import InMemoryRunner, RunnerEvent, SimulatedCrash
from durable_agent_runner.storage import SQLiteStore
from durable_agent_runner.worker import Worker

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

    start = subparsers.add_parser("start", help="enqueue a durable run")
    start.add_argument("workflow", choices=("demo",))
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

    worker = subparsers.add_parser("worker", help="claim and execute queued work")
    worker.add_argument("--once", action="store_true", help="execute at most one step")
    worker.add_argument("--run-id", help="only claim work from this run")
    worker.add_argument("--worker-id", help="stable identity to record with the lease")
    worker.add_argument("--lease-seconds", type=int, default=30)
    worker.add_argument("--crash-before-commit", choices=STEP_NAMES)
    failure = worker.add_mutually_exclusive_group()
    failure.add_argument("--retryable-failure", choices=STEP_NAMES)
    failure.add_argument("--terminal-failure", choices=STEP_NAMES)
    add_database_argument(worker)
    return parser


def print_event(event: RunnerEvent) -> None:
    print(f"{event.kind.upper():9} {event.step_name}")


def open_store(path: Path) -> SQLiteStore:
    store = SQLiteStore(path)
    store.initialize()
    return store


def resolve_demo_workflow(
    workflow_name: str,
    *,
    failure_step: str | None = None,
    failure_kind: str | None = None,
):
    if workflow_name != "demo-research":
        raise ValueError(f"unknown workflow: {workflow_name}")
    return build_demo_workflow(
        "Why durability matters",
        DemoServices(failure_step=failure_step, failure_kind=failure_kind),
    )


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
        print(f"QUEUED  {workflow.name}")
        print(f"Run one step with: runner worker --once --run-id {run_id}")
        return 0

    if args.command == "resume":
        store = open_store(args.db)
        return run_durable(store, args.run_id, crash_after=args.crash_after)

    if args.command == "inspect":
        store = open_store(args.db)
        run = store.get_run(args.run_id)
        print(f"RUN {run.id} {run.workflow_name} {run.status}")
        for step in store.get_steps(args.run_id):
            retry = f" next={step.next_attempt_at}" if step.next_attempt_at else ""
            error = f" error={step.last_error}" if step.last_error else ""
            print(
                f"{step.position:02} {step.status:13} {step.name} "
                f"attempts={step.attempt_count}{retry}{error}"
            )
        return 0

    if args.command == "events":
        store = open_store(args.db)
        for event in store.get_events(args.run_id):
            step = f" {event.step_name}" if event.step_name else ""
            print(f"{event.id:04} {event.event_type}{step}")
        return 0

    if args.command == "worker":
        if not args.once:
            raise SystemExit("worker currently requires --once")
        store = open_store(args.db)
        worker_id = args.worker_id or f"worker-{uuid4()}"
        failure_step = args.retryable_failure or args.terminal_failure
        failure_kind = "retryable" if args.retryable_failure else "terminal"
        worker = Worker(
            store,
            lambda workflow_name: resolve_demo_workflow(
                workflow_name,
                failure_step=failure_step,
                failure_kind=failure_kind if failure_step else None,
            ),
            worker_id=worker_id,
            observer=print_event,
        )
        try:
            work = worker.run_once(
                run_id=args.run_id,
                lease_duration=timedelta(seconds=args.lease_seconds),
                crash_before_commit=args.crash_before_commit,
            )
        except SimulatedCrash as error:
            print(f"\n{error}; its lease must expire before another worker can claim it")
            return 1
        if work is None:
            print("NO_WORK")
            return 0
        state = "RUN_COMPLETED" if work.run_completed else "STEP_COMPLETED"
        if work.status == "waiting_retry":
            state = "RETRY_SCHEDULED"
        elif work.status == "failed":
            state = "RUN_FAILED"
        print(
            f"{state} {work.run_id} {work.step_name} "
            f"worker={worker_id} attempt={work.attempt_count}"
        )
        return 0

    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
