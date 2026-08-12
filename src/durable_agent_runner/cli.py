"""Command-line entry point for the durable agent runner."""

import argparse
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

from durable_agent_runner.artifacts import ArtifactManager
from durable_agent_runner.demo import DemoServices, build_demo_workflow
from durable_agent_runner.durable_runner import DurableRunner
from durable_agent_runner.operations import IdempotentPublisher
from durable_agent_runner.runner import InMemoryRunner, RunnerEvent, SimulatedCrash
from durable_agent_runner.storage import RunBudget, SQLiteStore
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
    start.add_argument("--max-attempts", type=int)
    start.add_argument("--max-steps", type=int)
    start.add_argument("--max-tokens", type=int)
    start.add_argument("--max-tool-calls", type=int)
    start.add_argument("--max-cost-micros", type=int)
    add_database_argument(start)

    cancel = subparsers.add_parser("cancel", help="cancel a pending or running run")
    cancel.add_argument("run_id")
    cancel.add_argument("--reason", default="cancelled by user")
    add_database_argument(cancel)

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

    artifacts = subparsers.add_parser("artifacts", help="show durable run artifacts")
    artifacts.add_argument("run_id")
    add_database_argument(artifacts)

    worker = subparsers.add_parser("worker", help="claim and execute queued work")
    worker.add_argument("--once", action="store_true", help="execute at most one step")
    worker.add_argument("--run-id", help="only claim work from this run")
    worker.add_argument("--worker-id", help="stable identity to record with the lease")
    worker.add_argument("--lease-seconds", type=int, default=30)
    worker.add_argument("--crash-before-commit", choices=STEP_NAMES)
    worker.add_argument(
        "--crash-after-side-effect",
        action="store_true",
        help="crash publish after the external effect but before recording its result",
    )
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
    claim,
    store: SQLiteStore,
    *,
    failure_step: str | None = None,
    failure_kind: str | None = None,
    crash_after_effect: bool = False,
):
    if claim.workflow_name != "demo-research":
        raise ValueError(f"unknown workflow: {claim.workflow_name}")
    return build_demo_workflow(
        "Why durability matters",
        DemoServices(
            failure_step=failure_step,
            failure_kind=failure_kind,
            publisher=IdempotentPublisher(
                store,
                claim.run_id,
                crash_after_effect=crash_after_effect,
            ),
            artifacts=ArtifactManager(store, store.path.parent / "artifacts"),
            run_id=claim.run_id,
        ),
    )


def run_durable(
    store: SQLiteStore,
    run_id: str,
    *,
    crash_after: str | None,
) -> int:
    services = DemoServices(
        publisher=IdempotentPublisher(store, run_id),
        artifacts=ArtifactManager(store, store.path.parent / "artifacts"),
        run_id=run_id,
    )
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
        run_id = DurableRunner(store).create_run(
            workflow,
            RunBudget(
                max_attempts=args.max_attempts,
                max_steps=args.max_steps,
                max_tokens=args.max_tokens,
                max_tool_calls=args.max_tool_calls,
                max_cost_micros=args.max_cost_micros,
            ),
        )
        print(f"RUN_ID {run_id}")
        print(f"QUEUED  {workflow.name}")
        print(f"Run one step with: runner worker --once --run-id {run_id}")
        return 0

    if args.command == "cancel":
        store = open_store(args.db)
        changed = store.cancel_run(args.run_id, args.reason)
        print("CANCELLED" if changed else "UNCHANGED")
        return 0

    if args.command == "resume":
        store = open_store(args.db)
        return run_durable(store, args.run_id, crash_after=args.crash_after)

    if args.command == "inspect":
        store = open_store(args.db)
        run = store.get_run(args.run_id)
        print(f"RUN {run.id} {run.workflow_name} {run.status}")
        if run.terminal_reason:
            print(f"REASON {run.terminal_reason}")
        print(
            "USAGE "
            f"attempts={run.used_attempts} steps={run.used_steps} "
            f"tokens={run.used_tokens} tool_calls={run.used_tool_calls} "
            f"cost_micros={run.used_cost_micros}"
        )
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

    if args.command == "artifacts":
        store = open_store(args.db)
        manager = ArtifactManager(store, store.path.parent / "artifacts")
        for artifact in store.list_artifacts(args.run_id):
            print(
                f"{artifact.id} {artifact.name} {artifact.media_type} "
                f"{artifact.size_bytes}B sha256={artifact.sha256} "
                f"path={manager.path_for(artifact)}"
            )
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
            lambda claim: resolve_demo_workflow(
                claim,
                store,
                failure_step=failure_step,
                failure_kind=failure_kind if failure_step else None,
                crash_after_effect=args.crash_after_side_effect,
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
