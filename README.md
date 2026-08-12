# Durable Agent Runner

A teaching implementation of the correctness layer beneath a long-running agent.
It persists workflow progress, coordinates workers with expiring leases, retries
transient failures, protects supported side effects with idempotency keys, verifies
file artifacts, enforces run controls, and demonstrates recovery under crashes.

This is intentionally a small single-machine system. It explains durable execution;
it is not a production service or a persistent VM platform.

## Quick start

Requirements: Python 3.11+ and [`uv`](https://docs.astral.sh/uv/).

```bash
uv sync
uv run pytest
uv run runner init
uv run runner start demo
```

`start` prints a run UUID. Use that UUID in place of `<run-id>` below. Each worker
invocation executes at most one eligible step:

```bash
uv run runner worker --once --run-id <run-id>
uv run runner inspect <run-id>
uv run runner events <run-id>
```

The demo has four sequential steps:

```text
plan → collect → write_report → publish
```

Run four workers to finish it, or let workers omit `--run-id` to claim the oldest
eligible step from any run.

## What to read

- [Architecture](docs/ARCHITECTURE.md) — components, data model, state machines,
  transaction boundaries, and the relationship to sandbox infrastructure.
- [Failure semantics](docs/FAILURE_SEMANTICS.md) — guarantees, crash behavior,
  retries, leases, idempotency, and known limitations.
- [Hands-on guide](docs/HANDS_ON.md) — reproducible exercises for every feature.
- [CLI reference](docs/CLI.md) — commands, options, defaults, and exit behavior.
- [Extending the runner](docs/EXTENDING.md) — workflows, steps, retry policies,
  budgets, side effects, and artifacts.

## Useful commands

```text
runner demo       Run the intentionally non-durable baseline
runner init       Initialize or migrate the SQLite database
runner start      Enqueue a durable run
runner worker     Claim and execute one step
runner resume     Drain one stored run in the current process
runner inspect    Show current state, usage, and errors
runner events     Show durable event history
runner artifacts  List checksummed files produced by a run
runner cancel     Prevent a run from claiming more work
runner chaos      Exercise four crash boundaries and verify recovery
```

Run `uv run runner <command> --help` for command-specific options.

## Core result

The runner provides **at-least-once step execution**:

- A step committed as `succeeded` is reused after restart.
- A step whose worker disappeared is re-executed after its lease expires.
- Only the current unexpired lease owner may commit a result.
- Retried external effects are safe only when their integration supports a stable
  idempotency key or equivalent reconciliation.

The quickest end-to-end verification is:

```bash
uv run runner chaos
```

It crashes workers after claim, before commit, after commit, and after publication,
then verifies that the run completes and exactly one publication exists.

## Project status

The implementation is complete for its teaching scope. It uses SQLite and local
files, executes linear workflows, and starts workers manually. See
[Known limitations](docs/FAILURE_SEMANTICS.md#known-limitations) before adapting it
to real workloads.
