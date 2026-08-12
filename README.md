# Durable Agent Runner

A teaching project for learning how long-running agent workflows survive crashes,
retries, and duplicated work.

The project is built one durability concept at a time. Its current runner is an
intentionally non-durable baseline: all progress exists only in process memory.

## Development

```bash
uv sync
uv run pytest
uv run runner demo
```

To simulate a process dying after its `collect` step:

```bash
uv run runner demo --crash-after collect
```

Running the command again starts from `plan`. There is no database or checkpoint
from which to resume yet. A crash after `publish` also illustrates a more subtle
problem: the external effect may have happened even though the runner never
recorded completion.

## Durable workflow with workers

The SQLite-backed scheduler stores work until a worker claims it. Initialize a local
database and enqueue a run:

```bash
uv run runner init
uv run runner start demo
```

Copy the printed run ID. Each invocation below claims and executes one eligible step:

```bash
uv run runner worker --once --run-id <run-id>
uv run runner worker --once --run-id <run-id>
uv run runner worker --once --run-id <run-id>
uv run runner worker --once --run-id <run-id>
```

Inspect current state or durable history at any point:

```bash
uv run runner inspect <run-id>
uv run runner events <run-id>
```

Every claim has an owner and expiration time. A second worker cannot execute the
same step while its lease remains valid. Long-running workers heartbeat to extend
their leases, while work held by a dead worker becomes claimable after expiration.
