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

## Durable workflow

The SQLite-backed runner resumes at the first unfinished step. Initialize a local
database and start a run that crashes after `collect`:

```bash
uv run runner init
uv run runner start demo --crash-after collect
```

Copy the printed run ID, inspect its persisted state, and resume it:

```bash
uv run runner inspect <run-id>
uv run runner events <run-id>
uv run runner resume <run-id>
```

The resumed process skips `plan` and `collect` because their outputs were committed
to SQLite before the simulated crash.
