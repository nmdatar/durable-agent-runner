# Hands-on Guide

Run all commands from the repository root. The default database is
`.runner/runner.db`. When this guide says `<run-id>`, replace the entire placeholder,
including angle brackets, with the UUID printed by `runner start`.

Resetting `.runner/` is unnecessary: every experiment creates a new run UUID.

## 1. Observe the non-durable baseline

```bash
uv run runner demo --crash-after collect
uv run runner demo
```

The second process starts again at `plan` because progress existed only in a Python
dictionary.

## 2. Complete a durable run one step at a time

```bash
uv run runner start demo
uv run runner inspect <run-id>
```

Run one worker and inspect again:

```bash
uv run runner worker --once --run-id <run-id> --worker-id worker-1
uv run runner inspect <run-id>
```

Only `plan` is `succeeded`. Run three more workers:

```bash
uv run runner worker --once --run-id <run-id> --worker-id worker-2
uv run runner worker --once --run-id <run-id> --worker-id worker-3
uv run runner worker --once --run-id <run-id> --worker-id worker-4
uv run runner inspect <run-id>
```

The run is now `completed`. View how it arrived there:

```bash
uv run runner events <run-id>
```

## 3. Recover an expired lease

Create a new run, then crash after claiming `plan`:

```bash
uv run runner start demo
uv run runner worker --once --run-id <run-id> \
  --worker-id worker-a --lease-seconds 5 --crash-after-claim plan
```

An immediate competitor finds no work because the lease is live:

```bash
uv run runner worker --once --run-id <run-id> --worker-id worker-b
```

After five seconds, repeat the command. Worker B reclaims and completes `plan`.
The event history includes `step_lease_expired`.

## 4. Observe durable retry state

```bash
uv run runner start demo
uv run runner worker --once --run-id <run-id> --retryable-failure plan
uv run runner inspect <run-id>
```

`plan` is `waiting_retry`, with `attempts=1`, an error, and `next_attempt_at`.
After that time:

```bash
uv run runner worker --once --run-id <run-id>
uv run runner inspect <run-id>
```

`plan` succeeds with `attempts=2`. A terminal failure has no retry:

```bash
uv run runner start demo
uv run runner worker --once --run-id <new-run-id> --terminal-failure plan
```

## 5. Recover an ambiguous external effect

Create a run and complete its first three steps:

```bash
uv run runner start demo
uv run runner worker --once --run-id <run-id>
uv run runner worker --once --run-id <run-id>
uv run runner worker --once --run-id <run-id>
```

Publish, but crash after the external row is created:

```bash
uv run runner worker --once --run-id <run-id> \
  --lease-seconds 5 --crash-after-side-effect
```

Inspect the two sides of the ambiguous operation. Substitute the actual UUID in
both SQL strings:

```bash
sqlite3 -header -column .runner/runner.db \
  "SELECT idempotency_key, status, result_json
   FROM operations WHERE run_id = '<run-id>';"

sqlite3 -header -column .runner/runner.db \
  "SELECT publication_id, idempotency_key
   FROM publications
   WHERE idempotency_key = '<run-id>:publish:v1';"
```

The operation is `started`, but one publication exists. After the lease expires:

```bash
uv run runner worker --once --run-id <run-id>
uv run runner inspect <run-id>
```

`publish` has two attempts. The operation is now `completed`, and this query still
returns `1`:

```bash
sqlite3 .runner/runner.db \
  "SELECT COUNT(*) FROM publications
   WHERE idempotency_key = '<run-id>:publish:v1';"
```

## 6. Inspect and repair an artifact

Create a run and complete through `write_report`:

```bash
uv run runner start demo
uv run runner worker --once --run-id <run-id>
uv run runner worker --once --run-id <run-id>
uv run runner worker --once --run-id <run-id>
uv run runner artifacts <run-id>
```

The output prints the absolute `report.md` path and checksum. Replace the UUID and
corrupt it deliberately:

```bash
printf 'corrupt\n' > .runner/artifacts/<run-id>/report.md
uv run runner worker --once --run-id <run-id>
uv run runner events <run-id>
```

`publish` verifies the file, regenerates it, and records `artifact_repaired`.

## 7. Cancel and budget a run

```bash
uv run runner start demo
uv run runner cancel <run-id> --reason "experiment finished"
uv run runner worker --once --run-id <run-id>
uv run runner inspect <run-id>
```

The worker prints `NO_WORK`, and the run remains `cancelled`.

To fail before a step would cross its token budget:

```bash
uv run runner start demo --max-tokens 99
uv run runner worker --once --run-id <run-id>
uv run runner inspect <run-id>
```

`plan` declares 100 simulated tokens, so it never executes.

## 8. Run the complete chaos campaign

```bash
uv run runner chaos
```

Success ends with:

```text
RESULT status=completed publish_attempts=2 publications=1
```

The timeline should contain three `step_lease_expired` events: after the `plan`
claim, the uncommitted `collect`, and the ambiguous `publish`. `write_report`
crashes after its commit, so it needs no lease recovery.
