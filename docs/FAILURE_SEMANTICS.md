# Failure Semantics

## Guarantee summary

| Property | Guarantee |
|---|---|
| Completed step output | Survives process restart in SQLite |
| Step execution | At least once |
| Concurrent ownership | At most one valid lease at a time |
| Stale completion | Rejected after lease expiry or reassignment |
| Retry schedule | Durable and bounded per step |
| Arbitrary side effects | No exactly-once guarantee |
| Demo publication | One effect through a stable idempotency key |
| Artifact reuse | Checked by size and SHA-256 before reading |
| Cancellation | Prevents new claims; it does not preempt Python code |
| Budgets | Declared usage reserved before execution |

## Why execution is at least once

There is no atomic transaction spanning arbitrary Python work and SQLite. Consider:

```text
claim step → perform work → commit output
```

If the worker dies after performing work but before committing output, the runner
cannot prove that the work finished. When the lease expires, it executes the step
again. This is the correct conservative choice and produces at-least-once behavior.

Steps should therefore be deterministic, side-effect-free, or explicitly
idempotent. “Exactly once” for arbitrary code is not promised.

## Crash boundaries

| Crash point | Durable state | Recovery behavior |
|---|---|---|
| Before claim | Step remains eligible | Any worker may claim it |
| After claim | Step is `running` with a lease | Another worker waits, then reclaims after expiry |
| During execution | No output committed | Step is re-executed after expiry |
| After execution, before commit | Effect may have occurred; no output committed | Step is re-executed |
| After step commit | Step is `succeeded` with output | Later workers advance to the next step |
| After external publish, before local result | Publication exists; operation is `started` | Repeat with the same key and recover its ID |

The `runner chaos` command exercises the last four meaningful boundaries.

## Leases and heartbeats

A claim records `lease_owner` and `lease_expires_at`. While executing, the worker
renews the lease in a background thread. Completion is conditional on the same
owner and an unexpired timestamp.

If a process dies, its heartbeat dies. The step becomes eligible after expiry and
the next claim records `step_lease_expired`. The old worker cannot commit after a
new owner takes over.

This assumes workers use sufficiently synchronized clocks. A production
multi-machine system should use database time rather than client clocks.

## Retries

Only `RetryableStepError` schedules a retry. `TerminalStepError` and other
exceptions fail the step and run immediately. The default policy is:

```text
maximum attempts: 3
initial delay:     1 second
maximum delay:     30 seconds
backoff:           min(initial × 2^(attempt-1), maximum)
```

SQLite stores `attempt_count`, `next_attempt_at`, and `last_error`. A retry cannot
be claimed early. Once attempts are exhausted, `step_failed` and `run_failed` are
recorded.

## Idempotent side effects

The publish operation uses:

```text
<run-id>:publish:v1
```

The suffix identifies the logical operation and its semantic version. A run may
use other keys such as `<run-id>:send-email:v1`.

The local operation ledger records intent before contacting the publisher. The
publisher independently maps the key to one publication. Repeating the same key
and input returns the same publication ID; reusing it with different input fails.

In a real integration, the remote service must honor the idempotency key. If it
does not, the runner needs a reconciliation strategy using a stable remote ID,
outbox/inbox protocol, or domain-specific deduplication.

## Artifacts and checkpoints

An artifact is a logical checkpoint, not a VM snapshot. The report is written with:

1. a temporary file in the target directory;
2. flush and `fsync`;
3. atomic rename;
4. SQLite metadata containing path, size, and SHA-256.

Reading rejects missing files, paths outside the artifact root, size mismatches,
and checksum mismatches. The demo report is deterministic, so `publish` repairs it
from earlier persisted outputs and records `artifact_repaired`.

Non-deterministic artifacts should not be silently regenerated. They require a
durable object store, replication, or a step-specific recovery policy.

## Cancellation

Cancellation changes a pending or running run to `cancelled`, stores its reason,
and makes all its steps ineligible for new claims. Cancellation is cooperative:
an already executing Python function is not interrupted. If that function commits,
its step may become `succeeded`, but the run remains `cancelled` and no next step is
claimed.

## Budgets

Each step declares estimated `tokens`, `tool_calls`, and `cost_micros`. Before
execution, SQLite atomically reserves those values plus one attempt and, on the
first attempt, one distinct step. A run may set any combination of maxima.

If a reservation would cross a limit, the claimed step and run fail with a durable
reason before the Python function executes. Reserved usage is not refunded after a
crash or failed attempt because the external resources may already have been spent.
These are declared estimates, not measurements from a real model provider.

## Known limitations

- SQLite and local artifacts make this a single-host teaching system.
- SQLite write claims serialize through `BEGIN IMMEDIATE`; this is not a high-scale
  distributed queue.
- Workers execute one step and exit; no continuous loop, supervisor, or autoscaler
  is included.
- Workflows are strictly linear; there is no DAG, fan-out, join, or dynamic step
  creation.
- Step outputs must be JSON-serializable.
- Workflow code is recreated locally. Compatibility validation checks only the
  workflow name and ordered step names, not code or prompt versions.
- The fake external publisher shares the SQLite database. It demonstrates the API
  contract but not a real network or database failure boundary.
- Artifact files are local and are not replicated with the database.
- Schema migration is intentionally simple and lacks production migration tooling.
- Events are append-only by application convention, not protected from direct SQL
  updates or deletion.
- There is no authentication, authorization, encryption, secret isolation, or
  network policy.
- Cancellation does not interrupt in-flight code.
- No production metrics, alerting, retention policy, or garbage collection exists.
