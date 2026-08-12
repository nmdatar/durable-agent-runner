# Architecture

## Purpose

The runner persists an agent workflow's **logical progress**. It answers:

- Which run is this?
- Which step is eligible next?
- Which worker owns it?
- What output was durably committed?
- Can a failed attempt retry?
- Did an external side effect already happen?

It does not preserve a process, memory image, installed packages, or operating
system. Those belong to a sandbox control plane.

## Components

```text
CLI
 │  enqueue, inspect, cancel, launch one worker
 ▼
Worker
 │  claim → reserve budget → execute → complete/fail
 ▼
SQLiteStore
 ├── current run and step state
 ├── append-only event history
 ├── leases and retry schedules
 ├── operation ledger
 └── artifact metadata

Step execution
 ├── deterministic demo services
 ├── local artifact files
 └── idempotent fake publisher
```

The main modules are:

- `runner.py`: workflow, step, retry-policy, usage, and in-memory baseline types.
- `worker.py`: lease ownership, heartbeat, execution, failure classification, and
  completion.
- `storage.py`: schema and transactional state transitions.
- `operations.py`: idempotent side-effect wrapper.
- `artifacts.py`: atomic file writes and integrity verification.
- `chaos.py`: deterministic recovery campaign.
- `demo.py`: four-step example workflow.
- `cli.py`: user-facing commands and dependency wiring.

## Execution flow

### Enqueue

`runner start demo` creates one `runs` row, four ordered `steps` rows in `pending`,
and a `run_created` event. It returns a UUID and performs no workflow work.

### Claim

A worker opens `BEGIN IMMEDIATE`, finds the first eligible step, assigns its worker
ID and lease expiration, increments its attempt count, records `step_claimed`, and
commits. The query requires all earlier steps in that run to be `succeeded`.

Eligible means one of:

- `pending`;
- `waiting_retry` whose `next_attempt_at` has arrived;
- `running` whose lease has expired.

An expired claim records `step_lease_expired` before ownership changes.

### Reserve and execute

The worker recreates the workflow from application code and verifies that its name
and ordered step names match the stored run. SQLite then atomically reserves the
step's declared attempts, steps, tokens, tool calls, and simulated cost. Work that
would exceed a limit is not executed.

The worker loads outputs of earlier successful steps and calls the claimed step's
Python function. A background heartbeat renews the lease approximately every third
of the configured lease duration.

### Commit

Successful output is JSON-serialized. One transaction changes the step to
`succeeded`, clears the lease, stores the output, appends `step_completed`, and—if
all steps succeeded—marks the run `completed` and appends `run_completed`.

The update matches the run ID, step name, worker ID, `running` status, and an
unexpired lease. A stale worker therefore cannot overwrite a newer owner's result.

## Data model

```text
runs 1 ──────── * steps
  │
  ├──────────── * events
  ├──────────── * operations
  └──────────── * artifacts

publications   fake external resource store keyed by idempotency key
schema_version current local schema version
```

### `runs`

One row per workflow execution. It stores the UUID, workflow name, lifecycle state,
terminal reason, optional limits, accumulated usage, and timestamps.

Run states:

```text
pending → running → completed
                  ↘ failed
pending/running ──→ cancelled
```

### `steps`

One row per `(run_id, step_name)`, with a unique position within the run. It stores
status, JSON output, attempts, retry time, last error, lease owner, lease expiry,
and timestamps.

Step states:

```text
pending → running → succeeded
             │
             ├──→ waiting_retry → running
             └──→ failed

running with expired lease → running under a new owner
```

### `events`

Chronological records such as `run_created`, `step_claimed`, `budget_reserved`,
`step_retry_scheduled`, `step_lease_expired`, `artifact_saved`, and
`run_completed`. Events explain how current state arose; the `runs` and `steps`
tables remain efficient current-state projections.

### `operations` and `publications`

`operations` is the runner's durable intent/result ledger. `publications` emulates
an external API that stores one resource for each idempotency key. They are kept as
separate tables to expose the ambiguous window where the external resource exists
but the local operation result is still only `started`.

### `artifacts`

Metadata for files stored under `.runner/artifacts/<run-id>/`: relative path,
SHA-256, size, media type, name, and producing step. Steps store a small artifact
reference rather than the report contents.

## Transaction boundaries

The important atomic transitions are:

- create run, steps, and `run_created`;
- claim a step and record its lease event;
- reserve budget usage or fail the run;
- commit step output and its completion event;
- schedule retry or fail the run;
- cancel a run and record its reason.

Step execution itself cannot be part of a database transaction: it may take minutes
and call remote systems. Leases bridge that boundary, and idempotency protects
supported external effects when execution repeats.

Artifact bytes and SQLite metadata are also not one transaction. Files are written
to a temporary file, flushed, and atomically renamed before metadata is recorded.
Readers verify metadata before use.

## Relationship to Sailboxes

This project persists workflow intent and progress. A Sailbox-like control plane
persists and isolates the computer that performs the work:

```text
Durable runner
  decides what should run and records logical completion
        │
        ▼
Sandbox control plane
  creates, pauses, resumes, snapshots, and isolates a machine
        │
        ▼
Container or microVM
  repository, dependencies, processes, filesystem, network
```

An integration would replace local step execution with a sandbox adapter. The
runner would store the sandbox ID with the run, execute commands through the
adapter, checkpoint durable files, and retain its existing leases, retries,
idempotency, events, cancellation, and budgets.
