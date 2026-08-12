# Extending the Runner

## Define a workflow

A workflow is an ordered sequence of named `Step` objects. Each action receives the
successful outputs of earlier steps:

```python
from durable_agent_runner.runner import RetryPolicy, Step, StepUsage, Workflow


def plan(_outputs):
    return {"queries": ["durable agents"]}


def research(outputs):
    return search(outputs["plan"]["queries"])


workflow = Workflow(
    name="research-v1",
    steps=(
        Step(
            "plan",
            plan,
            retry_policy=RetryPolicy(max_attempts=3),
            usage=StepUsage(tokens=500, cost_micros=2_000),
        ),
        Step(
            "research",
            research,
            usage=StepUsage(tool_calls=1, cost_micros=1_000),
        ),
    ),
)
```

Names and order are part of the persisted compatibility contract. Existing runs
cannot resume with a different workflow name or ordered list of step names.

Outputs must be JSON-serializable. Store large data as artifacts and return a small
reference from the step.

## Resolve stored workflows

A worker receives a `ClaimedStep`; application code must recreate the matching
workflow:

```python
def resolve_workflow(claim):
    if claim.workflow_name == "research-v1":
        return build_research_workflow(run_id=claim.run_id)
    raise ValueError(f"unknown workflow: {claim.workflow_name}")
```

The resolver is the natural place to inject run-scoped clients, artifact managers,
and idempotent tool wrappers. For production evolution, persist an explicit harness
or workflow version rather than relying only on the name.

## Classify failures

Raise `RetryableStepError` only for failures likely to succeed unchanged later,
such as temporary rate limits or service unavailability:

```python
from durable_agent_runner.errors import RetryableStepError, TerminalStepError

if response.status_code == 429:
    raise RetryableStepError("rate limited")
if response.status_code == 400:
    raise TerminalStepError("invalid request")
```

Unexpected exceptions are terminal by default. Do not retry validation errors,
permission failures, or deterministic bugs indefinitely.

## Make external effects safe

Derive a stable key from the run and logical operation, not the worker or attempt:

```text
<run-id>:create-issue:v1
```

Record intent, send the key to the external service, and record its returned ID. On
recovery, either return the completed ledger result or repeat/reconcile the remote
request using the same key. Never derive the key from a random attempt ID.

Version the suffix when the meaning or input contract changes. Reusing one key for
different input must fail rather than silently returning an unrelated result.

## Store artifacts

Use `ArtifactManager.write_text()` after constructing deterministic content and
return its `{"artifact_id": ...}` reference. Consumers must call `read_text()` so
path, size, and checksum validation cannot be accidentally skipped.

Only regenerate content when doing so is semantically safe. For model-generated or
otherwise nondeterministic artifacts, prefer a durable object store and treat loss
as a terminal integrity failure.

## Set budgets

`StepUsage` is reserved on every execution attempt. Set run limits with `RunBudget`:

```python
from durable_agent_runner.storage import RunBudget

budget = RunBudget(
    max_attempts=10,
    max_steps=5,
    max_tokens=100_000,
    max_tool_calls=20,
    max_cost_micros=5_000_000,
)
```

The current values are declared estimates. A real model adapter should report
measured usage and define how estimates, actual charges, and refunds are reconciled.

## Replace local execution with a sandbox

Keep the worker's claim and commit protocol, but make a step call a sandbox client:

```python
def run_tests(outputs):
    sandbox_id = outputs["create_sandbox"]["sandbox_id"]
    return sandbox.exec(sandbox_id, "pytest")
```

Persist stable sandbox and command identifiers. Treat sandbox API calls as external
effects: use idempotency where offered, reconcile ambiguous results, and checkpoint
important outputs into verified artifacts before committing the step.
