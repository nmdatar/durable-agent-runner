import pytest

from durable_agent_runner.artifacts import ArtifactIntegrityError, ArtifactManager
from durable_agent_runner.demo import DemoServices, build_demo_workflow
from durable_agent_runner.durable_runner import DurableRunner
from durable_agent_runner.storage import SQLiteStore
from durable_agent_runner.worker import Worker


def make_artifacts(tmp_path):
    store = SQLiteStore(tmp_path / "runner.db")
    store.initialize()
    return store, ArtifactManager(store, tmp_path / "artifacts")


def test_artifact_round_trip_records_checksum_and_metadata(tmp_path) -> None:
    store, artifacts = make_artifacts(tmp_path)
    workflow = build_demo_workflow("Test task", DemoServices())
    run_id = DurableRunner(store).create_run(workflow)

    reference = artifacts.write_text(
        run_id=run_id,
        step_name="write_report",
        name="report.md",
        content="durable report",
        media_type="text/markdown",
    )

    assert artifacts.read_text(reference) == "durable report"
    metadata = store.get_artifact(reference["artifact_id"])
    assert metadata.size_bytes == len(b"durable report")
    assert len(metadata.sha256) == 64
    assert metadata.media_type == "text/markdown"


@pytest.mark.parametrize("damage", ["missing", "corrupt"])
def test_missing_or_corrupt_artifact_is_detected(tmp_path, damage) -> None:
    store, artifacts = make_artifacts(tmp_path)
    workflow = build_demo_workflow("Test task", DemoServices())
    run_id = DurableRunner(store).create_run(workflow)
    reference = artifacts.write_text(
        run_id=run_id,
        step_name="write_report",
        name="report.md",
        content="original",
    )
    metadata = store.get_artifact(reference["artifact_id"])
    path = artifacts.path_for(metadata)
    if damage == "missing":
        path.unlink()
    else:
        path.write_text("tampered")

    with pytest.raises(ArtifactIntegrityError):
        artifacts.read_text(reference)


def test_publish_repairs_a_deterministic_artifact_before_use(tmp_path) -> None:
    store, artifacts = make_artifacts(tmp_path)
    services = DemoServices(artifacts=artifacts)
    workflow = build_demo_workflow("Test task", services)
    run_id = DurableRunner(store).create_run(workflow)
    services.run_id = run_id
    worker = Worker(store, lambda _claim: workflow, worker_id="artifact-worker")

    for _ in range(3):
        worker.run_once(run_id=run_id)

    metadata = store.list_artifacts(run_id)[0]
    artifacts.path_for(metadata).write_text("corrupt contents")
    result = worker.run_once(run_id=run_id)

    assert result is not None and result.run_completed
    assert artifacts.read_text({"artifact_id": metadata.id}).startswith("# Test task")
    event_types = [event.event_type for event in store.get_events(run_id)]
    assert "artifact_repaired" in event_types
