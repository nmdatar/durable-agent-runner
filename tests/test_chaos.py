from durable_agent_runner.chaos import run_chaos_campaign
from durable_agent_runner.storage import SQLiteStore


def test_chaos_campaign_recovers_without_duplicate_effects(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "runner.db")
    store.initialize()

    report = run_chaos_campaign(store, tmp_path / "artifacts")

    assert report.status == "completed"
    assert report.injected_crashes == (
        "plan:after_claim",
        "collect:before_commit",
        "write_report:after_commit",
        "publish:after_side_effect",
    )
    assert report.publish_attempts == 2
    assert report.publication_count == 1
    assert report.event_types.count("step_lease_expired") == 3
    assert report.event_types[-1] == "run_completed"
