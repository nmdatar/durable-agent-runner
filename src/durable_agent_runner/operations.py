"""Idempotent wrappers for external side effects."""

from durable_agent_runner.runner import SimulatedCrash
from durable_agent_runner.storage import SQLiteStore


class IdempotentPublisher:
    """Publish once, even when a worker repeats the operation after a crash."""

    def __init__(
        self,
        store: SQLiteStore,
        run_id: str,
        *,
        crash_after_effect: bool = False,
    ) -> None:
        self._store = store
        self._run_id = run_id
        self._crash_after_effect = crash_after_effect

    def publish(self, report: str) -> str:
        key = f"{self._run_id}:publish:v1"
        operation = self._store.get_operation(key)
        if operation is not None and operation.status == "completed":
            return operation.result

        self._store.begin_operation(self._run_id, "publish", key)
        publication_id = self._store.publish_once(key, report)

        # The external effect exists, but our operation result is not recorded yet.
        if self._crash_after_effect:
            raise SimulatedCrash("publish-side-effect")

        self._store.complete_operation(key, publication_id)
        return publication_id

