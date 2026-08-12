"""Checksummed file artifacts used as logical workflow checkpoints."""

import hashlib
import os
import tempfile
from pathlib import Path

from durable_agent_runner.storage import ArtifactSnapshot, SQLiteStore


class ArtifactIntegrityError(RuntimeError):
    """An artifact is missing, outside its root, or does not match metadata."""


class ArtifactManager:
    def __init__(self, store: SQLiteStore, root: str | Path) -> None:
        self._store = store
        self.root = Path(root).resolve()

    def write_text(
        self,
        *,
        run_id: str,
        step_name: str,
        name: str,
        content: str,
        media_type: str = "text/plain",
        repaired: bool = False,
    ) -> dict[str, str]:
        data = content.encode("utf-8")
        relative_path = Path(run_id) / name
        destination = self.root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)

        descriptor, temporary_path = tempfile.mkstemp(
            prefix=f".{name}.",
            dir=destination.parent,
        )
        try:
            with os.fdopen(descriptor, "wb") as temporary_file:
                temporary_file.write(data)
                temporary_file.flush()
                os.fsync(temporary_file.fileno())
            os.replace(temporary_path, destination)
        finally:
            if os.path.exists(temporary_path):
                os.unlink(temporary_path)

        artifact = self._store.save_artifact(
            run_id=run_id,
            step_name=step_name,
            name=name,
            path=str(relative_path),
            sha256=hashlib.sha256(data).hexdigest(),
            size_bytes=len(data),
            media_type=media_type,
            repaired=repaired,
        )
        return {"artifact_id": artifact.id}

    def read_text(self, reference: dict[str, str]) -> str:
        artifact = self._store.get_artifact(reference["artifact_id"])
        path = (self.root / artifact.path).resolve()
        if not path.is_relative_to(self.root):
            raise ArtifactIntegrityError("artifact path escapes its configured root")
        try:
            data = path.read_bytes()
        except FileNotFoundError as error:
            raise ArtifactIntegrityError("artifact file is missing") from error
        if len(data) != artifact.size_bytes:
            raise ArtifactIntegrityError("artifact size does not match metadata")
        if hashlib.sha256(data).hexdigest() != artifact.sha256:
            raise ArtifactIntegrityError("artifact checksum does not match metadata")
        return data.decode("utf-8")

    def path_for(self, artifact: ArtifactSnapshot) -> Path:
        return self.root / artifact.path
