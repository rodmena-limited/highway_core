# --- persistence/file_storage.py ---
# Purpose: Implements storage for large files/artifacts.
# Responsibilities:
# - Saves large binaries or text (e.g., log files) to disk/S3.
# - Complements the DB storage, which is for structured state.


import logging

logger = logging.getLogger(__name__)


class FileStorage:
    def __init__(self, base_path: str = "/var/highway/artifacts"):
        self.base_path = base_path
        logger.info("FileStorage initialized: %s", base_path)

    def save_artifact(self, workflow_id: str, artifact_name: str, content: bytes):
        """Saves a large file artifact."""
        logger.info(
            "  [FileStorage] STUB: Saving artifact '%s' for %s.",
            artifact_name,
            workflow_id,
        )
        # path = f"{self.base_path}/{workflow_id}/{artifact_name}"
        # os.makedirs(os.path.dirname(path), exist_ok=True)
        # with open(path, 'wb') as f:
        #     f.write(content)
        pass

    def load_artifact(self, workflow_id: str, artifact_name: str) -> bytes:
        """Loads a large file artifact."""
        logger.info(
            "  [FileStorage] STUB: Loading artifact '%s' for %s.",
            artifact_name,
            workflow_id,
        )
        return b"stubbed artifact content"
