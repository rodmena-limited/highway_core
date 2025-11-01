# --- persistence/file_storage.py ---
# Purpose: Implements storage for large files/artifacts.
# Responsibilities:
# - Saves large binaries or text (e.g., log files) to disk/S3.
# - Complements the DB storage, which is for structured state.


class FileStorage:
    def __init__(self, base_path: str = "/var/highway/artifacts"):
        self.base_path = base_path
        print(f"FileStorage initialized: {base_path}")

    def save_artifact(self, workflow_id: str, artifact_name: str, content: bytes):
        """Saves a large file artifact."""
        print(
            f"  [FileStorage] STUB: Saving artifact '{artifact_name}' for {workflow_id}."
        )
        # path = f"{self.base_path}/{workflow_id}/{artifact_name}"
        # os.makedirs(os.path.dirname(path), exist_ok=True)
        # with open(path, 'wb') as f:
        #     f.write(content)
        pass

    def load_artifact(self, workflow_id: str, artifact_name: str) -> bytes:
        """Loads a large file artifact."""
        print(
            f"  [FileStorage] STUB: Loading artifact '{artifact_name}' for {workflow_id}."
        )
        return b"stubbed artifact content"
