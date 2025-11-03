# --- persistence/db_storage.py ---
# Purpose: Implements persistence using a database.
# Responsibilities:
# - Connects to a DB (e.g., PostgreSQL, SQLite).
# - Serializes the WorkflowState to JSON and saves it.
# - Deserializes JSON from the DB to restore a WorkflowState.

import json
import logging
import os

from highway_core.engine.state import WorkflowState

from .manager import PersistenceManager

logger = logging.getLogger(__name__)


class DatabasePersistence(PersistenceManager):
    def __init__(self, connection_string: str = ".workflow_state/"):
        self.storage_path = connection_string
        os.makedirs(self.storage_path, exist_ok=True)
        logger.info("DatabasePersistence (File) initialized at: %s", self.storage_path)

    def _get_state_file_path(self, workflow_run_id: str) -> str:
        return os.path.join(self.storage_path, f"{workflow_run_id}.json")

    def save_workflow_state(
        self,
        workflow_run_id: str,
        state: WorkflowState,
        completed_tasks: set[str],
    ) -> None:
        """Saves the current state of a workflow execution to a JSON file."""
        state_file = self._get_state_file_path(workflow_run_id)
        logger.info(
            "  [Persistence] Saving state for %s to %s",
            workflow_run_id,
            state_file,
        )

        try:
            # Create a serializable snapshot
            snapshot = {
                "completed_tasks": list(completed_tasks),
                "workflow_state": state.model_dump(),  # Use Pydantic's method
            }
            with open(state_file, "w") as f:
                json.dump(snapshot, f, indent=2)
        except Exception as e:
            logger.error("  [Persistence] FAILED to save state: %s", e)

    def load_workflow_state(
        self, workflow_run_id: str
    ) -> tuple[WorkflowState | None, set[str]]:
        """Loads a workflow state from a JSON file."""
        state_file = self._get_state_file_path(workflow_run_id)
        if not os.path.exists(state_file):
            logger.info(
                "  [Persistence] No state file found for %s. Starting new run.",
                workflow_run_id,
            )
            return None, set()

        logger.info(
            "  [Persistence] Loading state for %s from %s",
            workflow_run_id,
            state_file,
        )
        try:
            with open(state_file, "r") as f:
                snapshot = json.load(f)

            # Re-hydrate the Pydantic model
            state = WorkflowState.model_validate(snapshot["workflow_state"])
            completed_tasks = set(snapshot["completed_tasks"])
            return state, completed_tasks
        except Exception as e:
            logger.error(
                "  [Persistence] FAILED to load state: %s. Starting new run.",
                e,
            )
            return None, set()
