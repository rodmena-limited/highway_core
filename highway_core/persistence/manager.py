# --- persistence/manager.py ---
# Purpose: Defines the abstract base class for persistence.
# Responsibilities:
# - Provides a standard interface for saving/loading workflow state.

from abc import ABC, abstractmethod
from highway_core.engine.state import WorkflowState
from typing import Tuple, Set


class PersistenceManager(ABC):
    @abstractmethod
    def save_workflow_state(
        self, workflow_run_id: str, state: WorkflowState, completed_tasks: Set[str]
    ):
        """Saves the current state of a workflow execution."""
        pass

    @abstractmethod
    def load_workflow_state(
        self, workflow_run_id: str
    ) -> Tuple[WorkflowState, Set[str]]:
        """Loads a workflow state from persistence."""
        pass
