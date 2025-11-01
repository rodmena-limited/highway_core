# --- persistence/manager.py ---
# Purpose: Defines the abstract base class for persistence.
# Responsibilities:
# - Provides a standard interface for saving/loading workflow state.

from abc import ABC, abstractmethod
from highway_core.engine.state import WorkflowState


class PersistenceManager(ABC):
    @abstractmethod
    def save_workflow_state(self, workflow_id: str, state: WorkflowState):
        """Saves the current state of a workflow execution."""
        pass

    @abstractmethod
    def load_workflow_state(self, workflow_id: str) -> WorkflowState:
        """Loads a workflow state from persistence."""
        pass
