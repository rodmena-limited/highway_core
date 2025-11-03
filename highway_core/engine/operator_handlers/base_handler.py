# --- engine/operator_handlers/base_handler.py ---
# Purpose: Defines the abstract base class for all operator handlers.

from abc import ABC, abstractmethod

from highway_core.engine.models import BaseOperatorModel
from highway_core.engine.state import WorkflowState


class BaseOperatorHandler(ABC):
    @abstractmethod
    def execute(self, task: BaseOperatorModel, state: WorkflowState) -> list[str]:
        """
        Executes the logic for a specific operator.
        Returns a list of the next task IDs to be queued.
        """
        pass
