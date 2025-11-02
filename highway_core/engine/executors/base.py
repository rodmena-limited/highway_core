# highway_core/engine/executors/base.py
import logging
from abc import ABC, abstractmethod
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from highway_core.engine.models import TaskOperatorModel
    from highway_core.engine.state import WorkflowState
    from highway_core.tools.registry import ToolRegistry
    from highway_core.tools.bulkhead import BulkheadManager

logger = logging.getLogger(__name__)


class BaseExecutor(ABC):
    """
    Abstract base class for all task executors.
    An executor is responsible for the *how* of running a task.
    """

    @abstractmethod
    def execute(
        self,
        task: "TaskOperatorModel",
        state: "WorkflowState",
        registry: "ToolRegistry",
        bulkhead_manager: Optional["BulkheadManager"],
    ) -> Any:
        """
        Executes a task given the current state and dependencies.

        Args:
            task: The task model containing its definition.
            state: The current workflow state, used for templating.
            registry: The tool registry, used by executors that call Python tools.
            bulkhead_manager: The bulkhead manager, for executors that support it.

        Returns:
            The result of the task execution (e.g., function return value,
            container logs).
        """
        pass
