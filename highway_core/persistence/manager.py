# --- persistence/manager.py ---
# Purpose: Defines the abstract base class for persistence.
# Responsibilities:
# - Provides a standard interface for saving/loading workflow state.

from abc import ABC, abstractmethod
from highway_core.engine.state import WorkflowState
from highway_core.engine.models import TaskOperatorModel, AnyOperatorModel
from typing import Tuple, Set, Dict, Any, Optional
from datetime import datetime


class PersistenceManager(ABC):
    @abstractmethod
    def save_workflow_state(
        self, workflow_run_id: str, state: WorkflowState, completed_tasks: Set[str]
    ) -> None:
        """Saves the current state of a workflow execution."""
        pass

    @abstractmethod
    def load_workflow_state(
        self, workflow_run_id: str
    ) -> Tuple[WorkflowState | None, Set[str]]:
        """Loads a workflow state from persistence."""
        pass

    @abstractmethod
    def start_workflow(
        self, workflow_id: str, workflow_name: str, variables: Dict[str, Any]
    ) -> None:
        """Start tracking a new workflow execution."""
        pass

    @abstractmethod
    def complete_workflow(self, workflow_id: str) -> None:
        """Complete a workflow execution."""
        pass

    @abstractmethod
    def fail_workflow(self, workflow_id: str, error_message: str) -> None:
        """Mark a workflow as failed."""
        pass

    @abstractmethod
    def start_task(self, workflow_id: str, task: AnyOperatorModel) -> None:
        """Start tracking a task execution."""
        pass

    @abstractmethod
    def complete_task(self, workflow_id: str, task_id: str, result: Any) -> None:
        """Complete a task execution."""
        pass

    @abstractmethod
    def fail_task(self, workflow_id: str, task_id: str, error_message: str) -> None:
        """Mark a task as failed."""
        pass

    @abstractmethod
    def save_task_result(self, workflow_run_id: str, task_id: str, result: Any) -> bool:
        """Save the result of a task execution."""
        pass

    @abstractmethod
    def save_task_execution(
        self,
        workflow_run_id: str,
        task_id: str,
        executor_runtime: str,
        execution_args: Optional[Dict[str, Any]] = None,
        execution_kwargs: Optional[Dict[str, Any]] = None,
        result: Optional[Any] = None,
        error_message: Optional[str] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        duration_ms: Optional[int] = None,
        status: str = "completed",
    ) -> bool:
        """Save task execution details."""
        pass

    @abstractmethod
    def mark_task_completed(self, workflow_run_id: str, task_id: str) -> bool:
        """Mark a task as completed."""
        pass

    @abstractmethod
    def save_task(
        self,
        workflow_run_id: str,
        task_id: str,
        operator_type: str,
        runtime: str = "python",
        function: Optional[str] = None,
        image: Optional[str] = None,
        command: Optional[list] = None,
        args: Optional[list] = None,
        kwargs: Optional[dict] = None,
        result_key: Optional[str] = None,
        dependencies: Optional[list] = None,
    ) -> bool:
        """Save task details."""
        pass

    @abstractmethod
    def save_task_if_not_exists(
        self,
        workflow_run_id: str,
        task_id: str,
        operator_type: str,
        runtime: str = "python",
        function: Optional[str] = None,
        image: Optional[str] = None,
        command: Optional[list] = None,
        args: Optional[list] = None,
        kwargs: Optional[dict] = None,
        result_key: Optional[str] = None,
        dependencies: Optional[list] = None,
    ) -> bool:
        """Save task details only if task doesn't already exist."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close any resources used by the persistence manager."""
        pass
