from datetime import datetime
from typing import Any, Dict, Optional, Set, Tuple

from highway_core.engine.models import TaskOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.persistence.manager import PersistenceManager


class MockPersistenceManager(PersistenceManager):
    def __init__(self):
        self.state_store = {}
        self.completed_tasks_store = {}
        self.workflow_store = {}  # Store workflow metadata

    def save_workflow_state(
        self,
        workflow_run_id: str,
        state: WorkflowState,
        completed_tasks: Set[str],
    ):
        """Mock save method"""
        self.state_store[workflow_run_id] = state
        self.completed_tasks_store[workflow_run_id] = completed_tasks

    def load_workflow_state(
        self, workflow_run_id: str
    ) -> Tuple[WorkflowState, Set[str]]:
        """Mock load method"""
        state = self.state_store.get(workflow_run_id)
        completed_tasks = self.completed_tasks_store.get(workflow_run_id, set())
        return state, completed_tasks

    def start_workflow(
        self, workflow_id: str, workflow_name: str, variables: Dict
    ) -> None:
        """Mock method to start workflow"""
        self.workflow_store[workflow_id] = {
            "workflow_id": workflow_id,
            "workflow_name": workflow_name,
            "variables": variables,
            "status": "running",
        }

    def complete_workflow(self, workflow_id: str) -> None:
        """Mock method to complete workflow"""
        if workflow_id in self.workflow_store:
            self.workflow_store[workflow_id]["status"] = "completed"

    def fail_workflow(self, workflow_id: str, error_message: str) -> None:
        """Mock method to fail workflow"""
        if workflow_id in self.workflow_store:
            self.workflow_store[workflow_id]["status"] = "failed"
            self.workflow_store[workflow_id]["error_message"] = error_message

    def start_task(self, workflow_id: str, task: Any) -> None:
        """Mock method to start task"""
        pass

    def complete_task(self, workflow_id: str, task_id: str, result: Any) -> None:
        """Mock method to complete task"""
        pass

    def fail_task(self, workflow_id: str, task_id: str, error_message: str) -> None:
        """Mock method to fail task"""
        pass

    def save_task_result(self, workflow_run_id: str, task_id: str, result: Any) -> bool:
        """Mock method to save task result"""
        return True

    def save_task_execution(
        self,
        workflow_run_id: str,
        task_id: str,
        executor_runtime: str,
        execution_args: Optional[Dict[str, Any]] = None,
        execution_kwargs: Optional[Dict[str, Any]] = None,
        result: Optional[Any] = None,
        error_message: Optional[str] = None,
        started_at: Optional[Any] = None,
        completed_at: Optional[Any] = None,
        duration_ms: Optional[int] = None,
        status: str = "completed",
    ) -> bool:
        """Mock method to save task execution"""
        return True

    def mark_task_completed(self, workflow_run_id: str, task_id: str) -> bool:
        """Mock method to mark task completed"""
        return True

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
        """Mock method to save task"""
        return True

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
        """Mock method to save task if not exists"""
        return True

    def close(self) -> None:
        """Mock method to close"""
        pass
