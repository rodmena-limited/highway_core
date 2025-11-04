import logging
from datetime import datetime, timezone
from typing import Any, Dict, Set, Tuple

from highway_core.engine.models import AnyOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.persistence.manager import PersistenceManager
from highway_core.persistence.sql_persistence import SQLPersistence

logger = logging.getLogger(__name__)


class SQLPersistenceManager(PersistenceManager):
    def __init__(self, db_path=None, is_test: bool = False):
        # Initialize SQL persistence
        self.sql_persistence = SQLPersistence(
            db_path=db_path, is_test=is_test or db_path is not None
        )

    def start_workflow(
        self, workflow_id: str, workflow_name: str, variables: Dict[str, Any]
    ) -> None:
        self.sql_persistence.start_workflow(workflow_id, workflow_name, variables)

    def complete_workflow(self, workflow_id: str) -> None:
        self.sql_persistence.complete_workflow(workflow_id)

    def fail_workflow(self, workflow_id: str, error_message: str) -> None:
        self.sql_persistence.fail_workflow(workflow_id, error_message)

    def save_workflow_state(
        self, workflow_run_id: str, state: WorkflowState, completed_tasks: Set[str]
    ) -> None:
        self.sql_persistence.save_workflow_state(
            workflow_run_id, state, completed_tasks
        )

    def load_workflow_state(
        self, workflow_run_id: str
    ) -> Tuple[WorkflowState | None, Set[str]]:
        return self.sql_persistence.load_workflow_state(workflow_run_id)

    def start_task(self, workflow_id: str, task: AnyOperatorModel) -> bool:
        """Tries to acquire a lock and start a task, returns True if successful."""
        # Use the SQL-only version which implements proper locking
        return self.sql_persistence.start_task_sql_only(workflow_id, task)

    def complete_task(self, workflow_id: str, task_id: str, result: Any) -> None:
        self.sql_persistence.complete_task(workflow_id, task_id, result)

    def set_memory(self, workflow_id: str, memory_key: str, memory_value: Any) -> bool:
        """Store a memory value for a workflow."""
        return self.sql_persistence.db_manager.store_memory(
            workflow_id, memory_key, memory_value
        )

    def get_memory(self, workflow_id: str, memory_key: str) -> Any:
        """Get a memory value for a workflow."""
        memory_dict = self.sql_persistence.db_manager.load_memory(workflow_id)
        return memory_dict.get(memory_key)

    def get_all_memory(self, workflow_id: str) -> Dict[str, Any]:
        """Get all memory values for a workflow."""
        return self.sql_persistence.db_manager.load_memory(workflow_id)

    def store_workflow_result(self, workflow_id: str, result: Any) -> bool:
        """Store workflow result."""
        return self.sql_persistence.db_manager.store_workflow_result(
            workflow_id, result
        )

    def get_workflow_result(self, workflow_id: str) -> Any:
        """Get workflow result."""
        return self.sql_persistence.db_manager.get_workflow_result(workflow_id)

    def fail_task(self, workflow_id: str, task_id: str, error_message: str) -> None:
        self.sql_persistence.fail_task(workflow_id, task_id, error_message)

    def save_task_result(self, workflow_run_id: str, task_id: str, result: Any) -> bool:
        return self.sql_persistence.save_task_result(workflow_run_id, task_id, result)

    def save_task_execution(
        self, workflow_run_id: str, task_id: str, executor_runtime: str, **kwargs
    ) -> bool:
        return self.sql_persistence.save_task_execution(
            workflow_run_id, task_id, executor_runtime, **kwargs
        )

    def mark_task_completed(self, workflow_run_id: str, task_id: str) -> bool:
        return self.sql_persistence.mark_task_completed(workflow_run_id, task_id)

    def save_task(
        self, workflow_run_id: str, task_id: str, operator_type: str, **kwargs
    ) -> bool:
        return self.sql_persistence.save_task(
            workflow_run_id, task_id, operator_type, **kwargs
        )

    def save_task_if_not_exists(
        self, workflow_run_id: str, task_id: str, operator_type: str, **kwargs
    ) -> bool:
        return self.sql_persistence.save_task_if_not_exists(
            workflow_run_id, task_id, operator_type, **kwargs
        )

    def close(self) -> None:
        self.sql_persistence.close()
