import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple

from highway_core.config import settings
from highway_core.engine.models import AnyOperatorModel, TaskOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.persistence.database_manager import DatabaseManager
from highway_core.persistence.manager import PersistenceManager

logger = logging.getLogger(__name__)


class SQLPersistence(PersistenceManager):
    """
    SQL-based persistence manager for Highway Core workflows.
    Uses SQLAlchemy database to store workflow state, tasks, and results.
    """

    def __init__(self, db_path: Optional[str] = None, is_test: bool = False):
        """
        Initialize the SQL persistence manager.

        Args:
            db_path: Path to the SQLite database file.
                     If None, uses ~/.highway.sqlite3 for production and tests/data/highway.sqlite3 for tests
            is_test: Whether this is running in test mode
        """
        if is_test:
            self.db_manager = DatabaseManager(engine_url=settings.DATABASE_URL)
        elif db_path is not None:
            self.db_manager = DatabaseManager(db_path)
        else:
            # Use default production database
            self.db_manager = DatabaseManager()

    def start_workflow(
        self, workflow_id: str, workflow_name: str, variables: Dict
    ) -> None:
        """Record workflow start in database"""
        # Use the existing create_workflow method in DatabaseManager
        self.db_manager.create_workflow(
            workflow_id=workflow_id,
            name=workflow_name,
            start_task="",  # start_task is not directly available here, it's part of WorkflowModel
            variables=variables,
        )

    def complete_workflow(self, workflow_id: str) -> None:
        """Mark workflow as completed"""
        self.db_manager.update_workflow_status(workflow_id, "completed")

    def fail_workflow(self, workflow_id: str, error_message: str) -> None:
        """Mark workflow as failed with error details"""
        self.db_manager.update_workflow_status(
            workflow_id, "failed", error_message=error_message
        )

    def start_task(self, workflow_id: str, task: AnyOperatorModel) -> None:
        """Record task start"""
        # Create or update the task with executing status
        # Use getattr to safely access properties that may not exist on all operator types
        self.db_manager.create_task(
            workflow_id=workflow_id,
            task_id=task.task_id,
            operator_type=task.operator_type,
            runtime=getattr(task, "runtime", "python"),
            function=getattr(task, "function", None),
            image=getattr(task, "image", None),
            command=getattr(task, "command", None),
            args=getattr(task, "args", []),
            kwargs=getattr(task, "kwargs", {}),
            result_key=getattr(task, "result_key", None),
            dependencies=getattr(task, "dependencies", []),
            status="executing",
        )

    def complete_task(self, workflow_id: str, task_id: str, result: Any) -> None:
        """Record task completion with result"""
        from datetime import datetime

        # Update the task with result and completion status
        self.db_manager.update_task_with_result(task_id, result, datetime.now(timezone.utc))

        # Find the task's result_key to store in workflow results
        all_tasks = self.db_manager.get_tasks_by_workflow(workflow_id)
        task_result_key = None
        for t in all_tasks:
            if t["task_id"] == task_id:
                task_result_key = t.get("result_key")
                break

        if task_result_key:
            # Store the result in workflow results
            self.db_manager.store_result(workflow_id, task_id, task_result_key, result)

    def fail_task(self, workflow_id: str, task_id: str, error_message: str) -> None:
        """Record task failure with error details"""
        self.db_manager.update_task_status(
            task_id, "failed", error_message=error_message
        )

    def save_workflow_state(
        self,
        workflow_run_id: str,
        state: WorkflowState,
        completed_tasks: Set[str],
    ) -> None:
        """
        Save the workflow state to the database.

        Args:
            workflow_run_id: Unique identifier for the workflow run
            state: Current workflow state
            completed_tasks: Set of completed task IDs
        """
        # This method currently stores the state and completed tasks
        # In a SQL implementation, we might save task completion status to the tasks table
        for task_id in completed_tasks:
            self.db_manager.update_task_status(task_id, "completed")

    def load_workflow_state(
        self, workflow_run_id: str
    ) -> Tuple[Optional[WorkflowState], Set[str]]:
        """
        Load the workflow state from the database.

        Args:
            workflow_run_id: Unique identifier for the workflow run

        Returns:
            Tuple of (WorkflowState object or None, set of completed task IDs)
        """
        try:
            state, completed_tasks = self.get_workflow_state(workflow_run_id)
            if not state:
                logger.info(f"No workflow found for ID: {workflow_run_id}")
                return None, set()

            # Load memory values
            memory = self.db_manager.load_memory(workflow_run_id)

            # Load results
            results = self.db_manager.load_results(workflow_run_id)

            state.memory = memory
            state.results = results

            logger.debug(
                f"Loaded workflow state for {workflow_run_id}, "
                f"memory keys: {list(memory.keys())}, "
                f"result keys: {list(results.keys())}, "
                f"completed tasks: {len(completed_tasks)}"
            )

            return state, completed_tasks

        except Exception as e:
            logger.error(f"Error loading workflow state for {workflow_run_id}: {e}")
            return None, set()

    def get_workflow_state(
        self, workflow_id: str
    ) -> Tuple[Optional[WorkflowState], Set[str]]:
        """Load workflow state and completed tasks"""
        state = None
        completed_tasks = set()

        # Load workflow
        workflow_data = self.db_manager.load_workflow(workflow_id)
        if workflow_data:
            variables = workflow_data.get("variables", {})
            state = WorkflowState(variables)

            # Load results
            results = self.db_manager.load_results(workflow_id)
            state.results = results

            # Get completed tasks
            completed_tasks = self.db_manager.get_completed_tasks(workflow_id)

        return state, completed_tasks

    def save_task_result(self, workflow_run_id: str, task_id: str, result: Any) -> bool:
        """
        Save the result of a task execution.

        Args:
            workflow_run_id: The workflow ID
            task_id: The task ID
            result: The result of the task execution

        Returns:
            True if successful, False otherwise
        """
        try:
            # Get the task to find its result_key
            all_tasks = self.db_manager.get_tasks_by_workflow(workflow_run_id)
            task_result_key = None
            for task in all_tasks:
                if task["task_id"] == task_id:
                    task_result_key = task.get("result_key")
                    break

            if task_result_key:
                # Store the result using the workflow results table
                self.db_manager.store_result(
                    workflow_run_id, task_id, task_result_key, result
                )

            return True
        except Exception as e:
            logger.error(f"Error saving task result for task {task_id}: {e}")
            return False

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
        """
        Save task execution details.

        Args:
            workflow_run_id: The workflow ID
            task_id: The task ID
            executor_runtime: Runtime used for execution (python, docker, etc.)
            execution_args: Arguments used for execution
            execution_kwargs: Keyword arguments used for execution
            result: Result of execution
            error_message: Error message if execution failed
            started_at: Start timestamp
            completed_at: Completion timestamp
            duration_ms: Duration in milliseconds
            status: Execution status

        Returns:
            True if successful, False otherwise
        """
        try:
            if status == "failed":
                self.fail_task(
                    workflow_run_id, task_id, error_message or "Unknown error"
                )
            return True
        except Exception as e:
            logger.error(f"Error saving task execution for task {task_id}: {e}")
            return False

    def mark_task_completed(self, workflow_run_id: str, task_id: str) -> bool:
        """
        Mark a task as completed.

        Args:
            workflow_run_id: The workflow ID
            task_id: The task ID

        Returns:
            True if successful, False otherwise
        """
        try:
            # Update the task status to completed
            self.db_manager.update_task_status(task_id, "completed")
            return True
        except Exception as e:
            logger.error(f"Error marking task {task_id} as completed: {e}")
            return False

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
        """
        Save task information to the database.

        Args:
            workflow_run_id: The workflow ID
            task_id: The task ID
            operator_type: Type of operator (task, condition, etc.)
            runtime: Runtime to use (python, docker)
            function: Function name for python runtime
            image: Docker image name
            command: Command for docker runtime
            args: Arguments for the task
            kwargs: Keyword arguments for the task
            result_key: Key to store result under
            dependencies: List of task IDs this task depends on

        Returns:
            True if successful, False otherwise
        """
        try:
            # Create the task record
            success = self.db_manager.create_task(
                workflow_id=workflow_run_id,
                task_id=task_id,
                operator_type=operator_type,
                runtime=runtime,
                function=function,
                image=image,
                command=command,
                args=args,
                kwargs=kwargs,
                result_key=result_key,
                dependencies=dependencies,
            )

            if success and dependencies:
                # Save dependencies
                self.db_manager.store_dependencies(
                    workflow_run_id, task_id, dependencies or []
                )

            return success
        except Exception as e:
            logger.error(f"Error saving task {task_id}: {e}")
            return False

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
        """
        Save task information to the database if it doesn't already exist.

        Args:
            workflow_run_id: The workflow ID
            task_id: The task ID
            operator_type: Type of operator (task, condition, etc.)
            runtime: Runtime to use (python, docker)
            function: Function name for python runtime
            image: Docker image name
            command: Command for docker runtime
            args: Arguments for the task
            kwargs: Keyword arguments for the task
            result_key: Key to store result under
            dependencies: List of task IDs this task depends on

        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if task already exists for this specific workflow
            # We'll do this by attempting to get the task
            all_tasks = self.db_manager.get_tasks_by_workflow(workflow_run_id)
            task_exists = any(task["task_id"] == task_id for task in all_tasks)

            if not task_exists:
                logger.debug(
                    f"Creating new task {task_id} in workflow {workflow_run_id}"
                )
                # Create the task record
                success = self.db_manager.create_task(
                    workflow_id=workflow_run_id,
                    task_id=task_id,
                    operator_type=operator_type,
                    runtime=runtime,
                    function=function,
                    image=image,
                    command=command,
                    args=args,
                    kwargs=kwargs,
                    result_key=result_key,
                    dependencies=dependencies,
                )
                logger.debug(
                    f"Task creation {'successful' if success else 'failed'} for {task_id}"
                )
                return success
            logger.debug(
                f"Task {task_id} already exists in workflow {workflow_run_id}, skipping creation"
            )
            return True  # Task already exists, which is fine
        except Exception as e:
            logger.error(f"Error saving task {task_id} if not exists: {e}")
            return False

    def close(self) -> None:
        """
        Close the database connection.
        """
        try:
            self.db_manager.close_all_connections()
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")
