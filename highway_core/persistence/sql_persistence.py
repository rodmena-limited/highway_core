import logging
from typing import Optional, Tuple, Dict, Any
from pathlib import Path
import json

from highway_core.engine.state import WorkflowState
from highway_core.persistence.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class SQLPersistence:
    """
    SQL-based persistence manager for Highway Core workflows.
    Uses SQLite3 database to store workflow state, tasks, and results.
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
            # Use test database
            test_db_path = Path("tests/data/highway.sqlite3")
            test_db_path.parent.mkdir(parents=True, exist_ok=True)
            self.db_manager = DatabaseManager(str(test_db_path))
        elif db_path is not None:
            self.db_manager = DatabaseManager(db_path)
        else:
            # Use default production database
            self.db_manager = DatabaseManager()

    def save_workflow_state(
        self, workflow_run_id: str, state: WorkflowState, completed_tasks: set
    ) -> None:
        """
        Save the current workflow state to the database.

        Args:
            workflow_run_id: Unique identifier for the workflow run
            state: Current workflow state object
            completed_tasks: Set of completed task IDs
        """
        try:
            # Check if workflow exists, if not create it
            if not self.db_manager.workflow_exists(workflow_run_id):
                # Create workflow record
                self.db_manager.create_workflow(
                    workflow_id=workflow_run_id,
                    name=workflow_run_id,  # In the current implementation, run ID serves as name
                    start_task="",  # Start task would need to be passed separately if needed
                    variables=state.variables,
                )
            else:
                # Update workflow with current variables if needed
                # For now, we just update the updated_at timestamp by changing status to itself
                workflow_data = self.db_manager.load_workflow(workflow_run_id)
                if workflow_data:
                    self.db_manager.update_workflow_status(
                        workflow_run_id, workflow_data.get("status", "running")
                    )

            # Save memory values - make a copy to avoid concurrent modification issues
            memory_copy = dict(state.memory)
            for key, value in memory_copy.items():
                self.db_manager.store_memory(workflow_run_id, key, value)

            # Save result values - make a copy to avoid concurrent modification issues
            results_copy = dict(state.results)
            for key, value in results_copy.items():
                self.db_manager.store_result(workflow_run_id, key, value)

            logger.debug(f"Saved workflow state for {workflow_run_id}")

        except Exception as e:
            logger.error(f"Error saving workflow state for {workflow_run_id}: {e}")
            raise

    def load_workflow_state(
        self, workflow_run_id: str
    ) -> Tuple[Optional[WorkflowState], set]:
        """
        Load the workflow state from the database.

        Args:
            workflow_run_id: Unique identifier for the workflow run

        Returns:
            Tuple of (WorkflowState object or None, set of completed task IDs)
        """
        try:
            # Load workflow data
            workflow_data = self.db_manager.load_workflow(workflow_run_id)
            if not workflow_data:
                logger.info(f"No workflow found for ID: {workflow_run_id}")
                return None, set()

            # Load memory values
            memory = self.db_manager.load_memory(workflow_run_id)

            # Load results
            results = self.db_manager.load_results(workflow_run_id)

            # Get completed tasks - need to get tasks with status 'completed'
            completed_tasks = self.db_manager.get_completed_tasks(workflow_run_id)

            # Create workflow state object
            # Note: We may need to get the initial variables from the workflow definition
            # For now, we'll use the saved variables or an empty dict if not available
            variables = workflow_data.get("variables", {})

            state = WorkflowState(variables)
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
            # Update task status to completed
            from datetime import datetime

            self.db_manager.update_task_status(task_id, "completed")
            self.db_manager.update_task_completion(task_id, datetime.now())

            # If result_key exists for this task, save it to results
            # We need to get the task to check for result_key
            task_info = self.db_manager.fetch_one(
                "SELECT result_key FROM tasks WHERE task_id = ? AND workflow_id = ?",
                (task_id, workflow_run_id),
            )

            if task_info and task_info["result_key"]:
                result_key = task_info["result_key"]
                self.db_manager.store_result(workflow_run_id, result_key, result)

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
            success = self.db_manager.create_task_execution(
                task_id=task_id,
                workflow_id=workflow_run_id,
                executor_runtime=executor_runtime,
                execution_args=execution_args,
                execution_kwargs=execution_kwargs,
                result=result,
                error_message=error_message,
                started_at=started_at,
                completed_at=completed_at,
                duration_ms=duration_ms,
                status=status,
            )

            if success and status == "completed" and result is not None:
                # Update the task's result key if one exists
                task_info = self.db_manager.fetch_one(
                    "SELECT result_key FROM tasks WHERE task_id = ? AND workflow_id = ?",
                    (task_id, workflow_run_id),
                )

                if task_info and task_info["result_key"]:
                    result_key = task_info["result_key"]
                    self.db_manager.store_result(workflow_run_id, result_key, result)

            return success
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
            # First check if task exists
            task_exists = self.db_manager.fetch_one(
                "SELECT 1 FROM tasks WHERE task_id = ?", (task_id,)
            )

            if not task_exists:
                # Create a minimal task record if it doesn't exist
                # This is a fallback - ideally tasks should be created when first encountered
                self.db_manager.create_task(
                    workflow_id=workflow_run_id,
                    task_id=task_id,
                    operator_type="unknown",  # Will be updated later
                    runtime="python",
                    status="completed",  # Set status to completed directly
                )
            else:
                # Update task status to completed
                from datetime import datetime

                self.db_manager.update_task_status(task_id, "completed")
                self.db_manager.update_task_completion(task_id, datetime.now())
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
            existing_task = self.db_manager.fetch_one(
                "SELECT 1 FROM tasks WHERE task_id = ? AND workflow_id = ?",
                (task_id, workflow_run_id),
            )

            if not existing_task:
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
