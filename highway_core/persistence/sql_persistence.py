import logging
import json
from typing import Optional, Tuple, Dict, Any, Set
from pathlib import Path

from highway_core.engine.models import TaskOperatorModel
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

    def start_workflow(
        self, workflow_id: str, workflow_name: str, variables: Dict
    ) -> None:
        """Record workflow start in database"""
        with self.db_manager.database_transaction() as conn:
            conn.execute(
                "INSERT INTO workflows (workflow_id, workflow_name, variables_json) VALUES (?, ?, ?)",
                (workflow_id, workflow_name, json.dumps(variables)),
            )

    def complete_workflow(self, workflow_id: str) -> None:
        """Mark workflow as completed"""
        with self.db_manager.database_transaction() as conn:
            conn.execute(
                "UPDATE workflows SET status = 'completed', end_time = CURRENT_TIMESTAMP WHERE workflow_id = ?",
                (workflow_id,),
            )

    def fail_workflow(self, workflow_id: str, error_message: str) -> None:
        """Mark workflow as failed with error details"""
        with self.db_manager.database_transaction() as conn:
            conn.execute(
                "UPDATE workflows SET status = 'failed', error_message = ?, end_time = CURRENT_TIMESTAMP WHERE workflow_id = ?",
                (error_message, workflow_id),
            )

    def start_task(self, workflow_id: str, task: TaskOperatorModel) -> None:
        """Record task start"""
        with self.db_manager.database_transaction() as conn:
            conn.execute(
                """
                INSERT INTO tasks (
                    workflow_id, task_id, operator_type, runtime, function, image,
                    command_json, args_json, kwargs_json, result_key, dependencies_json,
                    status, started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'executing', CURRENT_TIMESTAMP)
                ON CONFLICT(workflow_id, task_id) DO UPDATE SET
                    status = 'executing',
                    started_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    workflow_id,
                    task.task_id,
                    task.operator_type,
                    getattr(task, "runtime", "python"),
                    getattr(task, "function", None),
                    getattr(task, "image", None),
                    json.dumps(getattr(task, "command", None)),
                    json.dumps(getattr(task, "args", [])),
                    json.dumps(getattr(task, "kwargs", {})),
                    getattr(task, "result_key", None),
                    json.dumps(getattr(task, "dependencies", [])),
                ),
            )

    def complete_task(self, workflow_id: str, task_id: str, result: Any) -> None:
        """Record task completion with result"""
        result_json = json.dumps(result)
        with self.db_manager.database_transaction() as conn:
            conn.execute(
                """
                UPDATE tasks
                SET status = 'completed', completed_at = CURRENT_TIMESTAMP, result_value_json = ?
                WHERE workflow_id = ? AND task_id = ?
                """,
                (result_json, workflow_id, task_id),
            )
            task_info = conn.execute(
                "SELECT result_key FROM tasks WHERE workflow_id = ? AND task_id = ?",
                (workflow_id, task_id),
            ).fetchone()
            if task_info and task_info[0]:
                conn.execute(
                    """
                    INSERT INTO workflow_results (workflow_id, task_id, result_key, result_value_json)
                    VALUES (?, ?, ?, ?)
                    """,
                    (workflow_id, task_id, task_info[0], result_json),
                )

    def fail_task(self, workflow_id: str, task_id: str, error_message: str) -> None:
        """Record task failure with error details"""
        with self.db_manager.database_transaction() as conn:
            conn.execute(
                """
                UPDATE tasks
                SET status = 'failed', error_message = ?, completed_at = CURRENT_TIMESTAMP
                WHERE workflow_id = ? AND task_id = ?
                """,
                (error_message, workflow_id, task_id),
            )

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
        with self.db_manager.database_transaction() as conn:
            workflow_row = conn.execute(
                "SELECT variables_json FROM workflows WHERE workflow_id = ?",
                (workflow_id,),
            ).fetchone()
            if workflow_row:
                variables = json.loads(workflow_row[0])
                state = WorkflowState(variables)
                results_rows = conn.execute(
                    "SELECT result_key, result_value_json FROM workflow_results WHERE workflow_id = ?",
                    (workflow_id,),
                ).fetchall()
                for row in results_rows:
                    state.results[row[0]] = json.loads(row[1])

                completed_tasks_rows = conn.execute(
                    "SELECT task_id FROM tasks WHERE workflow_id = ? AND status = 'completed'",
                    (workflow_id,),
                ).fetchall()
                completed_tasks = {row[0] for row in completed_tasks_rows}

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
            # The result is already saved by complete_task, this method is now a no-op
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
            # This method is now a no-op
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
