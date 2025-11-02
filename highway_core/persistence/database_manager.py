import sqlite3
import threading
import json
import os
from pathlib import Path
from contextlib import contextmanager
from typing import Optional, Dict, Any, List
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Thread-safe database manager for Highway Core workflow persistence.
    Manages connections, transactions, and provides a clean interface
    for all database operations.
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the database manager.

        Args:
            db_path: Path to the SQLite database file. If None, uses ~/.highway.sqlite3
        """
        if db_path is None:
            # Default to user's home directory
            home = Path.home()
            self.db_path = home / ".highway.sqlite3"
        else:
            self.db_path = Path(db_path)

        # Ensure the directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Thread-local storage for connections
        self._local = threading.local()

        # Create schema
        self._initialize_schema()

    def _get_connection(self) -> sqlite3.Connection:
        """Get a thread-local database connection."""
        if not hasattr(self._local, "connection"):
            self._local.connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                detect_types=sqlite3.PARSE_DECLTYPES,
            )
            # Enable foreign key constraints
            self._local.connection.execute("PRAGMA foreign_keys = ON")
            # Enable WAL mode for better concurrency
            self._local.connection.execute("PRAGMA journal_mode = WAL")
            # Set row factory for dict-like access
            self._local.connection.row_factory = sqlite3.Row

        return self._local.connection

    def _initialize_schema(self) -> None:
        """Initialize the database schema."""
        schema_file = Path(__file__).parent / "sql_schema.sql"

        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_file}")

        with open(schema_file, "r") as f:
            schema_sql = f.read()

        conn = self._get_connection()

        # Execute the main schema (this will not replace existing tables)
        conn.executescript(schema_sql)

        # Add missing columns if needed (for backward compatibility)
        cursor = conn.cursor()

        # Check if tasks table has updated_at column
        cursor.execute("PRAGMA table_info(tasks)")
        columns = [row[1] for row in cursor.fetchall()]

        if "updated_at" not in columns:
            try:
                cursor.execute(
                    "ALTER TABLE tasks ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                )
            except sqlite3.OperationalError:
                # Column might already exist in newer schema
                pass

        conn.commit()

    @contextmanager
    def transaction(self):
        """Context manager for database transactions."""
        conn = self._get_connection()
        original_isolation = conn.isolation_level
        conn.isolation_level = None  # Start transaction

        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.isolation_level = original_isolation

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        """Execute a SQL statement and return the cursor."""
        conn = self._get_connection()
        cursor = conn.execute(sql, params)
        return cursor

    def execute_many(self, sql: str, params_list: List[tuple]) -> sqlite3.Cursor:
        """Execute a SQL statement multiple times with different parameters."""
        conn = self._get_connection()
        cursor = conn.executemany(sql, params_list)
        return cursor

    def fetch_one(self, sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        """Execute a query and return one result."""
        cursor = self.execute(sql, params)
        return cursor.fetchone()

    def fetch_all(self, sql: str, params: tuple = ()) -> List[sqlite3.Row]:
        """Execute a query and return all results."""
        cursor = self.execute(sql, params)
        return cursor.fetchall()

    def workflow_exists(self, workflow_id: str) -> bool:
        """Check if a workflow exists."""
        result = self.fetch_one(
            "SELECT 1 FROM workflows WHERE workflow_id = ?", (workflow_id,)
        )
        return result is not None

    def create_workflow(
        self, workflow_id: str, name: str, start_task: str, variables: Dict[str, Any]
    ) -> bool:
        """Create a new workflow record."""
        try:
            with self.transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO workflows 
                    (workflow_id, name, start_task, variables_json) 
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        workflow_id,
                        name,
                        start_task,
                        json.dumps(variables) if variables else None,
                    ),
                )
            return True
        except sqlite3.IntegrityError:
            logger.error(f"Workflow with ID {workflow_id} already exists")
            return False
        except Exception as e:
            logger.error(f"Error creating workflow {workflow_id}: {e}")
            return False

    def update_workflow_status(self, workflow_id: str, status: str) -> bool:
        """Update workflow status."""
        try:
            with self.transaction() as conn:
                conn.execute(
                    """
                    UPDATE workflows 
                    SET status = ?, updated_at = CURRENT_TIMESTAMP 
                    WHERE workflow_id = ?
                    """,
                    (status, workflow_id),
                )
            return True
        except Exception as e:
            logger.error(f"Error updating workflow {workflow_id} status: {e}")
            return False

    def load_workflow(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Load a workflow by ID."""
        result = self.fetch_one(
            """
            SELECT workflow_id, name, start_task, variables_json, 
                   created_at, updated_at, status
            FROM workflows 
            WHERE workflow_id = ?
            """,
            (workflow_id,),
        )

        if result:
            return {
                "workflow_id": result["workflow_id"],
                "name": result["name"],
                "start_task": result["start_task"],
                "variables": json.loads(result["variables_json"])
                if result["variables_json"]
                else {},
                "created_at": result["created_at"],
                "updated_at": result["updated_at"],
                "status": result["status"],
            }
        return None

    def create_task(
        self,
        workflow_id: str,
        task_id: str,
        operator_type: str,
        runtime: str = "python",
        function: Optional[str] = None,
        image: Optional[str] = None,
        command: Optional[List[str]] = None,
        args: Optional[List[Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        result_key: Optional[str] = None,
        dependencies: Optional[List[str]] = None,
        status: str = "pending",  # Add status parameter with default value
    ) -> bool:
        """Create a new task record."""
        try:
            # First check if workflow exists, create it if not
            workflow_exists = self.workflow_exists(workflow_id)
            if not workflow_exists:
                # Create a minimal workflow record
                self.create_workflow(
                    workflow_id=workflow_id,
                    name=workflow_id,  # Use workflow_id as name
                    start_task="",  # Empty for now
                    variables={},  # Empty variables
                )

            with self.transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO tasks 
                    (task_id, workflow_id, operator_type, runtime, function, image, 
                     command_json, args_json, kwargs_json, result_key, dependencies_json, status) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        task_id,
                        workflow_id,
                        operator_type,
                        runtime,
                        function,
                        image,
                        json.dumps(command) if command else None,
                        json.dumps(args) if args else None,
                        json.dumps(kwargs) if kwargs else None,
                        result_key,
                        json.dumps(dependencies) if dependencies else None,
                        status,  # Add status parameter
                    ),
                )
            return True
        except sqlite3.IntegrityError as e:
            logger.error(
                f"Error creating task {task_id} in workflow {workflow_id}: {e}"
            )
            return False
        except Exception as e:
            logger.error(f"Unexpected error creating task {task_id}: {e}")
            return False

    def update_task_status(
        self, task_id: str, status: str, started_at: Optional[datetime] = None
    ) -> bool:
        """Update task status."""
        try:
            sql = "UPDATE tasks SET status = ?, updated_at = CURRENT_TIMESTAMP"
            params = [status]

            if started_at:
                sql += ", started_at = ?"
                params.append(started_at.isoformat())

            sql += " WHERE task_id = ?"
            params.append(task_id)

            with self.transaction() as conn:
                conn.execute(sql, tuple(params))
            return True
        except Exception as e:
            logger.error(f"Error updating task {task_id} status: {e}")
            return False

    def update_task_completion(self, task_id: str, completed_at: datetime) -> bool:
        """Update task completion timestamp."""
        try:
            with self.transaction() as conn:
                conn.execute(
                    "UPDATE tasks SET completed_at = ?, status = 'completed' WHERE task_id = ?",
                    (completed_at.isoformat(), task_id),
                )
            return True
        except Exception as e:
            logger.error(f"Error updating task {task_id} completion: {e}")
            return False

    def create_task_execution(
        self,
        task_id: str,
        workflow_id: str,
        executor_runtime: str,
        execution_args: Optional[Dict[str, Any]] = None,
        execution_kwargs: Optional[Dict[str, Any]] = None,
        result: Optional[Any] = None,
        error_message: Optional[str] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        duration_ms: Optional[int] = None,
        status: str = "pending",
    ) -> bool:
        """Create a task execution record."""
        try:
            with self.transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO task_executions 
                    (execution_id, task_id, workflow_id, executor_runtime, 
                     execution_args_json, execution_kwargs_json, result_json, 
                     error_message, started_at, completed_at, duration_ms, status) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"{task_id}_exec_{int(datetime.now().timestamp())}",  # Generate unique ID
                        task_id,
                        workflow_id,
                        executor_runtime,
                        json.dumps(execution_args) if execution_args else None,
                        json.dumps(execution_kwargs) if execution_kwargs else None,
                        json.dumps(result) if result is not None else None,
                        error_message,
                        started_at.isoformat() if started_at else None,
                        completed_at.isoformat() if completed_at else None,
                        duration_ms,
                        status,
                    ),
                )
            return True
        except Exception as e:
            logger.error(f"Error creating task execution for {task_id}: {e}")
            return False

    def get_tasks_by_workflow(self, workflow_id: str) -> List[Dict[str, Any]]:
        """Get all tasks for a workflow."""
        results = self.fetch_all(
            """
            SELECT task_id, workflow_id, operator_type, runtime, function, image,
                   command_json, args_json, kwargs_json, result_key, dependencies_json,
                   created_at, started_at, completed_at, status
            FROM tasks 
            WHERE workflow_id = ?
            ORDER BY created_at
            """,
            (workflow_id,),
        )

        tasks = []
        for result in results:
            task = {
                "task_id": result["task_id"],
                "workflow_id": result["workflow_id"],
                "operator_type": result["operator_type"],
                "runtime": result["runtime"],
                "function": result["function"],
                "image": result["image"],
                "command": json.loads(result["command_json"])
                if result["command_json"]
                else None,
                "args": json.loads(result["args_json"])
                if result["args_json"]
                else None,
                "kwargs": json.loads(result["kwargs_json"])
                if result["kwargs_json"]
                else None,
                "result_key": result["result_key"],
                "dependencies": json.loads(result["dependencies_json"])
                if result["dependencies_json"]
                else None,
                "created_at": result["created_at"],
                "started_at": result["started_at"],
                "completed_at": result["completed_at"],
                "status": result["status"],
            }
            tasks.append(task)

        return tasks

    def get_task_executions(self, task_id: str) -> List[Dict[str, Any]]:
        """Get all executions for a task."""
        results = self.fetch_all(
            """
            SELECT execution_id, task_id, workflow_id, executor_runtime,
                   execution_args_json, execution_kwargs_json, result_json,
                   error_message, started_at, completed_at, duration_ms, status
            FROM task_executions 
            WHERE task_id = ?
            ORDER BY started_at DESC
            """,
            (task_id,),
        )

        executions = []
        for result in results:
            execution = {
                "execution_id": result["execution_id"],
                "task_id": result["task_id"],
                "workflow_id": result["workflow_id"],
                "executor_runtime": result["executor_runtime"],
                "execution_args": json.loads(result["execution_args_json"])
                if result["execution_args_json"]
                else None,
                "execution_kwargs": json.loads(result["execution_kwargs_json"])
                if result["execution_kwargs_json"]
                else None,
                "result": json.loads(result["result_json"])
                if result["result_json"]
                else None,
                "error_message": result["error_message"],
                "started_at": result["started_at"],
                "completed_at": result["completed_at"],
                "duration_ms": result["duration_ms"],
                "status": result["status"],
            }
            executions.append(execution)

        return executions

    def get_completed_tasks(self, workflow_id: str) -> set:
        """Get set of completed task IDs for a workflow."""
        results = self.fetch_all(
            """
            SELECT task_id 
            FROM tasks 
            WHERE workflow_id = ? AND status = 'completed'
            """,
            (workflow_id,),
        )
        return {row["task_id"] for row in results}

    def store_result(
        self, workflow_id: str, result_key: str, result_value: Any
    ) -> bool:
        """Store a result value for a workflow."""
        try:
            with self.transaction() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO workflow_results 
                    (workflow_id, result_key, result_value_json)
                    VALUES (?, ?, ?)
                    """,
                    (workflow_id, result_key, json.dumps(result_value)),
                )
            return True
        except Exception as e:
            logger.error(
                f"Error storing result {result_key} for workflow {workflow_id}: {e}"
            )
            return False

    def load_results(self, workflow_id: str) -> Dict[str, Any]:
        """Load all results for a workflow."""
        results = self.fetch_all(
            """
            SELECT result_key, result_value_json 
            FROM workflow_results 
            WHERE workflow_id = ?
            """,
            (workflow_id,),
        )

        return {
            row["result_key"]: json.loads(row["result_value_json"]) for row in results
        }

    def store_memory(
        self, workflow_id: str, memory_key: str, memory_value: Any
    ) -> bool:
        """Store a memory value for a workflow."""
        try:
            with self.transaction() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO workflow_memory 
                    (workflow_id, memory_key, memory_value_json, updated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (workflow_id, memory_key, json.dumps(memory_value)),
                )
            return True
        except Exception as e:
            logger.error(
                f"Error storing memory {memory_key} for workflow {workflow_id}: {e}"
            )
            return False

    def load_memory(self, workflow_id: str) -> Dict[str, Any]:
        """Load all memory for a workflow."""
        results = self.fetch_all(
            """
            SELECT memory_key, memory_value_json 
            FROM workflow_memory 
            WHERE workflow_id = ?
            """,
            (workflow_id,),
        )

        return {
            row["memory_key"]: json.loads(row["memory_value_json"]) for row in results
        }

    def store_dependencies(
        self, workflow_id: str, task_id: str, dependencies: List[str]
    ) -> bool:
        """Store task dependencies."""
        try:
            with self.transaction() as conn:
                # First, remove existing dependencies for this task
                conn.execute(
                    "DELETE FROM task_dependencies WHERE task_id = ? AND workflow_id = ?",
                    (task_id, workflow_id),
                )

                # Add new dependencies
                for dep_task_id in dependencies:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO task_dependencies 
                        (task_id, depends_on_task_id, workflow_id)
                        VALUES (?, ?, ?)
                        """,
                        (task_id, dep_task_id, workflow_id),
                    )
            return True
        except Exception as e:
            logger.error(f"Error storing dependencies for task {task_id}: {e}")
            return False

    def get_dependencies(self, task_id: str) -> List[str]:
        """Get dependencies for a task."""
        results = self.fetch_all(
            """
            SELECT depends_on_task_id 
            FROM task_dependencies 
            WHERE task_id = ?
            """,
            (task_id,),
        )
        return [row["depends_on_task_id"] for row in results]

    def get_dependents(self, task_id: str) -> List[str]:
        """Get tasks that depend on this task."""
        results = self.fetch_all(
            """
            SELECT task_id 
            FROM task_dependencies 
            WHERE depends_on_task_id = ?
            """,
            (task_id,),
        )
        return [row["task_id"] for row in results]

    def close(self) -> None:
        """Close the database connection for the current thread."""
        if hasattr(self._local, "connection"):
            self._local.connection.close()
            delattr(self._local, "connection")

    def close_all_connections(self) -> None:
        """Close all connections (call this on application shutdown)."""
        self.close()
