import threading
import json
import os
from pathlib import Path
from contextlib import contextmanager
from typing import Optional, Dict, Any, List
import logging
from datetime import datetime
from sqlalchemy import create_engine, text, Column, DateTime
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import StaticPool
from .models import (
    Base,
    Workflow,
    Task,
    TaskExecution,
    WorkflowResult,
    WorkflowMemory,
    TaskDependency,
)


logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Thread-safe database manager for Highway Core workflow persistence using SQLAlchemy.
    Manages connections, transactions, and provides a clean interface
    for all database operations with abstracted database engine.
    """

    def __init__(self, db_path: Optional[str] = None, engine_url: Optional[str] = None):
        """
        Initialize the database manager.

        Args:
            db_path: Path to the SQLite database file. If None, uses ~/.highway.sqlite3
            engine_url: Database engine URL. If None, defaults to SQLite with db_path
        """
        if engine_url is None:
            if db_path is None:
                # Default to user's home directory
                home = Path.home()
                db_path = home / ".highway.sqlite3"

            # Ensure the directory exists
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)

            self.engine_url = f"sqlite:///{db_path}"
        else:
            self.engine_url = engine_url

        # Create SQLAlchemy engine with proper configuration for thread safety
        if self.engine_url.startswith("sqlite://"):
            self.engine = create_engine(
                self.engine_url,
                poolclass=StaticPool,  # Use StaticPool for SQLite to avoid issues with multiple threads
                connect_args={
                    "check_same_thread": False,
                    "timeout": 30.0,  # Increase timeout for busy operations
                },
                echo=False,  # Set to True for SQL debugging
            )
        else:
            # For other database engines
            self.engine = create_engine(
                self.engine_url,
                echo=False,  # Set to True for SQL debugging
            )

        # Create session factory
        self.SessionLocal = sessionmaker(bind=self.engine, expire_on_commit=False)

        # Thread-local storage for sessions
        self._local = threading.local()

        # Create schema
        self._initialize_schema()

    def _get_session(self) -> Session:
        """Get a thread-local database session."""
        if not hasattr(self._local, "session"):
            self._local.session = self.SessionLocal()
        return self._local.session

    def _initialize_schema(self) -> None:
        """Initialize the database schema using SQLAlchemy metadata."""
        Base.metadata.create_all(bind=self.engine)

        # Add missing columns if needed for backward compatibility (as much as possible with SQLAlchemy)
        # For SQLite, we need to use raw SQL for column additions since SQLAlchemy doesn't handle this well
        if self.engine_url.startswith("sqlite://"):
            with self.engine.connect() as conn:
                # Check if tasks table has updated_at column
                result = conn.execute(text("PRAGMA table_info(tasks)"))
                columns = [row[1] for row in result.fetchall()]

                if "updated_at" not in columns:
                    try:
                        # Add updated_at column to tasks table
                        conn.execute(
                            text(
                                "ALTER TABLE tasks ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                            )
                        )
                        conn.commit()
                    except Exception:
                        # Column might already exist
                        pass

    @contextmanager
    def transaction(self):
        """Context manager for database transactions."""
        session = self._get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise

    @contextmanager
    def database_transaction(self):
        """Context manager for database transactions (for backward compatibility)."""
        # For SQLAlchemy, both methods do the same thing
        with self.transaction() as session:
            yield session

    def execute_raw_sql(self, sql: str, params: Dict[str, Any] = None) -> Any:
        """Execute raw SQL for cases where SQLAlchemy ORM is not suitable."""
        with self.engine.connect() as conn:
            if params:
                result = conn.execute(text(sql), params)
            else:
                result = conn.execute(text(sql))
            conn.commit()
            return result

    def workflow_exists(self, workflow_id: str) -> bool:
        """Check if a workflow exists."""
        session = self._get_session()
        result = (
            session.query(Workflow).filter(Workflow.workflow_id == workflow_id).first()
        )
        return result is not None

    def create_workflow(
        self, workflow_id: str, name: str, start_task: str, variables: Dict[str, Any]
    ) -> bool:
        """Create a new workflow record."""
        try:
            with self.transaction() as session:
                workflow = Workflow(
                    workflow_id=workflow_id,
                    workflow_name=name,  # Use workflow_name field that matches the schema
                    start_task=start_task,
                    variables=variables,
                    start_time=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                )
                session.add(workflow)
            return True
        except IntegrityError:
            logger.error(f"Workflow with ID {workflow_id} already exists")
            return False
        except Exception as e:
            logger.error(f"Error creating workflow {workflow_id}: {e}")
            return False

    def update_workflow_status(self, workflow_id: str, status: str) -> bool:
        """Update workflow status."""
        try:
            with self.transaction() as session:
                workflow = (
                    session.query(Workflow)
                    .filter(Workflow.workflow_id == workflow_id)
                    .first()
                )
                if workflow:
                    workflow.status = status
                    workflow.updated_at = datetime.utcnow()
                    return True
                return False
        except Exception as e:
            logger.error(f"Error updating workflow {workflow_id} status: {e}")
            return False

    def load_workflow(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Load a workflow by ID."""
        session = self._get_session()
        workflow = (
            session.query(Workflow).filter(Workflow.workflow_id == workflow_id).first()
        )

        if workflow:
            return {
                "workflow_id": workflow.workflow_id,
                "name": workflow.name,  # Using the property alias
                "start_task": workflow.start_task or "",  # Use the field we added
                "variables": workflow.variables,
                "created_at": workflow.start_time,  # Use start_time which matches original schema
                "updated_at": workflow.updated_at,  # Now this field exists
                "status": workflow.status,
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
        error_message: Optional[str] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
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

            with self.transaction() as session:
                task = Task(
                    task_id=task_id,
                    workflow_id=workflow_id,
                    operator_type=operator_type,
                    runtime=runtime,
                    function=function,
                    image=image,
                    command=command,
                    args=args,
                    kwargs=kwargs,
                    result_key=result_key,
                    dependencies_list=dependencies,
                    status=status,
                    error_message=error_message,
                    started_at=started_at,
                    completed_at=completed_at,
                    updated_at=datetime.utcnow(),
                )
                session.add(task)
            return True
        except IntegrityError as e:
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
            with self.transaction() as session:
                task = session.query(Task).filter(Task.task_id == task_id).first()
                if task:
                    task.status = status
                    task.updated_at = datetime.utcnow()
                    if started_at:
                        task.started_at = started_at
                    return True
                return False
        except Exception as e:
            logger.error(f"Error updating task {task_id} status: {e}")
            return False

    def update_task_completion(self, task_id: str, completed_at: datetime) -> bool:
        """Update task completion timestamp."""
        try:
            with self.transaction() as session:
                task = session.query(Task).filter(Task.task_id == task_id).first()
                if task:
                    task.completed_at = completed_at
                    task.status = "completed"
                    return True
                return False
        except Exception as e:
            logger.error(f"Error updating task {task_id} completion: {e}")
            return False

    def update_task_with_result(
        self, task_id: str, result: Any, completed_at: datetime = None
    ) -> bool:
        """Update task with result and completion status."""
        try:
            with self.transaction() as session:
                task = session.query(Task).filter(Task.task_id == task_id).first()
                if task:
                    task.result_value = result
                    task.status = "completed"
                    if completed_at:
                        task.completed_at = completed_at
                    else:
                        task.completed_at = datetime.utcnow()
                    return True
                return False
        except Exception as e:
            logger.error(f"Error updating task {task_id} with result: {e}")
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
            with self.transaction() as session:
                execution = TaskExecution(
                    execution_id=f"{task_id}_exec_{int(datetime.utcnow().timestamp())}",  # Generate unique ID
                    task_id=task_id,
                    workflow_id=workflow_id,
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
                session.add(execution)
            return True
        except Exception as e:
            logger.error(f"Error creating task execution for {task_id}: {e}")
            return False

    def get_tasks_by_workflow(self, workflow_id: str) -> List[Dict[str, Any]]:
        """Get all tasks for a workflow."""
        session = self._get_session()
        tasks = (
            session.query(Task)
            .filter(Task.workflow_id == workflow_id)
            .order_by(Task.created_at)
            .all()
        )

        task_list = []
        for task in tasks:
            task_dict = {
                "task_id": task.task_id,
                "workflow_id": task.workflow_id,
                "operator_type": task.operator_type,
                "runtime": task.runtime,
                "function": task.function,
                "image": task.image,
                "command": task.command,
                "args": task.args,
                "kwargs": task.kwargs,
                "result_key": task.result_key,
                "dependencies": task.dependencies_list,
                "created_at": task.created_at,
                "started_at": task.started_at,
                "completed_at": task.completed_at,
                "status": task.status,
            }
            task_list.append(task_dict)

        return task_list

    def get_task_executions(self, task_id: str) -> List[Dict[str, Any]]:
        """Get all executions for a task."""
        session = self._get_session()
        executions = (
            session.query(TaskExecution)
            .filter(TaskExecution.task_id == task_id)
            .order_by(TaskExecution.created_at.desc())
            .all()
        )

        execution_list = []
        for execution in executions:
            execution_dict = {
                "execution_id": execution.execution_id,
                "task_id": execution.task_id,
                "workflow_id": execution.workflow_id,
                "executor_runtime": execution.executor_runtime,
                "execution_args": execution.execution_args,
                "execution_kwargs": execution.execution_kwargs,
                "result": execution.result,
                "error_message": execution.error_message,
                "started_at": execution.started_at,
                "completed_at": execution.completed_at,
                "duration_ms": execution.duration_ms,
                "status": execution.status,
            }
            execution_list.append(execution_dict)

        return execution_list

    def get_completed_tasks(self, workflow_id: str) -> set:
        """Get set of completed task IDs for a workflow."""
        session = self._get_session()
        completed_tasks = (
            session.query(Task.task_id)
            .filter(Task.workflow_id == workflow_id, Task.status == "completed")
            .all()
        )

        return {task.task_id for task in completed_tasks}

    def store_result(
        self, workflow_id: str, task_id: str, result_key: str, result_value: Any
    ) -> bool:
        """Store a result value for a task in a workflow (matches original schema with FK to tasks)."""
        try:
            with self.transaction() as session:
                # Check if result already exists, if so, update it, otherwise create new
                result_obj = (
                    session.query(WorkflowResult)
                    .filter(
                        WorkflowResult.workflow_id == workflow_id,
                        WorkflowResult.task_id == task_id,
                        WorkflowResult.result_key == result_key,
                    )
                    .first()
                )

                if result_obj:
                    result_obj.result_value = result_value
                else:
                    result_obj = WorkflowResult(
                        workflow_id=workflow_id,
                        task_id=task_id,
                        result_key=result_key,
                        result_value=result_value,
                    )
                    session.add(result_obj)
            return True
        except Exception as e:
            logger.error(
                f"Error storing result {result_key} for task {task_id} in workflow {workflow_id}: {e}"
            )
            return False

    def load_results(self, workflow_id: str) -> Dict[str, Any]:
        """Load all results for a workflow."""
        session = self._get_session()
        results = (
            session.query(WorkflowResult)
            .filter(WorkflowResult.workflow_id == workflow_id)
            .all()
        )

        return {result.result_key: result.result_value for result in results}

    def store_memory(
        self, workflow_id: str, memory_key: str, memory_value: Any
    ) -> bool:
        """Store a memory value for a workflow."""
        try:
            with self.transaction() as session:
                # Check if memory already exists, if so, update it, otherwise create new
                memory_obj = (
                    session.query(WorkflowMemory)
                    .filter(
                        WorkflowMemory.workflow_id == workflow_id,
                        WorkflowMemory.memory_key == memory_key,
                    )
                    .first()
                )

                if memory_obj:
                    memory_obj.memory_value = memory_value
                    memory_obj.updated_at = datetime.utcnow()
                else:
                    memory_obj = WorkflowMemory(
                        workflow_id=workflow_id,
                        memory_key=memory_key,
                        memory_value=memory_value,
                    )
                    session.add(memory_obj)
            return True
        except Exception as e:
            logger.error(
                f"Error storing memory {memory_key} for workflow {workflow_id}: {e}"
            )
            return False

    def load_memory(self, workflow_id: str) -> Dict[str, Any]:
        """Load all memory for a workflow."""
        session = self._get_session()
        memory_entries = (
            session.query(WorkflowMemory)
            .filter(WorkflowMemory.workflow_id == workflow_id)
            .all()
        )

        return {memory.memory_key: memory.memory_value for memory in memory_entries}

    def store_dependencies(
        self, workflow_id: str, task_id: str, dependencies: List[str]
    ) -> bool:
        """Store task dependencies."""
        try:
            with self.transaction() as session:
                # First, remove existing dependencies for this task
                session.query(TaskDependency).filter(
                    TaskDependency.task_id == task_id,
                    TaskDependency.workflow_id == workflow_id,
                ).delete()

                # Add new dependencies
                for dep_task_id in dependencies:
                    dependency = TaskDependency(
                        task_id=task_id,
                        depends_on_task_id=dep_task_id,
                        workflow_id=workflow_id,
                    )
                    session.add(dependency)
            return True
        except Exception as e:
            logger.error(f"Error storing dependencies for task {task_id}: {e}")
            return False

    def get_dependencies(self, task_id: str) -> List[str]:
        """Get dependencies for a task."""
        session = self._get_session()
        dependencies = (
            session.query(TaskDependency.depends_on_task_id)
            .filter(TaskDependency.task_id == task_id)
            .all()
        )

        return [dep.depends_on_task_id for dep in dependencies]

    def get_dependents(self, task_id: str) -> List[str]:
        """Get tasks that depend on this task."""
        session = self._get_session()
        dependents = (
            session.query(TaskDependency.task_id)
            .filter(TaskDependency.depends_on_task_id == task_id)
            .all()
        )

        return [dep.task_id for dep in dependents]

    def close(self) -> None:
        """Close the database session for the current thread."""
        if hasattr(self._local, "session"):
            self._local.session.close()
            delattr(self._local, "session")

    def close_all_connections(self) -> None:
        """Close all connections (call this on application shutdown)."""
        self.engine.dispose()
