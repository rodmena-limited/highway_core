import functools
import logging
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from time import sleep
from typing import Any, Dict, Generator, List, Optional, Union

from sqlalchemy import Column, DateTime, create_engine, event, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from sqlalchemy.pool import QueuePool, StaticPool

from .models import (
    Base,
    Task,
    TaskDependency,
    TaskExecution,
    Workflow,
    WorkflowMemory,
    WorkflowResult,
)

logger = logging.getLogger(__name__)


def retry_on_lock_error(max_retries: int = 3, delay: float = 0.1):
    """
    Decorator to retry database operations that fail due to lock contention.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # Check for lock-related errors
                    error_str = str(e).lower()
                    if (
                        any(
                            lock_error in error_str
                            for lock_error in [
                                "database is locked",
                                "locked",
                                "busy",
                                "timeout",
                            ]
                        )
                        and attempt < max_retries - 1
                    ):
                        wait_time = delay * (2**attempt)  # Exponential backoff
                        logger.warning(
                            f"Database lock error on attempt {attempt + 1}, retrying in {wait_time:.2f}s: {e}"
                        )
                        sleep(wait_time)
                        continue
                    else:
                        # Re-raise if not a lock error or last attempt
                        raise
            return None

        return wrapper

    return decorator


class DatabaseManager:
    """
    Thread-safe database manager for Highway Core workflow persistence using SQLAlchemy.
    Provides proper connection pooling, transaction management, and resource cleanup.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """Implement singleton pattern to ensure only one instance exists."""
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, db_path: Optional[str] = None, engine_url: Optional[str] = None):
        """
        Initialize the database manager.
        This method is called every time __new__ returns the same instance,
        so we must guard against re-initialization.
        """
        # Check if instance is already initialized by checking for engine attribute
        if hasattr(self, 'engine'):
            return

        with self._lock:
            # Double-check after acquiring lock
            if hasattr(self, 'engine'):
                return

            self._session_lock = threading.RLock()  # For thread safety
            self._sessions_created = 0
            self._sessions_closed = 0
            self._active_sessions: set[int] = set()
            self._schema_lock = threading.Lock()  # Dedicated lock for schema operations
            self._initialized = False  # Track initialization status

            if engine_url is None:
                if db_path is None:
                    home = Path.home()
                    db_path = str(home / ".highway.sqlite3")

                Path(db_path).parent.mkdir(parents=True, exist_ok=True)
                self.engine_url = f"sqlite:///{db_path}"
            else:
                self.engine_url = engine_url

            # Create SQLAlchemy engine with proper configuration for SQLite concurrency
            if self.engine_url.startswith("sqlite://"):
                # Use QueuePool with proper settings for SQLite concurrent access
                self.engine = create_engine(
                    self.engine_url,
                    poolclass=QueuePool,  # Use QueuePool instead of StaticPool for better concurrency
                    pool_size=1,  # Single connection for SQLite to avoid locking issues
                    max_overflow=0,  # No additional connections
                    pool_pre_ping=True,  # Verify connections before use
                    pool_recycle=3600,  # Recycle connections after 1 hour
                    connect_args={
                        "check_same_thread": False,  # Allow cross-thread access
                        "timeout": 5.0,  # Shorter timeout for faster failure detection
                        "isolation_level": "EXCLUSIVE",  # Use EXCLUSIVE isolation for better consistency
                    },
                    echo=False,
                    echo_pool=False,
                )
                self._optimize_sqlite_connection()
            else:
                # For other database engines, use connection pooling
                self.engine = create_engine(
                    self.engine_url,
                    pool_size=16,
                    max_overflow=4,
                    pool_pre_ping=True,  # Verify connections before use
                    echo=False,
                )

            # Create scoped session factory with proper cleanup
            self.SessionLocal = scoped_session(
                sessionmaker(
                    bind=self.engine,
                    expire_on_commit=False,
                    autocommit=False,  # Disable autocommit for explicit transaction control
                    autoflush=False,
                    class_=Session,
                )
            )

            # Create schema
            self._initialize_schema()

            logger.info(f"DatabaseManager initialized with URL: {self.engine_url}")
            self._initialized = True

    def _optimize_sqlite_connection(self) -> None:
        """
        Applies PRAGMA statements for optimal SQLite concurrency and performance.
        """
        if not self.engine_url.startswith("sqlite://"):
            return

        @event.listens_for(self.engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            try:
                # Optimized settings for concurrent access
                cursor.execute(
                    "PRAGMA journal_mode=WAL"
                )  # Enable WAL for concurrent reads/writes
                cursor.execute(
                    "PRAGMA synchronous=NORMAL"
                )  # Balance safety and performance
                cursor.execute("PRAGMA cache_size=-64000")  # 64MB cache (negative = KB)
                cursor.execute(
                    "PRAGMA foreign_keys=ON"
                )  # Enable FK constraints for data integrity
                cursor.execute("PRAGMA locking_mode=EXCLUSIVE")  # Use EXCLUSIVE locking mode
                cursor.execute(
                    "PRAGMA temp_store=MEMORY"
                )  # Memory temp tables for speed
                cursor.execute(
                    "PRAGMA busy_timeout=5000"
                )  # 5 second timeout for lock contention
                cursor.execute(
                    "PRAGMA wal_autocheckpoint=1000"
                )  # WAL checkpoint every 1000 pages
                cursor.execute("PRAGMA mmap_size=268435456")  # 256MB memory mapping
                cursor.execute("PRAGMA page_size=4096")  # Standard page size
                logger.debug(
                    "Applied SQLite PRAGMA settings for concurrency optimization."
                )
            except Exception as e:
                logger.warning(f"Failed to apply SQLite PRAGMA settings: {e}")
            finally:
                cursor.close()

    def _initialize_schema(self) -> None:
        """Initialize the database schema with proper concurrency handling and race condition prevention."""
        with self._schema_lock:  # Use dedicated schema lock
            if self._initialized and hasattr(self, '_schema_created') and self._schema_created:
                return

            try:
                # First, ensure WAL mode is enabled for concurrent access
                if self.engine_url.startswith("sqlite://"):
                    try:
                        # Use a raw connection to enable WAL mode before any transactions
                        with self.engine.connect().execution_options(
                            isolation_level="AUTOCOMMIT"
                        ) as conn:
                            conn.execute(text("PRAGMA journal_mode=WAL"))
                            logger.debug("Enabled WAL mode for SQLite concurrency")
                    except Exception as e:
                        logger.warning(f"Could not enable WAL mode: {e}")

                # Check if tables already exist using a more reliable method
                try:
                    with self.session_scope() as session:
                        # Try to query workflows table
                        result = session.execute(
                            text(
                                "SELECT name FROM sqlite_master WHERE type='table' AND name='workflows'"
                            )
                        )
                        table_exists = result.fetchone() is not None

                        if table_exists:
                            self._schema_created = True
                            logger.info("Database schema already exists")
                            return
                except Exception as e:
                    logger.debug(f"Error checking table existence: {e}")

                # Create tables with proper error handling for concurrent creation
                try:
                    # Use a more robust approach - create tables individually
                    Base.metadata.create_all(bind=self.engine, checkfirst=True)
                    self._schema_created = True
                    logger.info("Database schema initialized successfully")
                except IntegrityError as e:
                    # Another process created the schema, verify it exists
                    logger.debug(f"Schema creation race condition handled: {e}")
                    try:
                        with self.session_scope() as session:
                            session.execute(text("SELECT 1 FROM workflows LIMIT 1"))
                        self._schema_created = True
                        logger.info("Schema verified after race condition")
                    except Exception:
                        logger.error(
                            "Schema does not exist after race condition handling"
                        )
                        raise
                except SQLAlchemyError as e:
                    # Handle other SQLAlchemy-specific errors
                    if (
                        "already exists" in str(e).lower()
                        or "disk I/O error" in str(e).lower()
                    ):
                        logger.debug(
                            f"Table already exists or I/O error, continuing: {e}"
                        )
                        self._schema_created = True
                    else:
                        logger.error(
                            f"Database error during schema initialization: {e}"
                        )
                        raise

            except Exception as e:
                logger.error(f"Failed to initialize database schema: {e}")
                # Don't set _schema_created = True on real errors
                raise

    @contextmanager
    def session_scope(self) -> Generator[Session, None, None]:
        """
        Provide a transactional scope around a series of operations with improved concurrency handling.
        This is the primary method for database access.
        """
        session = self._get_session()
        session_id = id(session)

        with self._session_lock:
            self._active_sessions.add(session_id)

        try:
            yield session
            # Use a more robust commit approach
            try:
                session.commit()
                logger.debug(f"Session {session_id} committed successfully")
            except Exception as commit_error:
                # If commit fails, try to rollback and re-raise
                try:
                    session.rollback()
                    logger.error(
                        f"Session {session_id} rolled back due to commit error: {commit_error}"
                    )
                except Exception as rollback_error:
                    logger.error(
                        f"Session {session_id} rollback failed after commit error: {rollback_error}"
                    )
                raise commit_error
        except Exception as e:
            # Enhanced rollback handling
            try:
                session.rollback()
                logger.error(f"Session {session_id} rolled back due to error: {e}")
            except Exception as rollback_error:
                logger.error(f"Session {session_id} rollback failed: {rollback_error}")
            raise
        finally:
            self._close_session(session)
            with self._session_lock:
                self._active_sessions.discard(session_id)

    @contextmanager
    def transaction(self) -> Generator[Session, None, None]:
        """Context manager for database transactions (alias for session_scope)."""
        with self.session_scope() as session:
            yield session

    @contextmanager
    def database_transaction(self) -> Generator[Session, None, None]:
        """Context manager for database transactions (backward compatibility)."""
        with self.session_scope() as session:
            yield session

    def _get_session(self) -> Session:
        """Get a thread-local database session with proper initialization."""
        with self._session_lock:
            session = self.SessionLocal()
            self._sessions_created += 1
            logger.debug(f"Session created (total: {self._sessions_created})")
            return session

    def _close_session(self, session: Optional[Session] = None) -> None:
        """Close a session and remove it from the registry."""
        with self._session_lock:
            try:
                if session is None:
                    session = self.SessionLocal()

                session.close()
                self.SessionLocal.remove()
                self._sessions_closed += 1
                logger.debug(f"Session closed (total: {self._sessions_closed})")
            except Exception as e:
                logger.warning(f"Error closing session: {e}")

    def execute_raw_sql(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute raw SQL with proper connection management."""
        with self.session_scope() as session:
            if params:
                result = session.execute(text(sql), params)
            else:
                result = session.execute(text(sql))
            return result

    def workflow_exists(self, workflow_id: str) -> bool:
        """Check if a workflow exists."""
        with self.session_scope() as session:
            result = (
                session.query(Workflow)
                .filter(Workflow.workflow_id == workflow_id)
                .first()
            )
            return result is not None

    def create_workflow(
        self,
        workflow_id: str,
        name: str,
        start_task: str,
        variables: Dict[str, Any],
    ) -> bool:
        """Create a new workflow record with retry logic for lock contention."""

        @retry_on_lock_error(max_retries=3, delay=0.1)
        def _create_workflow_internal():
            try:
                with self.session_scope() as session:
                    workflow = Workflow(
                        workflow_id=workflow_id,
                        workflow_name=name,
                        start_task=start_task,
                        variables=variables,
                        start_time=datetime.now(timezone.utc),
                        updated_at=datetime.now(timezone.utc),
                    )
                    session.add(workflow)
                    logger.info(f"Created workflow: {workflow_id}")
                return True
            except IntegrityError:
                logger.warning(f"Workflow with ID {workflow_id} already exists")
                return False
            except Exception as e:
                logger.error(f"Error creating workflow {workflow_id}: {e}")
                raise

        return _create_workflow_internal()

    def update_workflow_status(
        self, workflow_id: str, status: str, error_message: Optional[str] = None
    ) -> bool:
        """Update workflow status."""
        try:
            with self.session_scope() as session:
                workflow = (
                    session.query(Workflow)
                    .filter(Workflow.workflow_id == workflow_id)
                    .with_for_update()  # Lock the row for update
                    .first()
                )
                if workflow:
                    workflow.status = status  # type: ignore
                    workflow.updated_at = datetime.now(timezone.utc)  # type: ignore
                    if error_message:
                        workflow.error_message = error_message  # type: ignore
                    logger.debug(f"Updated workflow {workflow_id} status to {status}")
                    return True
                logger.warning(f"Workflow {workflow_id} not found for status update")
                return False
        except Exception as e:
            logger.error(f"Error updating workflow {workflow_id} status: {e}")
            return False

    def load_workflow(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Load a workflow by ID."""
        try:
            with self.session_scope() as session:
                workflow = (
                    session.query(Workflow)
                    .filter(Workflow.workflow_id == workflow_id)
                    .first()
                )

                if workflow:
                    return {
                        "workflow_id": workflow.workflow_id,
                        "name": workflow.name,
                        "start_task": workflow.start_task or "",
                        "variables": workflow.variables,
                        "created_at": workflow.start_time,
                        "updated_at": workflow.updated_at,
                        "status": workflow.status,
                        "error_message": workflow.error_message,
                    }
                return None
        except Exception as e:
            logger.warning(f"Error loading workflow {workflow_id}: {e}")
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
        status: str = "pending",
        error_message: Optional[str] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
    ) -> bool:
        """Create a new task record."""
        try:
            with self.session_scope() as session:
                # Check if workflow exists and create if not, handling race conditions
                workflow_exists = (
                    session.query(Workflow)
                    .filter(Workflow.workflow_id == workflow_id)
                    .first()
                    is not None
                )
                if not workflow_exists:
                    workflow = Workflow(
                        workflow_id=workflow_id,
                        workflow_name=workflow_id,
                        start_task="",
                        variables={},
                        start_time=datetime.now(timezone.utc),
                        updated_at=datetime.now(timezone.utc),
                    )
                    session.add(workflow)
                    try:
                        session.flush()  # Flush to try insert, but keep transaction open
                    except IntegrityError:
                        # Another thread created the workflow, ignore this error by refreshing
                        session.rollback()
                        # Don't continue with a different session, just continue with task creation
                        # The workflow might exist now after rollback
                        pass

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
                    updated_at=datetime.now(timezone.utc),
                )
                session.add(task)
                session.flush()  # Ensure the task is flushed within the transaction
                logger.debug(f"Created task: {task_id} in workflow: {workflow_id}")
            return True
        except IntegrityError as e:
            logger.error(f"Integrity error creating task {task_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error creating task {task_id}: {e}")
            return False

    def update_task_status(
        self,
        task_id: str,
        status: str,
        started_at: Optional[datetime] = None,
        error_message: Optional[str] = None,
    ) -> bool:
        """Update task status."""
        try:
            with self.session_scope() as session:
                task = (
                    session.query(Task)
                    .filter(Task.task_id == task_id)
                    .with_for_update()
                    .first()
                )
                if task:
                    task.status = status  # type: ignore
                    task.updated_at = datetime.now(timezone.utc)  # type: ignore
                    if started_at:
                        task.started_at = started_at  # type: ignore
                    if error_message:
                        task.error_message = error_message  # type: ignore
                    logger.debug(f"Updated task {task_id} status to {status}")
                    return True
                return False
        except Exception as e:
            logger.error(f"Error updating task {task_id} status: {e}")
            return False

    def update_task_completion(self, task_id: str, completed_at: datetime) -> bool:
        """Update task completion timestamp."""
        try:
            with self.session_scope() as session:
                task = (
                    session.query(Task)
                    .filter(Task.task_id == task_id)
                    .with_for_update()
                    .first()
                )
                if task:
                    task.completed_at = completed_at  # type: ignore
                    task.status = "completed"  # type: ignore
                    return True
                return False
        except Exception as e:
            logger.error(f"Error updating task {task_id} completion: {e}")
            return False

    def update_task_with_result(
        self,
        task_id: str,
        result: Any,
        completed_at: Optional[datetime] = None,
    ) -> bool:
        """Update task with result and completion status."""
        try:
            with self.session_scope() as session:
                task = (
                    session.query(Task)
                    .filter(Task.task_id == task_id)
                    .with_for_update()
                    .first()
                )
                if task:
                    task.result_value = result
                    task.status = "completed"  # type: ignore
                    task.completed_at = completed_at or datetime.now(timezone.utc)  # type: ignore
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
            with self.session_scope() as session:
                execution = TaskExecution(
                    execution_id=f"{task_id}_exec_{int(datetime.now(timezone.utc).timestamp())}",
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
        with self.session_scope() as session:
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
        with self.session_scope() as session:
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
        try:
            with self.session_scope() as session:
                completed_tasks = (
                    session.query(Task.task_id)
                    .filter(Task.workflow_id == workflow_id, Task.status == "completed")
                    .all()
                )

                return {task.task_id for task in completed_tasks}
        except Exception as e:
            logger.warning(
                f"Error getting completed tasks for workflow {workflow_id}: {e}"
            )
            return set()

    def store_result(
        self,
        workflow_id: str,
        task_id: str,
        result_key: str,
        result_value: Any,
    ) -> bool:
        """Store a result value for a task in a workflow with retry logic for lock contention."""

        @retry_on_lock_error(max_retries=3, delay=0.1)
        def _store_result_internal():
            try:
                with self.session_scope() as session:
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
                            result_key=result_key,  # type: ignore
                            result_value=result_value,
                        )
                        session.add(result_obj)
                return True
            except Exception as e:
                logger.error(
                    f"Error storing result {result_key} for task {task_id}: {e}"
                )
                raise

        return _store_result_internal()

    def load_results(self, workflow_id: str) -> Dict[str, Any]:
        """Load all results for a workflow."""
        with self.session_scope() as session:
            results = (
                session.query(WorkflowResult)
                .filter(WorkflowResult.workflow_id == workflow_id)
                .all()
            )

            return {result.result_key: result.result_value for result in results}  # type: ignore

    def store_memory(
        self, workflow_id: str, memory_key: str, memory_value: Any
    ) -> bool:
        """Store a memory value for a workflow."""
        try:
            with self.session_scope() as session:
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
                    memory_obj.updated_at = datetime.now(timezone.utc)  # type: ignore
                else:
                    memory_obj = WorkflowMemory(
                        workflow_id=workflow_id,
                        memory_key=memory_key,  # type: ignore
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
        with self.session_scope() as session:
            memory_entries = (
                session.query(WorkflowMemory)
                .filter(WorkflowMemory.workflow_id == workflow_id)
                .all()
            )

            return {memory.memory_key: memory.memory_value for memory in memory_entries}  # type: ignore

    def store_dependencies(
        self, workflow_id: str, task_id: str, dependencies: List[str]
    ) -> bool:
        """Store task dependencies."""
        try:
            with self.session_scope() as session:
                session.query(TaskDependency).filter(
                    TaskDependency.task_id == task_id,
                    TaskDependency.workflow_id == workflow_id,
                ).delete()

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
        with self.session_scope() as session:
            dependencies = (
                session.query(TaskDependency.depends_on_task_id)
                .filter(TaskDependency.task_id == task_id)
                .all()
            )

            return [dep.depends_on_task_id for dep in dependencies]

    def get_dependents(self, task_id: str) -> List[str]:
        """Get tasks that depend on this task."""
        with self.session_scope() as session:
            dependents = (
                session.query(TaskDependency.task_id)
                .filter(TaskDependency.depends_on_task_id == task_id)
                .all()
            )

            return [dep.task_id for dep in dependents]

    def get_session_stats(self) -> Dict[str, Any]:
        """Get session statistics for monitoring."""
        with self._session_lock:
            return {
                "sessions_created": self._sessions_created,
                "sessions_closed": self._sessions_closed,
                "active_sessions": len(self._active_sessions),
                "engine_url": self.engine_url,
            }

    def health_check(self) -> bool:
        """Perform a health check on the database connection."""
        try:
            with self.session_scope() as session:
                session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

    def close(self) -> None:
        """Close the current thread's session."""
        self._close_session()

    def close_all_connections(self) -> None:
        """Close all connections and cleanup resources."""
        with self._session_lock:
            try:
                # Close any remaining active sessions
                active_count = len(self._active_sessions)
                if active_count > 0:
                    logger.warning(f"Force closing {active_count} active sessions")

                # Remove all sessions from the registry
                self.SessionLocal.remove()

                # Dispose of the engine
                self.engine.dispose()

                logger.info("All database connections closed successfully")
                logger.info(f"Session statistics: {self.get_session_stats()}")
            except Exception as e:
                logger.error(f"Error closing all connections: {e}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with proper cleanup."""
        self.close_all_connections()


# Global function to get the database manager instance
def get_db_manager(db_path: Optional[str] = None, engine_url: Optional[str] = None) -> DatabaseManager:
    """Get the singleton instance of DatabaseManager."""
    return DatabaseManager(db_path=db_path, engine_url=engine_url)


# Usage example
def main():
    """Example usage with proper resource management."""

    # Recommended usage pattern 1: Context manager
    with DatabaseManager() as db:
        db.create_workflow("wf1", "Test Workflow", "task1", {})
        stats = db.get_session_stats()
        print(f"Database stats: {stats}")

    # Recommended usage pattern 2: Manual management
    db = DatabaseManager()
    try:
        db.create_task("wf1", "task1", "python_operator")
        results = db.load_results("wf1")
        print(f"Results: {results}")
    finally:
        db.close_all_connections()


if __name__ == "__main__":
    main()