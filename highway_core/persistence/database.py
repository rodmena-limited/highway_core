import functools
import logging
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import sleep
from typing import Any, Dict, Generator, List, Optional, Union

from sqlalchemy import (
    Column,
    DateTime,
    and_,
    case,
    create_engine,
    event,
    func,
    inspect,
    text,
)
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from sqlalchemy.pool import QueuePool, StaticPool

from .models import (
    AdminTask,
    Base,
    Task,
    TaskDependency,
    TaskExecution,
    Webhook,
    Workflow,
    WorkflowMemory,
    WorkflowResult,
    WorkflowTemplate,
)

logger = logging.getLogger(__name__)


def retry_on_lock_error(max_retries: int = 3, delay: float = 0.1):
    """
    Decorator to retry database operations that fail due to lock contention
    (SQLite) or deadlock (PostgreSQL).
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except OperationalError as e:  # Catch general DB errors
                    error_str = str(e).lower()
                    if (
                        any(
                            lock_error in error_str
                            for lock_error in [
                                "database is locked",
                                "locked",
                                "busy",
                                "timeout",
                                "deadlock detected",  # PostgreSQL
                                "lock not available",  # PostgreSQL
                            ]
                        )
                        and attempt < max_retries - 1
                    ):
                        wait_time = delay * (2**attempt)  # Exponential backoff
                        logger.warning(
                            f"Database lock/deadlock error on attempt {attempt + 1}, retrying in {wait_time:.2f}s: {e}"
                        )
                        sleep(wait_time)
                        continue
                    else:
                        # Re-raise if not a lock error or last attempt
                        raise
                except Exception as e:
                    # Re-raise other exceptions immediately
                    raise
            return None  # Should not be reached if retries fail, as exception is raised

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
        if hasattr(self, "engine"):
            return

        with self._lock:
            # Double-check after acquiring lock
            if hasattr(self, "engine"):
                return

            self._session_lock = threading.RLock()  # For thread safety
            self._sessions_created = 0
            self._sessions_closed = 0
            self._active_sessions: set[int] = set()
            self._schema_lock = threading.Lock()  # Dedicated lock for schema operations
            self._initialized = False  # Track initialization status

            if engine_url is None:
                if db_path is None:
                    # This will now get the correct URL from config.py
                    from highway_core.config import settings

                    self.engine_url = settings.DATABASE_URL
                else:
                    # Use explicit db_path if provided
                    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
                    self.engine_url = f"sqlite:///{db_path}"
            else:
                self.engine_url = engine_url

            # Create SQLAlchemy engine with proper configuration for DB dialect
            if self.engine_url.startswith("sqlite://"):
                # Use QueuePool with proper settings for SQLite concurrent access
                self.engine = create_engine(
                    self.engine_url,
                    poolclass=QueuePool,  # Use QueuePool instead of StaticPool
                    pool_size=1,  # Single connection for SQLite
                    max_overflow=0,  # No additional connections
                    pool_pre_ping=True,  # Verify connections before use
                    pool_recycle=3600,  # Recycle connections after 1 hour
                    connect_args={
                        "check_same_thread": False,  # Allow cross-thread access
                        "timeout": 5.0,
                    },
                    echo=False,
                    echo_pool=False,
                )
                self._optimize_sqlite_connection()
                logger.info("DatabaseManager initialized with SQLite backend.")

            elif self.engine_url.startswith("postgresql+psycopg"):
                # For PostgreSQL, use a standard connection pool
                self.engine = create_engine(
                    self.engine_url,
                    poolclass=QueuePool,
                    pool_size=10,  # Pool of 10 connections
                    max_overflow=5,  # Allow 5 more
                    pool_pre_ping=True,
                    pool_recycle=1800,  # Recycle connections every 30 mins
                    echo=False,
                )
                logger.info("DatabaseManager initialized with PostgreSQL backend.")

            else:
                raise ValueError(f"Unsupported database engine URL: {self.engine_url}")

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
                cursor.execute(
                    "PRAGMA locking_mode=EXCLUSIVE"
                )  # Use EXCLUSIVE locking mode
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
            if (
                self._initialized
                and hasattr(self, "_schema_created")
                and self._schema_created
            ):
                return

            try:
                # Special WAL pragma for SQLite
                if self.engine_url.startswith("sqlite://"):
                    try:
                        with self.engine.connect() as conn:
                            conn.execute(text("PRAGMA journal_mode=WAL"))
                            logger.debug("Enabled WAL mode for SQLite concurrency")
                    except Exception as e:
                        logger.warning(f"Could not enable WAL mode: {e}")

                # Check if tables already exist using the dialect-agnostic inspector
                try:
                    inspector = inspect(self.engine)
                    table_exists = inspector.has_table("workflows")

                    if table_exists:
                        self._schema_created = True
                        logger.info("Database schema already exists")
                        return
                except Exception as e:
                    logger.debug(f"Error checking table existence: {e}")

                # Create tables
                try:
                    Base.metadata.create_all(bind=self.engine, checkfirst=True)
                    self._schema_created = True
                    logger.info("Database schema created successfully.")
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
                    if "already exists" in str(e).lower():
                        logger.debug(f"Table already exists, continuing: {e}")
                        self._schema_created = True
                    else:
                        logger.error(
                            f"Database error during schema initialization: {e}"
                        )
                        raise

            except Exception as e:
                logger.error(f"Failed to initialize database schema: {e}")
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

        @retry_on_lock_error(max_retries=3, delay=0.1)
        def _update_workflow_status_internal():
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
                        logger.debug(
                            f"Updated workflow {workflow_id} status to {status}"
                        )
                        return True
                    logger.warning(
                        f"Workflow {workflow_id} not found for status update"
                    )
                    return False
            except Exception as e:
                logger.error(f"Error updating workflow {workflow_id} status: {e}")
                raise

        return _update_workflow_status_internal()

    def update_workflow_variables(
        self, workflow_id: str, variables: Dict[str, Any]
    ) -> bool:
        """Update workflow variables."""

        @retry_on_lock_error(max_retries=3, delay=0.1)
        def _update_workflow_variables_internal():
            try:
                with self.session_scope() as session:
                    workflow = (
                        session.query(Workflow)
                        .filter(Workflow.workflow_id == workflow_id)
                        .with_for_update()  # Lock the row for update
                        .first()
                    )
                    if workflow:
                        workflow.variables = variables  # type: ignore
                        workflow.updated_at = datetime.now(timezone.utc)  # type: ignore
                        logger.debug(f"Updated workflow {workflow_id} variables")
                        return True
                    logger.warning(
                        f"Workflow {workflow_id} not found for variables update"
                    )
                    return False
            except Exception as e:
                logger.error(f"Error updating workflow {workflow_id} variables: {e}")
                raise

        return _update_workflow_variables_internal()

    def store_workflow_result(self, workflow_id: str, result: Any) -> bool:
        """Store workflow-level result."""

        @retry_on_lock_error(max_retries=3, delay=0.1)
        def _store_workflow_result_internal():
            try:
                with self.session_scope() as session:
                    # Store as a special workflow result with task_id="__workflow__"
                    result_obj = WorkflowResult(
                        workflow_id=workflow_id,
                        task_id="__workflow__",
                        result_key="workflow_result",
                        result_value=result,
                        created_at=datetime.now(timezone.utc),
                    )
                    session.add(result_obj)
                    logger.debug(f"Stored workflow result for {workflow_id}")
                    return True
            except Exception as e:
                logger.error(f"Error storing workflow result for {workflow_id}: {e}")
                raise

        return _store_workflow_result_internal()

    def get_workflow_result(self, workflow_id: str) -> Any:
        """Get workflow-level result."""
        try:
            with self.session_scope() as session:
                result_obj = (
                    session.query(WorkflowResult)
                    .filter(
                        WorkflowResult.workflow_id == workflow_id,
                        WorkflowResult.task_id == "__workflow__",
                        WorkflowResult.result_key == "workflow_result",
                    )
                    .first()
                )

                if result_obj:
                    return result_obj.result_value
                return None
        except Exception as e:
            logger.error(f"Error getting workflow result for {workflow_id}: {e}")
            return None

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

        @retry_on_lock_error(max_retries=3, delay=0.1)
        def _create_task_internal():
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
                raise  # Re-raise so the retry decorator can catch it
            except Exception as e:
                logger.error(f"Unexpected error creating task {task_id}: {e}")
                raise  # Re-raise so the retry decorator can catch it

        return _create_task_internal()

    def update_task_status(
        self,
        task_id: str,
        status: str,
        started_at: Optional[datetime] = None,
        error_message: Optional[str] = None,
    ) -> bool:
        """Update task status."""

        @retry_on_lock_error(max_retries=3, delay=0.1)
        def _update_task_status_internal():
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
                raise

        return _update_task_status_internal()

    def update_task_completion(self, task_id: str, completed_at: datetime) -> bool:
        """Update task completion timestamp."""

        @retry_on_lock_error(max_retries=3, delay=0.1)
        def _update_task_completion_internal():
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
                raise

        return _update_task_completion_internal()

    def update_task_with_result(
        self,
        task_id: str,
        result: Any,
        completed_at: Optional[datetime] = None,
    ) -> bool:
        """Update task with result and completion status."""

        @retry_on_lock_error(max_retries=3, delay=0.1)
        def _update_task_with_result_internal():
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
                raise

        return _update_task_with_result_internal()

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

        @retry_on_lock_error(max_retries=3, delay=0.1)
        def _store_memory_internal():
            try:
                with self.session_scope() as session:
                    memory_obj = (
                        session.query(WorkflowMemory)
                        .filter(
                            WorkflowMemory.workflow_id == workflow_id,
                            WorkflowMemory.memory_key == memory_key,
                        )
                        .with_for_update()  # Lock the row for update
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
                raise

        return _store_memory_internal()

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

    def get_dependents(self, task_id: str) -> List[str]:
        """Get tasks that depend on this task."""
        with self.session_scope() as session:
            dependents = (
                session.query(TaskDependency.task_id)
                .filter(TaskDependency.depends_on_task_id == task_id)
                .all()
            )

            return [dep.task_id for dep in dependents]

    # Webhook Management Methods
    def create_webhook(
        self,
        task_id: str,
        execution_id: str,
        workflow_id: str,
        url: str,
        method: str = "POST",
        headers: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        webhook_type: str = "on_complete",
        status: str = "pending",
        max_retries: int = 3,
        rate_limit_requests: int = 10,
        rate_limit_window: int = 60,
    ) -> bool:
        """Create a new webhook record."""

        @retry_on_lock_error(max_retries=3, delay=0.1)
        def _create_webhook_internal():
            try:
                with self.session_scope() as session:
                    # Check if webhook with same execution_id, task_id, workflow_id, and webhook_type already exists
                    existing_webhook = (
                        session.query(Webhook)
                        .filter(
                            Webhook.execution_id == execution_id,
                            Webhook.task_id == task_id,
                            Webhook.workflow_id == workflow_id,
                            Webhook.webhook_type == webhook_type,
                        )
                        .first()
                    )

                    if existing_webhook:
                        logger.warning(
                            f"Webhook already exists for task {task_id}, workflow {workflow_id}, type {webhook_type}"
                        )
                        return False

                    webhook = Webhook(
                        task_id=task_id,
                        execution_id=execution_id,
                        workflow_id=workflow_id,
                        url=url,
                        method=method,
                        headers=headers or {},
                        payload=payload or {},
                        webhook_type=webhook_type,
                        status=status,
                        max_retries=max_retries,
                        rate_limit_requests=rate_limit_requests,
                        rate_limit_window=rate_limit_window,
                        created_at=datetime.now(timezone.utc),
                        updated_at=datetime.now(timezone.utc),
                    )
                    session.add(webhook)
                    logger.debug(
                        f"Created webhook for task {task_id}, workflow {workflow_id}, type {webhook_type}"
                    )
                    return True
            except Exception as e:
                logger.error(f"Error creating webhook: {e}")
                raise

        return _create_webhook_internal()

    def get_ready_webhooks(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """Get webhooks that are ready to be sent (pending or scheduled for retry)."""
        with self.session_scope() as session:
            # Get webhooks that are pending or have next_retry_at in the past
            webhooks = (
                session.query(Webhook)
                .filter(
                    Webhook.status.in_(["pending", "retry_scheduled"]),
                    (Webhook.next_retry_at.is_(None))
                    | (Webhook.next_retry_at <= datetime.now(timezone.utc)),
                )
                .order_by(Webhook.created_at)
                .limit(limit)
                .all()
            )

            webhook_list = []
            for webhook in webhooks:
                webhook_dict = {
                    "id": webhook.id,
                    "task_id": webhook.task_id,
                    "execution_id": webhook.execution_id,
                    "workflow_id": webhook.workflow_id,
                    "url": webhook.url,
                    "method": webhook.method,
                    "headers": webhook.headers,
                    "payload": webhook.payload,
                    "webhook_type": webhook.webhook_type,
                    "status": webhook.status,
                    "retry_count": webhook.retry_count,
                    "max_retries": webhook.max_retries,
                    "next_retry_at": webhook.next_retry_at,
                    "response_status": webhook.response_status,
                    "response_headers": webhook.response_headers,
                    "response_body": webhook.response_body,
                    "rate_limit_requests": webhook.rate_limit_requests,
                    "rate_limit_window": webhook.rate_limit_window,
                    "created_at": webhook.created_at,
                    "updated_at": webhook.updated_at,
                }
                webhook_list.append(webhook_dict)

            return webhook_list

    def update_webhook_response(
        self,
        webhook_id: int,
        response_status: int,
        response_headers: Dict[str, Any],
        response_body: str,
        success: bool = True,
    ) -> bool:
        """Update a webhook record with response information."""

        @retry_on_lock_error(max_retries=3, delay=0.1)
        def _update_webhook_response_internal():
            try:
                with self.session_scope() as session:
                    webhook = (
                        session.query(Webhook)
                        .filter(Webhook.id == webhook_id)
                        .with_for_update()
                        .first()
                    )
                    if webhook:
                        webhook.response_status = response_status
                        webhook.response_headers = response_headers
                        webhook.response_body = response_body
                        webhook.updated_at = datetime.now(timezone.utc)

                        if success:
                            webhook.status = "sent"
                        else:
                            webhook.status = "failed"

                        logger.debug(
                            f"Updated webhook {webhook_id} with response status {response_status}"
                        )
                        return True
                    return False
            except Exception as e:
                logger.error(f"Error updating webhook {webhook_id} response: {e}")
                raise

        return _update_webhook_response_internal()

    def schedule_webhook_retry(self, webhook_id: int) -> bool:
        """Schedule a webhook for retry with exponential backoff."""

        @retry_on_lock_error(max_retries=3, delay=0.1)
        def _schedule_webhook_retry_internal():
            try:
                with self.session_scope() as session:
                    webhook = (
                        session.query(Webhook)
                        .filter(Webhook.id == webhook_id)
                        .with_for_update()
                        .first()
                    )
                    if webhook:
                        # Exponential backoff with jitter: base_delay * 2^retry_count + random jitter
                        base_delay = 5  # 5 seconds base delay
                        multiplier = 2**webhook.retry_count
                        jitter = 1  # Add up to 1 second of jitter

                        import random

                        delay = (base_delay * multiplier) + random.uniform(0, jitter)

                        webhook.next_retry_at = datetime.now(timezone.utc) + timedelta(
                            seconds=delay
                        )
                        webhook.status = "retry_scheduled"
                        webhook.retry_count += 1
                        webhook.updated_at = datetime.now(timezone.utc)

                        logger.debug(
                            f"Scheduled retry for webhook {webhook_id} in {delay:.2f} seconds"
                        )
                        return True
                    return False
            except Exception as e:
                logger.error(f"Error scheduling webhook {webhook_id} retry: {e}")
                raise

        return _schedule_webhook_retry_internal()

    def get_webhooks_by_workflow(self, workflow_id: str) -> List[Dict[str, Any]]:
        """Get all webhooks for a specific workflow."""
        with self.session_scope() as session:
            webhooks = (
                session.query(Webhook)
                .filter(Webhook.workflow_id == workflow_id)
                .order_by(Webhook.created_at)
                .all()
            )

            webhook_list = []
            for webhook in webhooks:
                webhook_dict = {
                    "id": webhook.id,
                    "task_id": webhook.task_id,
                    "execution_id": webhook.execution_id,
                    "workflow_id": webhook.workflow_id,
                    "url": webhook.url,
                    "method": webhook.method,
                    "headers": webhook.headers,
                    "payload": webhook.payload,
                    "webhook_type": webhook.webhook_type,
                    "status": webhook.status,
                    "retry_count": webhook.retry_count,
                    "max_retries": webhook.max_retries,
                    "next_retry_at": webhook.next_retry_at,
                    "response_status": webhook.response_status,
                    "response_headers": webhook.response_headers,
                    "response_body": webhook.response_body,
                    "rate_limit_requests": webhook.rate_limit_requests,
                    "rate_limit_window": webhook.rate_limit_window,
                    "created_at": webhook.created_at,
                    "updated_at": webhook.updated_at,
                }
                webhook_list.append(webhook_dict)

            return webhook_list

    def get_webhooks_by_status(self, status_list: List[str]) -> List[Dict[str, Any]]:
        """Get webhooks by status."""
        with self.session_scope() as session:
            webhooks = (
                session.query(Webhook)
                .filter(Webhook.status.in_(status_list))
                .order_by(Webhook.created_at)
                .all()
            )

            webhook_list = []
            for webhook in webhooks:
                webhook_dict = {
                    "id": webhook.id,
                    "task_id": webhook.task_id,
                    "execution_id": webhook.execution_id,
                    "workflow_id": webhook.workflow_id,
                    "url": webhook.url,
                    "method": webhook.method,
                    "headers": webhook.headers,
                    "payload": webhook.payload,
                    "webhook_type": webhook.webhook_type,
                    "status": webhook.status,
                    "retry_count": webhook.retry_count,
                    "max_retries": webhook.max_retries,
                    "next_retry_at": webhook.next_retry_at,
                    "response_status": webhook.response_status,
                    "response_headers": webhook.response_headers,
                    "response_body": webhook.response_body,
                    "rate_limit_requests": webhook.rate_limit_requests,
                    "rate_limit_window": webhook.rate_limit_window,
                    "created_at": webhook.created_at,
                    "updated_at": webhook.updated_at,
                }
                webhook_list.append(webhook_dict)

            return webhook_list

    def cleanup_completed_webhooks(self, days_old: int = 30) -> int:
        """Remove old completed webhooks."""
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)

        try:
            with self.session_scope() as session:
                # Count how many will be deleted
                count = (
                    session.query(Webhook)
                    .filter(Webhook.status == "sent", Webhook.updated_at < cutoff_date)
                    .count()
                )

                # Actually delete them
                deleted_count = (
                    session.query(Webhook)
                    .filter(Webhook.status == "sent", Webhook.updated_at < cutoff_date)
                    .delete(synchronize_session=False)
                )

                logger.info(
                    f"Cleaned up {deleted_count} completed webhooks older than {days_old} days"
                )
                return deleted_count
        except Exception as e:
            logger.error(f"Error cleaning up completed webhooks: {e}")
            return 0

    def get_rate_limit_stats(self, base_url: str) -> Dict[str, Any]:
        """Get rate limiting stats for a URL."""
        window_start = datetime.now(timezone.utc) - timedelta(minutes=1)

        with self.session_scope() as session:
            stats = (
                session.query(
                    func.count(Webhook.id).label("total_requests"),
                    func.count(case((Webhook.status == "sent", 1))).label(
                        "successful_requests"
                    ),
                )
                .filter(
                    Webhook.url.startswith(base_url), Webhook.updated_at >= window_start
                )
                .first()
            )

            return {
                "total_requests": stats.total_requests or 0,
                "successful_requests": stats.successful_requests or 0,
                "failed_requests": (stats.total_requests or 0)
                - (stats.successful_requests or 0),
            }

    # Admin Operations Methods
    def schedule_admin_task(
        self,
        task_type: str,
        target_id: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Schedule an administrative task."""

        @retry_on_lock_error(max_retries=3, delay=0.1)
        def _schedule_admin_task_internal():
            try:
                with self.session_scope() as session:
                    admin_task = AdminTask(
                        task_type=task_type,
                        target_id=target_id,
                        status="pending",
                        parameters=parameters or {},
                        created_at=datetime.now(timezone.utc),
                    )
                    session.add(admin_task)
                    logger.info(
                        f"Scheduled admin task {task_type} for target {target_id}"
                    )
                    return True
            except Exception as e:
                logger.error(f"Error scheduling admin task: {e}")
                raise

        return _schedule_admin_task_internal()

    def get_admin_tasks_by_status(self, status: str) -> List[Dict[str, Any]]:
        """Get admin tasks by status."""
        with self.session_scope() as session:
            tasks = (
                session.query(AdminTask)
                .filter(AdminTask.status == status)
                .order_by(AdminTask.created_at)
                .all()
            )

            task_list = []
            for task in tasks:
                task_dict = {
                    "id": task.id,
                    "task_type": task.task_type,
                    "target_id": task.target_id,
                    "status": task.status,
                    "parameters": task.parameters,
                    "created_at": task.created_at,
                    "started_at": task.started_at,
                    "completed_at": task.completed_at,
                    "error_message": task.error_message,
                }
                task_list.append(task_dict)

            return task_list

    def cleanup_old_results(self, days_old: int = 30) -> int:
        """Clean up old workflow results."""
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)

        try:
            with self.session_scope() as session:
                # Count how many will be deleted
                count = (
                    session.query(WorkflowResult)
                    .filter(WorkflowResult.created_at < cutoff_date)
                    .count()
                )

                # Actually delete them
                deleted_count = (
                    session.query(WorkflowResult)
                    .filter(WorkflowResult.created_at < cutoff_date)
                    .delete(synchronize_session=False)
                )

                logger.info(
                    f"Cleaned up {deleted_count} old workflow results older than {days_old} days"
                )
                return deleted_count
        except Exception as e:
            logger.error(f"Error cleaning up old results: {e}")
            return 0

    def cleanup_old_workflows(self, status_list: List[str], days_old: int = 30) -> int:
        """Clean up old workflows with specific status."""
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)

        try:
            with self.session_scope() as session:
                # Count how many will be deleted
                count = (
                    session.query(Workflow)
                    .filter(
                        Workflow.status.in_(status_list),
                        Workflow.updated_at < cutoff_date,
                    )
                    .count()
                )

                # Actually delete them
                deleted_count = (
                    session.query(Workflow)
                    .filter(
                        Workflow.status.in_(status_list),
                        Workflow.updated_at < cutoff_date,
                    )
                    .delete(synchronize_session=False)
                )

                logger.info(
                    f"Cleaned up {deleted_count} old workflows with status {status_list} older than {days_old} days"
                )
                return deleted_count
        except Exception as e:
            logger.error(f"Error cleaning up old workflows: {e}")
            return 0

    def cancel_workflow_tasks(self, workflow_id: str) -> bool:
        """Cancel all tasks in a workflow."""

        @retry_on_lock_error(max_retries=3, delay=0.1)
        def _cancel_workflow_tasks_internal():
            try:
                with self.session_scope() as session:
                    # Update all tasks in the workflow that are not yet completed
                    updated_count = (
                        session.query(Task)
                        .filter(
                            Task.workflow_id == workflow_id,
                            Task.status.in_(["pending", "executing"]),
                        )
                        .update(
                            {
                                Task.status: "cancelled",
                                Task.updated_at: datetime.now(timezone.utc),
                            },
                            synchronize_session=False,
                        )
                    )

                    logger.info(
                        f"Cancelled {updated_count} tasks in workflow {workflow_id}"
                    )
                    return updated_count > 0
            except Exception as e:
                logger.error(f"Error cancelling workflow tasks for {workflow_id}: {e}")
                raise

        return _cancel_workflow_tasks_internal()

    def get_workflow_statistics(self) -> Dict[str, Any]:
        """Get overall workflow statistics."""
        with self.session_scope() as session:
            stats = session.query(
                func.count(Workflow.workflow_id).label("total_workflows"),
                func.count(case((Workflow.status == "completed", 1))).label(
                    "completed_workflows"
                ),
                func.count(case((Workflow.status == "failed", 1))).label(
                    "failed_workflows"
                ),
                func.count(case((Workflow.status == "running", 1))).label(
                    "running_workflows"
                ),
            ).first()

            return {
                "total_workflows": stats.total_workflows or 0,
                "completed_workflows": stats.completed_workflows or 0,
                "failed_workflows": stats.failed_workflows or 0,
                "running_workflows": stats.running_workflows or 0,
            }

    def get_task_statistics(self) -> Dict[str, Any]:
        """Get task execution statistics."""
        with self.session_scope() as session:
            stats = session.query(
                func.count(Task.task_id).label("total_tasks"),
                func.count(case((Task.status == "completed", 1))).label(
                    "completed_tasks"
                ),
                func.count(case((Task.status == "failed", 1))).label("failed_tasks"),
                func.count(case((Task.status == "pending", 1))).label("pending_tasks"),
                func.count(case((Task.status == "executing", 1))).label(
                    "executing_tasks"
                ),
            ).first()

            return {
                "total_tasks": stats.total_tasks or 0,
                "completed_tasks": stats.completed_tasks or 0,
                "failed_tasks": stats.failed_tasks or 0,
                "pending_tasks": stats.pending_tasks or 0,
                "executing_tasks": stats.executing_tasks or 0,
            }

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
def get_db_manager(
    db_path: Optional[str] = None, engine_url: Optional[str] = None
) -> DatabaseManager:
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
