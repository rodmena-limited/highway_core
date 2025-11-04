**Objective:**
Your task is to refactor the `highway_core` library from a simple, in-memory DAG runner into a durable, multi-process, database-driven workflow engine. This new architecture must support long-running workflows, durable timers (`wait`), and the ability to pause and resume workflows via external events (e.g., for human-in-the-loop tasks).

This will be accomplished by moving the "brain" of the engine from the `Orchestrator`'s in-memory `TopologicalSorter` to the PostgreSQL database (which we assume is already configured).

The engine will be split into three independent components:

1.  **A Flask API:** To submit new durable workflows and resume waiting tasks.
2.  **A Scheduler Service:** A background poller that finds and enqueues ready tasks.
3.  **A Worker Service:** A background process that executes tasks from a database-backed queue.

**Critical Constraint (Backward Compatibility):**
You must maintain backward compatibility. The existing `run_workflow_from_yaml` function (used by `cli.py run` and `factcheck.sh`) must continue to function exactly as it does. We will call this **"Local Mode"**. The new durable system will be **"Durable Mode"**.

To achieve this, you will implement a "hybrid mode" architecture:

  * **Local Mode (Existing):** `Orchestrator` runs in-memory, `wait` tasks block using `time.sleep()`, and no durable state is used. This is for simple scripts and all existing tests.
  * **Durable Mode (New):** The Flask API creates workflow records in the DB. The Scheduler and Worker services (running separately) drive the execution. `wait` tasks are non-blocking and update the database state.

Follow these steps precisely.

-----

### **Phase 1: Evolve the Database (The New "Brain")**

The database will become the single source of truth for the durable workflow's state.

**File: `highway_core/persistence/models.py`**

1.  **Modify `Workflow` Model:**

      * Add a `mode` column to differentiate "local" runs from "durable" runs.
      * Add more granular `status` values (`PENDING` for new durable workflows).
      * Add an index for the Scheduler to poll.
      * *Replace* the existing `status = Column(String(50), default="running")` line with:
        ```python
        status = Column(String(50), default="running", nullable=False)  # running, completed, failed, PENDING, PAUSED
        mode = Column(String(50), default="LOCAL", nullable=False)  # LOCAL, DURABLE
        ```
      * *Add* this `__table_args__` definition inside the `Workflow` class body:
        ```python
        __table_args__ = (
            Index("idx_workflows_poll", "status", "mode"),
            Index("idx_workflows_name", "workflow_name"),
        )
        ```

2.  **Modify `Task` Model:**

      * This is the most important change. The `status` column becomes the state machine.
      * Add new columns for durable waiting: `wake_up_at` (for timers) and `event_token` (for human tasks).
      * Add new indexes for polling.
      * *Replace* the existing `status = Column(String(50), default="pending")` line with:
        ```python
        status = Column(String(50), default="pending", nullable=False)  # pending, QUEUED, running, completed, failed, WAITING_FOR_TIMER, WAITING_FOR_EVENT
        ```
      * *Add* these new columns inside the `Task` class, after `result_type`:
        ```python
        wake_up_at = Column(DateTime(timezone=True), nullable=True)
        event_token = Column(String(255), nullable=True, unique=True)
        ```
      * *Replace* the existing `__table_args__` with this, adding the new poll/token indexes:
        ```python
        __table_args__ = (
            # Unique constraint on the composite key (task_id, workflow_id)
            UniqueConstraint("task_id", "workflow_id", name="uix_task_workflow_pair"),
            # Additional indexes for performance
            Index("idx_tasks_workflow_id", "workflow_id"),
            Index("idx_tasks_status", "status"),
            Index("idx_tasks_workflow_status", "workflow_id", "status"),
            
            # --- NEW INDEXES FOR DURABLE ENGINE ---
            # This is the most important index for the new scheduler
            Index("idx_tasks_poll_runnable", "status", "wake_up_at"),
            Index("idx_tasks_event_token", "event_token"),
        )
        ```

3.  **Add `TaskQueue` Model:**

      * This new table will act as the "to-do" list between the Scheduler and Workers.
      * *Add* this new class at the end of `highway_core/persistence/models.py`:
        ```python
        class TaskQueue(Base):  # type: ignore
            """
            A simple, durable task queue in the database.
            The Scheduler adds jobs here. Workers pull jobs from here.
            """
            __tablename__ = "task_queue"

            id = Column(Integer, primary_key=True, autoincrement=True)
            workflow_id = Column(String, ForeignKey("workflows.workflow_id", ondelete="CASCADE"), nullable=False)
            task_id = Column(String, nullable=False)
            status = Column(String(50), default="QUEUED", nullable=False) # QUEUED, RUNNING
            created_at = Column(DateTime, default=utc_now)

            __table_args__ = (
                Index("idx_queue_poll", "status", "created_at"),
            )
        ```

-----

### **Phase 2: Add New `DatabaseManager` Methods**

We need new functions in `database.py` to interact with our new durable models.

**File: `highway_core/persistence/database.py`**

  * Add the following new methods *inside* the `DatabaseManager` class.

    ```python
    def create_durable_workflow(
        self,
        workflow_id: str,
        workflow_name: str,
        start_task_id: str,
        variables: Dict[str, Any],
        tasks: List[Dict[str, Any]], # Use list of dicts for persistence
    ) -> str:
        """
        Creates a new workflow and all its tasks in the DB for the durable engine.
        Returns the new workflow_id.
        """
        with self.session_scope() as session:
            # 1. Create the Workflow
            workflow = Workflow(
                workflow_id=workflow_id,
                workflow_name=workflow_name,
                start_task=start_task_id,
                variables=variables,
                status="PENDING", # Will be started by the Scheduler
                mode="DURABLE",
            )
            session.add(workflow)
            
            # 2. Create all Task objects
            for task_data in tasks:
                dependencies = task_data.get("dependencies", [])
                task = Task(
                    task_id=task_data["task_id"],
                    workflow_id=workflow_id,
                    operator_type=task_data["operator_type"],
                    runtime=task_data.get("runtime", "python"),
                    function=task_data.get("function"),
                    image=task_data.get("image"),
                    command=task_data.get("command"),
                    args=task_data.get("args", []),
                    kwargs=task_data.get("kwargs", {}),
                    result_key=task_data.get("result_key"),
                    dependencies_list=dependencies,
                    status="PENDING", # All tasks start as PENDING
                )
                session.add(task)
                
                # 3. Store dependencies
                if dependencies:
                    for dep_task_id in dependencies:
                        dependency = TaskDependency(
                            task_id=task_data["task_id"],
                            depends_on_task_id=dep_task_id,
                            workflow_id=workflow_id,
                        )
                        session.add(dependency)
        
        logger.info(f"Created DURABLE workflow: {workflow_name} ({workflow_id})")
        return workflow_id

    def get_workflows_by_status(self, status: str, mode: str = "DURABLE") -> List[Workflow]:
        """Finds workflows with a specific status and mode."""
        with self.session_scope() as session:
            return session.query(Workflow).filter(
                Workflow.status == status,
                Workflow.mode == mode
            ).all()

    def get_task_by_id(self, workflow_id: str, task_id: str) -> Optional[Task]:
        """Gets a single task object by its ID."""
        with self.session_scope() as session:
            return session.query(Task).filter(
                Task.workflow_id == workflow_id,
                Task.task_id == task_id
            ).first()

    def get_tasks_by_status(self, workflow_id: str, status: str) -> List[Task]:
        """Finds tasks in a workflow with a specific status."""
        with self.session_scope() as session:
            return session.query(Task).filter(
                Task.workflow_id == workflow_id,
                Task.status == status
            ).all()

    def get_task_dependencies(self, workflow_id: str, task_id: str) -> List[str]:
        """Gets the list of dependency task_ids for a given task."""
        with self.session_scope() as session:
            deps = session.query(TaskDependency.depends_on_task_id).filter(
                TaskDependency.workflow_id == workflow_id,
                TaskDependency.task_id == task_id
            ).all()
            return [dep[0] for dep in deps]

    def are_task_dependencies_met(self, workflow_id: str, task_id: str) -> bool:
        """Checks if all dependencies for a task are 'completed'."""
        dependency_ids = self.get_task_dependencies(workflow_id, task_id)
        if not dependency_ids:
            return True # No dependencies, it's runnable
            
        with self.session_scope() as session:
            completed_deps_count = session.query(func.count(Task.task_id)).filter(
                Task.workflow_id == workflow_id,
                Task.task_id.in_(dependency_ids),
                Task.status == "completed"
            ).scalar()
            return completed_deps_count == len(dependency_ids)

    def find_tasks_to_wake(self) -> List[Task]:
        """Finds tasks whose timer has expired."""
        with self.session_scope() as session:
            now = datetime.now(timezone.utc)
            return session.query(Task).filter(
                Task.status == "WAITING_FOR_TIMER",
                Task.wake_up_at <= now
            ).all()

    def update_task_status(
        self,
        task_id: str,
        status: str,
        started_at: Optional[datetime] = None,
        error_message: Optional[str] = None,
    ) -> bool:
        """Update task status (This method already exists, we are enhancing it)."""
        @retry_on_lock_error(max_retries=3, delay=0.1)
        def _update_task_status_internal():
            try:
                with self.session_scope() as session:
                    # Note: task_id is only unique in combination with workflow_id
                    # This function might be ambiguous. Let's assume for now it's okay
                    # if task_id is globally unique in practice, or we enhance it.
                    # A safer way:
                    # task = session.query(Task).filter(Task.task_id == task_id).first()
                    
                    # Let's use the provided logic, but be aware of potential ambiguity.
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
                    logger.warning(f"Task {task_id} not found for status update")
                    return False
            except Exception as e:
                logger.error(f"Error updating task {task_id} status: {e}")
                raise
        return _update_task_status_internal()
        
    def update_task_status_by_workflow(self, workflow_id: str, task_id: str, status: str) -> bool:
        """Updates the status of a single task, scoped to a workflow."""
        @retry_on_lock_error(max_retries=3, delay=0.1)
        def _update_task_status_internal():
            try:
                with self.session_scope() as session:
                    task = session.query(Task).filter(
                        Task.workflow_id == workflow_id,
                        Task.task_id == task_id
                    ).with_for_update().first()
                    
                    if task:
                        task.status = status
                        task.updated_at = datetime.now(timezone.utc)
                        logger.debug(f"Updated task {task_id} in {workflow_id} to {status}")
                        return True
                    return False
            except Exception as e:
                logger.error(f"Error updating task {task_id} in {workflow_id} status: {e}")
                raise
        return _update_task_status_internal()

    def update_task_status_and_wakeup(self, workflow_id: str, task_id: str, status: str, wake_up_at: datetime):
        """Updates task status and sets its wake_up_at timer."""
        @retry_on_lock_error(max_retries=3, delay=0.1)
        def _update_internal():
            with self.session_scope() as session:
                task = session.query(Task).filter(
                    Task.workflow_id == workflow_id,
                    Task.task_id == task_id
                ).with_for_update().first()
                if task:
                    task.status = status
                    task.wake_up_at = wake_up_at
                    task.updated_at = datetime.now(timezone.utc)
        _update_internal()
                
    def update_task_status_and_token(self, workflow_id: str, task_id: str, status: str, token: str):
        """Updates task status and sets its event_token."""
        @retry_on_lock_error(max_retries=3, delay=0.1)
        def _update_internal():
            with self.session_scope() as session:
                task = session.query(Task).filter(
                    Task.workflow_id == workflow_id,
                    Task.task_id == task_id
                ).with_for_update().first()
                if task:
                    task.status = status
                    task.event_token = token
                    task.updated_at = datetime.now(timezone.utc)
        _update_internal()

    def get_task_by_token(self, token: str) -> Optional[Task]:
        """Finds a task that is waiting for an event with a specific token."""
        with self.session_scope() as session:
            return session.query(Task).filter(
                Task.event_token == token
            ).first()

    def enqueue_task(self, workflow_id: str, task_id: str) -> bool:
        """Adds a task to the durable TaskQueue for a worker to pick up."""
        from .models import TaskQueue
        try:
            with self.session_scope() as session:
                # Check if already in queue
                exists = session.query(TaskQueue).filter(
                    TaskQueue.workflow_id == workflow_id,
                    TaskQueue.task_id == task_id,
                    TaskQueue.status == "QUEUED"
                ).first()
                
                if exists:
                    logger.warning(f"Task {task_id} is already in the queue.")
                    return False
                    
                queue_item = TaskQueue(
                    workflow_id=workflow_id,
                    task_id=task_id,
                    status="QUEUED"
                )
                session.add(queue_item)
            
            self.update_task_status_by_workflow(workflow_id, task_id, "QUEUED")
            logger.info(f"Enqueued task {task_id} for workflow {workflow_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to enqueue task {task_id}: {e}")
            return False
            
    def get_next_queued_task_atomic(self) -> Optional[TaskQueue]:
        """
        Atomically fetches the oldest 'QUEUED' job, sets it to 'RUNNING',
        and returns it. This uses FOR UPDATE SKIP LOCKED.
        """
        from .models import TaskQueue
        try:
            with self.session_scope() as session:
                # This SQL is dialect-specific but works on PostgreSQL
                if self.engine.dialect.name == 'postgresql':
                    job = session.query(TaskQueue).filter(
                        TaskQueue.status == "QUEUED"
                    ).order_by(
                        TaskQueue.created_at
                    ).with_for_update(
                        skip_locked=True
                    ).first()
                else:
                    # Fallback for SQLite (which will lock the table)
                    job = session.query(TaskQueue).filter(
                        TaskQueue.status == "QUEUED"
                    ).order_by(
                        TaskQueue.created_at
                    ).with_for_update().first()

                
                if job:
                    job.status = "RUNNING"
                    return job
                return None
        except Exception as e:
            logger.error(f"Error getting next queued task: {e}")
            return None

    def delete_queue_job(self, job_id: int):
        """Deletes a job from the TaskQueue by its ID."""
        from .models import TaskQueue
        try:
            with self.session_scope() as session:
                job = session.query(TaskQueue).filter(TaskQueue.id == job_id).first()
                if job:
                    session.delete(job)
        except Exception as e:
            logger.error(f"Error deleting queue job {job_id}: {e}")
    ```

-----

### **Phase 3: Create The New Durable Services (Scheduler & Worker)**

1.  **Create new directory: `highway_core/services/`**

2.  **Create file: `highway_core/services/__init__.py`** (leave it empty)

3.  **Create file: `highway_core/services/scheduler.py`**

      * This is the new "brain" poller.

    <!-- end list -->

    ```python
    import logging
    import time
    from datetime import datetime, timezone

    from highway_core.persistence.database import get_db_manager

    logger = logging.getLogger("HighwayScheduler")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    class Scheduler:
        def __init__(self):
            self.db_manager = get_db_manager()
            self.poll_interval = 5  # Poll every 5 seconds

        def run(self):
            logger.info("Highway Scheduler started...")
            try:
                while True:
                    self.poll_for_ready_workflows()
                    self.poll_for_runnable_tasks()
                    self.poll_for_waiting_tasks()
                    self.poll_for_completed_workflows()
                    
                    time.sleep(self.poll_interval)
            except KeyboardInterrupt:
                logger.info("Highway Scheduler shutting down...")
            finally:
                self.db_manager.close_all_connections()

        def poll_for_ready_workflows(self):
            """Finds 'PENDING' workflows and starts them."""
            try:
                workflows = self.db_manager.get_workflows_by_status("PENDING", mode="DURABLE")
                for wf in workflows:
                    logger.info(f"Starting new durable workflow {wf.workflow_id}")
                    self.db_manager.update_workflow_status(wf.workflow_id, "RUNNING")
                    # Enqueue the start task
                    self.db_manager.enqueue_task(wf.workflow_id, wf.start_task)
            except Exception as e:
                logger.error(f"Scheduler error in poll_for_ready_workflows: {e}", exc_info=True)

        def poll_for_runnable_tasks(self):
            """Finds 'PENDING' tasks whose dependencies are met."""
            try:
                workflows = self.db_manager.get_workflows_by_status("RUNNING", mode="DURABLE")
                for wf in workflows:
                    pending_tasks = self.db_manager.get_tasks_by_status(wf.workflow_id, "PENDING")
                    for task in pending_tasks:
                        if self.db_manager.are_task_dependencies_met(wf.workflow_id, task.task_id):
                            logger.info(f"Dependencies met for task {task.task_id}. Enqueuing.")
                            self.db_manager.enqueue_task(wf.workflow_id, task.task_id)
            except Exception as e:
                logger.error(f"Scheduler error in poll_for_runnable_tasks: {e}", exc_info=True)

        def poll_for_waiting_tasks(self):
            """Finds 'WAITING_FOR_TIMER' tasks whose timer is up."""
            try:
                tasks_to_wake = self.db_manager.find_tasks_to_wake()
                for task in tasks_to_wake:
                    logger.info(f"Waking up task {task.task_id} from timer.")
                    # Set status to PENDING, not QUEUED. This lets the
                    # poll_for_runnable_tasks check its dependencies first
                    # (in case a task depends on both a timer and another task).
                    # For a simple timer, it will be enqueued on the *next* cycle.
                    self.db_manager.update_task_status_by_workflow(task.workflow_id, task.task_id, "PENDING")
            except Exception as e:
                logger.error(f"Scheduler error in poll_for_waiting_tasks: {e}", exc_info=True)

        def poll_for_completed_workflows(self):
            """Checks if any running workflows are now complete."""
            try:
                workflows = self.db_manager.get_workflows_by_status("RUNNING", mode="DURABLE")
                for wf in workflows:
                    active_tasks = 0
                    statuses_to_check = ["PENDING", "QUEUED", "RUNNING", "WAITING_FOR_TIMER", "WAITING_FOR_EVENT"]
                    for status in statuses_to_check:
                        active_tasks += len(self.db_manager.get_tasks_by_status(wf.workflow_id, status))
                        if active_tasks > 0:
                            break # No need to check other statuses
                    
                    if active_tasks == 0:
                        logger.info(f"Workflow {wf.workflow_id} has no active tasks. Marking as COMPLETED.")
                        self.db_manager.update_workflow_status(wf.workflow_id, "completed")
            except Exception as e:
                logger.error(f"Scheduler error in poll_for_completed_workflows: {e}", exc_info=True)

    if __name__ == "__main__":
        s = Scheduler()
        s.run()
    ```

4.  **Create file: `highway_core/services/worker.py`**

      * This is the new task executor.

    <!-- end list -->

    ```python
    import logging
    import time
    import json
    from typing import Dict, Any

    from highway_core.persistence.database import get_db_manager, TaskQueue, Task
    from highway_core.engine.orchestrator import Orchestrator
    from highway_core.engine.models import WorkflowModel, AnyOperatorModel
    from highway_core.persistence.sql_persistence_manager import SQLPersistenceManager
    from highway_core.tools.registry import ToolRegistry

    logger = logging.getLogger("HighwayWorker")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    def model_to_dict(model: Task) -> Dict[str, Any]:
        """Converts an SQLAlchemy model instance to a dict, handling JSON fields."""
        task_dict = {c.name: getattr(model, c.name) for c in model.__table__.columns}
        
        # Manually deserialize JSON fields for Pydantic
        task_dict['args'] = model.args
        task_dict['kwargs'] = model.kwargs
        task_dict['command'] = model.command
        task_dict['dependencies'] = model.dependencies_list
        return task_dict

    class Worker:
        def __init__(self):
            self.db_manager = get_db_manager()
            self.poll_interval = 1  # Poll every 1 second
            self.tool_registry = ToolRegistry()

        def run(self):
            logger.info("Highway Worker started...")
            try:
                while True:
                    job = self.db_manager.get_next_queued_task_atomic()
                    if job:
                        logger.info(f"Picked up job {job.id} for task {job.task_id}")
                        self.execute_task_from_job(job)
                    else:
                        time.sleep(self.poll_interval)
            except KeyboardInterrupt:
                logger.info("Highway Worker shutting down...")
            finally:
                self.db_manager.close_all_connections()

        def execute_task_from_job(self, job: TaskQueue):
            task_db = None
            orchestrator_context = None
            try:
                # 1. Get task and workflow models
                workflow_db = self.db_manager.load_workflow(job.workflow_id)
                if not workflow_db:
                    raise Exception(f"Workflow {job.workflow_id} not found.")
                
                task_db = self.db_manager.get_task_by_id(job.workflow_id, job.task_id)
                if not task_db:
                    raise Exception(f"Task {job.task_id} not found.")
                
                # 2. Update status to RUNNING
                self.db_manager.update_task_status_by_workflow(job.workflow_id, job.task_id, "running")
                
                # 3. Create a 'lite' Orchestrator context
                workflow_model = WorkflowModel(
                    name=workflow_db['name'],
                    start_task=workflow_db['start_task'],
                    variables=workflow_db['variables'],
                    tasks={} # Not needed for single task execution
                )
                # Set the mode on the pydantic model for handlers to read
                setattr(workflow_model, 'mode', workflow_db.get('mode', 'LOCAL')) 
                
                persistence = SQLPersistenceManager(engine_url=self.db_manager.engine_url)
                
                orchestrator_context = Orchestrator(
                    workflow_run_id=job.workflow_id,
                    workflow=workflow_model,
                    persistence_manager=persistence,
                    registry=self.tool_registry
                )
                
                # Load the current state for the handler
                orchestrator_context.state, _ = persistence.load_workflow_state(job.workflow_id)
                
                # 4. Get the handler and executor
                task_dict = model_to_dict(task_db)
                task_model: AnyOperatorModel = AnyOperatorModel.model_validate(task_dict)
                
                handler_func = orchestrator_context.handler_map.get(task_model.operator_type)
                if not handler_func:
                    raise ValueError(f"No handler for operator type: {task_model.operator_type}")

                selected_executor = None
                if hasattr(task_model, 'runtime'):
                    selected_executor = orchestrator_context.executors.get(task_model.runtime)
                    if not selected_executor:
                        raise ValueError(f"No executor registered for runtime: {task_model.runtime}")

                # 5. Execute the task handler
                handler_func(
                    task=task_model,
                    state=orchestrator_context.state,
                    orchestrator=orchestrator_context,
                    registry=orchestrator_context.registry,
                    bulkhead_manager=orchestrator_context.bulkhead_manager,
                    executor=selected_executor,
                    resource_manager=orchestrator_context.resource_manager,
                    workflow_run_id=orchestrator_context.run_id,
                )
                
                # 6. Mark as COMPLETED (if not durably paused)
                task_final_status = self.db_manager.get_task_by_id(job.workflow_id, job.task_id).status
                if task_final_status == "running":
                    logger.info(f"Task {job.task_id} handler finished. Marking COMPLETED.")
                    # We need to get the result from the state
                    result_to_save = None
                    if task_model.result_key:
                        result_to_save = orchestrator_context.state.get_result(task_model.result_key)
                    self.db_manager.complete_task(job.workflow_id, job.task_id, result_to_save)
                else:
                    logger.info(f"Task {job.task_id} handler finished with durable status: {task_final_status}")

            except Exception as e:
                logger.error(f"Error executing task {job.task_id}: {e}", exc_info=True)
                if task_db:
                    self.db_manager.fail_task(job.workflow_id, job.task_id, str(e))
            finally:
                if orchestrator_context:
                    orchestrator_context.close()
                self.db_manager.delete_queue_job(job.id)
                logger.info(f"Finished job {job.id} for task {job.task_id}")

    if __name__ == "__main__":
        w = Worker()
        w.run()
    ```

-----

### **Phase 4: Implement "Hybrid Mode" Handlers & Context**

Handlers must now support both `LOCAL` (blocking) and `DURABLE` (non-blocking) modes.

1.  **File: `highway_core/engine/orchestrator.py`**

      * In the `Orchestrator.__init__`, tag all workflows run by it as `LOCAL`.
      * *Add this line* inside `Orchestrator.__init__`, after `self.persistence = persistence_manager`:
        ```python
        # Set the mode on the workflow model for handlers to access
        # The 'LOCAL' mode is assumed for any workflow run via the old Orchestrator.run()
        setattr(self.workflow, 'mode', 'LOCAL')
        ```

2.  **File: `highway_core/engine/executors/base.py`**

      * The `execute` signature must be updated to pass the `Orchestrator` context and `workflow_run_id` to the `LocalPythonExecutor` for context injection.
      * *Replace* the `execute` method signature with:
        ```python
        def execute(
            self,
            task: "TaskOperatorModel",
            state: "WorkflowState",
            registry: "ToolRegistry",
            bulkhead_manager: Optional["BulkheadManager"],
            resource_manager: Optional["ContainerResourceManager"],
            orchestrator: Optional["Orchestrator"],  # <-- ADD THIS
            workflow_run_id: Optional[str],
        ) -> Any:
        ```

3.  **File: `highway_core/engine/executors/docker.py`**

      * Update the `execute` signature to match the base class.
      * *Replace* the `execute` method signature with:
        ```python
        def execute(
            self,
            task: "TaskOperatorModel",
            state: "WorkflowState",
            registry: "ToolRegistry",  # Ignored
            bulkhead_manager: Optional["BulkheadManager"],  # Ignored
            resource_manager: Optional["ContainerResourceManager"],
            orchestrator: Optional["Orchestrator"],  # <-- ADD THIS
            workflow_run_id: Optional[str],
        ) -> Any:
        ```

4.  **File: `highway_core/engine/operator_handlers/task_handler.py`**

      * Update the `execute` signature and pass the new arguments to `executor.execute()`.
      * *Replace* the `execute` method signature with:
        ```python
        def execute(
            task: TaskOperatorModel,
            state: WorkflowState,
            orchestrator: Optional["Orchestrator"],
            registry: ToolRegistry,  # <-- Keep this required
            bulkhead_manager: Optional[BulkheadManager] = None,
            executor: Optional["BaseExecutor"] = None,  # <-- NEW
            resource_manager: Optional["ContainerResourceManager"] = None,
            workflow_run_id: Optional[str] = None, # <-- ADD THIS
        ) -> List[str]:
        ```
      * *Inside `execute`*, find the `executor.execute` call and *replace* it with:
        ```python
        result = executor.execute(
            task=task,
            state=state,
            registry=registry,
            bulkhead_manager=bulkhead_manager,
            resource_manager=resource_manager,
            orchestrator=orchestrator,  # <-- PASS THIS
            workflow_run_id=workflow_run_id,  # <-- PASS THIS
        )
        ```

5.  **File: `highway_core/engine/executors/local_python.py`**

      * This is where we inject context for the new `human` tool.
      * *Replace* the `execute` method signature with:
        ```python
        def execute(
            self,
            task: "TaskOperatorModel",
            state: "WorkflowState",
            registry: "ToolRegistry",
            bulkhead_manager: Optional["BulkheadManager"],
            resource_manager: Optional["ContainerResourceManager"],
            orchestrator: Optional["Orchestrator"],  # <-- ADD THIS
            workflow_run_id: Optional[str],  # <-- ADD THIS
        ) -> Any:
        ```
      * *In `execute`*, change the call to `_execute_locally` to pass the new args:
        ```python
        return self._execute_locally(task, state, registry, bulkhead_manager, orchestrator, workflow_run_id)
        ```
      * *Replace* the `_execute_locally` method signature with:
        ```python
        def _execute_locally(
            self,
            task: "TaskOperatorModel",
            state: "WorkflowState",
            registry: "ToolRegistry",
            bulkhead_manager: Optional["BulkheadManager"],
            orchestrator: Optional["Orchestrator"],  # <-- ADD THIS
            workflow_run_id: Optional[str],  # <-- ADD THIS
        ) -> Any:
        ```
      * *Inside `_execute_locally`*, find the "Special check for tools that need state" and *replace* that logic block with:
        ```python
        # 3. Special context injection
        if tool_name.startswith("tools.memory."):
            # Inject the state object as the first argument
            resolved_args.insert(0, state)
        elif tool_name.startswith("human."):
            # Inject all context objects needed for durable pausing
            # (state, orchestrator, task_id, workflow_run_id)
            resolved_args.insert(0, workflow_run_id)
            resolved_args.insert(0, task.task_id)
            resolved_args.insert(0, orchestrator)
            resolved_args.insert(0, state)
        ```

6.  **File: `highway_core/engine/operator_handlers/wait_handler.py`**

      * This handler must become non-blocking for "Durable Mode".
      * *Replace* the *entire* `execute` function with this new hybrid logic:
        ```python
        import re
        import time
        from datetime import datetime, timezone, timedelta

        # ... (other imports)

        def execute(
            task: WaitOperatorModel,
            state: WorkflowState,
            orchestrator: "Orchestrator",
            registry: Optional["ToolRegistry"],
            bulkhead_manager: Optional["BulkheadManager"],
            executor: Optional["BaseExecutor"] = None,
            resource_manager=None,
            workflow_run_id: str = "",
        ) -> List[str]:
            """
            Executes a WaitOperator.
            - In 'LOCAL' mode, it blocks with time.sleep().
            - In 'DURABLE' mode, it sets the task status to WAITING_FOR_TIMER and returns.
            """
            wait_for = state.resolve_templating(task.wait_for)
            
            # Determine the workflow mode from the orchestrator's context
            workflow_mode = getattr(orchestrator.workflow, 'mode', 'LOCAL')
            
            logger.info(f"WaitHandler: Waiting for {wait_for} in {workflow_mode} mode.")
            
            # --- Calculate wake_up_time ---
            wake_up_time = None
            duration_sec = 0
            
            if isinstance(wait_for, (int, float)):
                duration_sec = wait_for
            elif isinstance(wait_for, str):
                if wait_for.startswith("duration:"):
                    duration_str = wait_for[9:]
                    match = re.match(r"(\d+\.?\d*)([smh])", duration_str)
                    if match:
                        value, unit = match.groups()
                        value = float(value)
                        if unit == "s": duration_sec = value
                        elif unit == "m": duration_sec = value * 60
                        elif unit == "h": duration_sec = value * 3600
                    else:
                        try: duration_sec = float(duration_str)
                        except ValueError: pass
                elif wait_for.startswith("time:"):
                    time_str = wait_for[5:]
                    target_time = datetime.strptime(time_str, "%H:%M:%S").time()
                    now = datetime.now(timezone.utc)
                    # Ensure the target_datetime is timezone-aware
                    target_datetime = datetime.combine(now.date(), target_time, tzinfo=timezone.utc)
                    if target_datetime < now:
                        target_datetime += timedelta(days=1)
                    wake_up_time = target_datetime
                    duration_sec = (target_datetime - now).total_seconds()

            if wake_up_time is None:
                wake_up_time = datetime.now(timezone.utc) + timedelta(seconds=duration_sec)
            # ------------------------------

            if workflow_mode == "DURABLE":
                # --- DURABLE MODE ---
                # Set task status to WAITING and let the Scheduler wake it up.
                db = orchestrator.persistence.db_manager
                db.update_task_status_and_wakeup(
                    workflow_run_id, task.task_id, "WAITING_FOR_TIMER", wake_up_time
                )
                logger.info(f"Task {task.task_id} set to WAITING_FOR_TIMER until {wake_up_time}.")
            
            else:
                # --- LOCAL MODE ---
                # Block the thread. This is the original behavior.
                if duration_sec > 0:
                    logger.info(f"Task {task.task_id} sleeping for {duration_sec}s...")
                    time.sleep(duration_sec)
                    logger.info("WaitHandler: Wait complete.")

            return [] # This handler never adds new tasks
        ```

7.  **File: `highway_core/engine/operator_handlers/condition_handler.py`**

      * This handler must be updated to skip branches in a durable-safe way.
      * *Replace* the `execute` function with this:
        ```python
        def execute(
            task: ConditionOperatorModel,
            state: WorkflowState,
            orchestrator: "Orchestrator",
            registry: Optional["ToolRegistry"],
            bulkhead_manager: Optional["BulkheadManager"],
            executor: Optional["BaseExecutor"] = None,
            resource_manager=None,
            workflow_run_id: str = "",
        ) -> None:
            """
            Evaluates a ConditionOperator.
            - In 'LOCAL' mode, updates the in-memory sorter.
            - In 'DURABLE' mode, updates the skipped task's status in the DB.
            """
            logger.info("ConditionHandler: Evaluating '%s'", task.condition)
            
            workflow_mode = getattr(orchestrator.workflow, 'mode', 'LOCAL')

            # 1. Resolve and evaluate the condition
            resolved_condition_value = state.resolve_templating(task.condition)
            resolved_condition_str = str(resolved_condition_value)
            result = eval_condition(resolved_condition_str)
            logger.info(
                "ConditionHandler: Resolved to '%s'. Result: %s",
                resolved_condition_str,
                result,
            )

            # 2. Determine which task to skip
            skipped_task_id: Optional[str] = None
            if result:
                skipped_task_id = task.if_false
                logger.info("ConditionHandler: Taking 'if_true' path to '%s'", task.if_true)
            else:
                skipped_task_id = task.if_true
                logger.info("ConditionHandler: Taking 'if_false' path to '%s'", task.if_false)

            # 3. Mark the skipped branch as completed
            if skipped_task_id:
                logger.info(
                    "ConditionHandler: Marking '%s' as conceptually completed.",
                    skipped_task_id,
                )
                
                if workflow_mode == "DURABLE":
                    # --- DURABLE MODE ---
                    # Set status to COMPLETED in the DB. This unblocks
                    # any dependent tasks for the Scheduler.
                    db = orchestrator.persistence.db_manager
                    db.update_task_status_by_workflow(workflow_run_id, skipped_task_id, "completed")
                else:
                    # --- LOCAL MODE ---
                    # Mark as done in the in-memory sorter.
                    orchestrator.sorter.done(skipped_task_id)
                    orchestrator.completed_tasks.add(skipped_task_id)
        ```

-----

### **Phase 5: Implement New "Human" Tools**

1.  **File: `highway_core/tools/human.py`**

      * Create this new file. This is the "durable pause" tool.

    <!-- end list -->

    ```python
    import logging
    import uuid
    from typing import TYPE_CHECKING, List, Optional

    from .decorators import tool
    from highway_core.engine.state import WorkflowState

    if TYPE_CHECKING:
        from highway_core.engine.orchestrator import Orchestrator

    logger = logging.getLogger(__name__)

    @tool("human.request_approval")
    def request_approval(
        state: WorkflowState,
        orchestrator: "Orchestrator", # Injected by the worker
        task_id: str, # Injected by the worker
        workflow_run_id: str, # Injected by the worker
        approval_prompt: str,
        manager_email: str
    ) -> None:
        """
        Durable Human Task: Pauses the workflow and waits for an external event.
        """
        
        workflow_mode = getattr(orchestrator.workflow, 'mode', 'LOCAL')
        
        if workflow_mode != "DURABLE":
            logger.error(f"Task {task_id} failed: 'human.request_approval' is only supported in DURABLE mode.")
            raise Exception("Human tasks are only supported in DURABLE mode.")
            
        # --- DURABLE MODE ---
        db = orchestrator.persistence.db_manager
        
        # 1. Generate a unique token for this human task
        token = str(uuid.uuid4())
        
        # 2. Pause the task, setting its status and token
        db.update_task_status_and_token(workflow_run_id, task_id, "WAITING_FOR_EVENT", token)
        
        # 3. Log the approval info (in a real system, you'd email it)
        approval_url = f"http://127.0.0.1:5000/workflow/resume?token={token}&decision=APPROVED"
        denial_url = f"http://127.0.0.1:5000/workflow/resume?token={token}&decision=DENIED"
        
        logger.info(f"--- HUMAN TASK ({task_id}) ---")
        logger.info(f"Prompt: {approval_prompt}")
        logger.info(f"To: {manager_email}")
        logger.info(f"APPROVE: {approval_url}")
        logger.info(f"DENY:    {denial_url}")
        logger.info(f"Workflow {workflow_run_id} is now durably PAUSED.")
        
        # 4. Return. The worker is now free.
    ```

2.  **File: `highway_core/tools/email.py`**

      * Create this new file for the stubs.

    <!-- end list -->

    ```python
    import logging
    from .decorators import tool

    logger = logging.getLogger(__name__)

    @tool("email.send_approved_notification")
    def send_approved_notification(email: str):
        logger.info(f"--- EMAIL STUB ---")
        logger.info(f"To: {email}")
        logger.info(f"Subject: Your expense report was approved.")

    @tool("email.send_denied_notification")
    def send_denied_notification(email: str):
        logger.info(f"--- EMAIL STUB ---")
        logger.info(f"To: {email}")
        logger.info(f"Subject: Your expense report was denied.")

    @tool("email.escalate_finance_queue")
    def escalate_finance_queue(report_id: str, reason: str):
        logger.info(f"--- EMAIL STUB ---")
        logger.info(f"To: finance_queue@company.com")
        logger.info(f"Subject: Overdue Report {report_id}. Reason: {reason}")
    ```

3.  **File: `highway_core/tools/db.py`**

      * Create this new file for the `db.save_report` stub.

    <!-- end list -->

    ```python
    import logging
    from .decorators import tool

    logger = logging.getLogger(__name__)

    @tool("db.save_report")
    def save_report(report_data: dict) -> dict:
        logger.info(f"--- DB STUB ---")
        logger.info(f"Saving report {report_data.get('id')} to database.")
        # In a real system, this would return the saved object with a new ID
        report_data['id'] = report_data.get('id', 'exp-123-saved')
        return report_data
    ```

4.  **File: `highway_core/tools/api.py`**

      * Create this new file for the `api.payment_gateway.pay` stub.

    <!-- end list -->

    ```python
    import logging
    from .decorators import tool

    logger = logging.getLogger(__name__)

    @tool("api.payment_gateway.pay")
    def pay(report_id: str, amount: int):
        logger.info(f"--- API STUB ---")
        logger.info(f"Calling payment gateway for report {report_id} for ${amount}.")
        return {"status": "paid", "transaction_id": "txn_abc123"}
    ```

-----

### **Phase 6: The API (Flask)**

1.  **File: `pyproject.toml`**

      * Add `flask` to the `[project.dependencies]`:
        ```toml
        "flask>=3.0.0",
        ```

2.  **File: `highway_core/api/app.py`**

      * Create this new directory and file. This is the API server.

    <!-- end list -->

    ```python
    import logging
    import os
    import json
    import uuid
    from flask import Flask, request, jsonify

    from highway_core.persistence.database import get_db_manager
    from highway_core.engine.models import AnyOperatorModel

    # We must use the highway_dsl library to load the workflow
    try:
        from highway_dsl import Workflow
    except ImportError:
        print("Error: highway_dsl library not found. Please install 'highway-dsl'.")
        exit(1)

    app = Flask(__name__)
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("HighwayAPI")

    # Use a single DB manager for the Flask app
    db_manager = get_db_manager()

    @app.route("/workflow/start/<path:workflow_name>", methods=["POST"])
    def start_workflow(workflow_name: str):
        """
        Starts a new DURABLE workflow.
        Finds the workflow .py file, loads it, and registers it with the DB.
        The Scheduler will pick it up.
        """
        logger.info(f"Received request to start durable workflow: {workflow_name}")
        
        # 1. Find the workflow file (assume it's in tests/data for now)
        from cli import load_workflow_from_python # Reuse the CLI's loader
        
        # This is a hack for testing. In prod, you'd have a workflow registry.
        workflow_path = os.path.join(
            os.path.dirname(__file__), 
            '..', '..', 'tests', 'data', f"{workflow_name}.py"
        )
        
        if not os.path.exists(workflow_path):
            logger.warning(f"Workflow definition '{workflow_name}.py' not found at {workflow_path}")
            return jsonify({"error": f"Workflow definition '{workflow_name}.py' not found."}), 404
            
        try:
            # 2. Load the highway_dsl.Workflow object
            workflow_dsl: Workflow = load_workflow_from_python(workflow_path)
            
            # 3. Convert Pydantic models to a list of dicts for persistence
            tasks_list = [
                task.model_dump(mode='json') for task in workflow_dsl.tasks.values()
            ]
            
            # 4. Create the durable workflow in the DB
            workflow_id = str(uuid.uuid4())
            db_manager.create_durable_workflow(
                workflow_id=workflow_id,
                workflow_name=workflow_dsl.name,
                start_task_id=workflow_dsl.start_task,
                variables=workflow_dsl.variables,
                tasks=tasks_list
            )
            
            return jsonify({
                "message": "Durable workflow created",
                "workflow_id": workflow_id,
                "status": "PENDING"
            }), 202
            
        except Exception as e:
            logger.error(f"Error starting workflow {workflow_name}: {e}", exc_info=True)
            return jsonify({"error": f"Failed to start workflow: {str(e)}"}), 500

    @app.route("/workflow/resume", methods=["GET"])
    def resume_workflow():
        """
        Resumes a workflow task that is 'WAITING_FOR_EVENT'.
        e.g., /workflow/resume?token=...&decision=APPROVED
        """
        token = request.args.get("token")
        decision = request.args.get("decision")
        
        if not token or not decision:
            return jsonify({"error": "Missing 'token' or 'decision' query parameter"}), 400
            
        try:
            task = db_manager.get_task_by_token(token)
            
            if not task:
                return jsonify({"error": "Invalid or expired token"}), 404
                
            if task.status != "WAITING_FOR_EVENT":
                return jsonify({"error": "Task is not waiting for an event"}), 400
            
            # 1. Save the result
            result_data = {"status": decision, "reason": "HUMAN_INPUT"}
            if task.result_key:
                db_manager.store_result(
                    task.workflow_id, task.task_id, task.result_key, result_data
                )
            
            # 2. Mark the human task as COMPLETED
            db_manager.update_task_status_by_workflow(task.workflow_id, task.task_id, "completed")
            
            logger.info(f"Resumed task {task.task_id} for workflow {task.workflow_id} with decision: {decision}")
            
            return jsonify({
                "message": "Workflow task resumed",
                "workflow_id": task.workflow_id,
                "task_id": task.task_id,
                "status": "COMPLETED"
            }), 200

        except Exception as e:
            logger.error(f"Error resuming with token {token}: {e}", exc_info=True)
            return jsonify({"error": f"Failed to resume workflow: {str(e)}"}), 500

    @app.route("/workflow/status/<workflow_id>", methods=["GET"])
    def get_workflow_status(workflow_id: str):
        """
        Gets the status of a workflow and all its tasks.
        """
        workflow = db_manager.load_workflow(workflow_id)
        if not workflow:
            return jsonify({"error": "Workflow not found"}), 404
            
        tasks = db_manager.get_tasks_by_workflow(workflow_id)
        tasks_status = [
            {
                "task_id": t['task_id'], 
                "status": t['status'], 
                "error": t.get('error_message')
            } for t in tasks
        ]
        
        return jsonify({
            "workflow_id": workflow_id,
            "workflow_name": workflow['name'],
            "workflow_status": workflow['status'],
            "tasks": tasks_status
        }), 200

    def run_server():
        app.run(debug=True, port=5000, host='0.0.0.0')

    if __name__ == "__main__":
        run_server()
    ```

-----

### **Phase 7: The New Test Workflow**

**File: `tests/data/long_running_with_pause_and_resume_workflow.py`**

  * Create this new file to define the "Expense Report" workflow.

    ```python
    import sys
    from datetime import timedelta

    try:
        from highway_dsl import (
            WorkflowBuilder,
            RetryPolicy,
            TimeoutPolicy
        )
    except ImportError:
        print("Error: highway_dsl library not found.")
        sys.exit(1)


    def create_expense_workflow():
        """
        Defines the Expense Report Approval workflow using Highway DSL.
        
        This workflow models a human-in-the-loop process with a timeout,
        conditional branching, and parallel execution.
        """

        builder = WorkflowBuilder("ExpenseReportApproval")

        # 1. save the report
        builder.task(
            "save_report",
            "db.save_report",
            args=["{{variables.report_data}}"],
            result_key="report",
            retry_policy=RetryPolicy(max_retries=2, delay=timedelta(seconds=1))
        )

        # 2. Request manager approval.
        # This task will durably pause. We set a 3-day timeout
        builder.task(
            "request_manager_approval",
            "human.request_approval",
            # We pass the prompt and manager email
            args=[
                "Approve expense report {{report.id}} for ${{report.amount}}?",
                "{{report.manager_email}}"
            ],
            result_key="approval", # The result will be {"status": "APPROVED" | "DENIED"}
            timeout_policy=TimeoutPolicy(
                timeout=timedelta(days=3),
                kill_on_timeout=False # We want it to report a timeout
            )
        )

        # 3. Main conditional logic
        builder.condition(
            "check_approval",
            condition="{{approval.status}} == 'APPROVED'",
            
            # --- 3a. IF TRUE (Approved) ---
            if_true=lambda approved_branch: approved_branch.parallel(
                "process_approved_branch",
                branches={
                    "payments": lambda pb: pb.task(
                        "run_payment",
                        "api.payment_gateway.pay",
                        args=["{{report.id}}", "{{report.amount}}"]
                    ),
                    "notify": lambda pb: pb.task(
                        "notify_employee_approved",
                        "email.send_approved_notification",
                        args=["{{report.user_email}}"]
                    )
                }
            ),

            # --- 3b. IF FALSE (Denied or Timeout) ---
            if_false=lambda denied_branch: denied_branch.condition(
                "check_denial_or_timeout",
                condition="{{approval.reason}} == 'TIMEOUT'",
                
                # --- IF TRUE (Timeout) ---
                if_true=lambda timeout_branch: timeout_branch.task(
                    "escalate_to_finance",
                    "email.escalate_finance_queue",
                    args=["{{report.id}}", "Overdue approval"]
                ),
                
                # --- IF FALSE (Explicitly Denied) ---
                if_false=lambda regular_denial_branch: regular_denial_branch.task(
                    "notify_employee_denied",
                    "email.send_denied_notification",
                    args=["{{report.user_email}}"]
                )
            )
        )
        
        # --- 4. Final "fan-in" task ---
        # This task runs after any of the three final outcomes.
        builder.task(
            "close_report",
            "tools.log.info", # Just log it
            args=["Closing report {{report.id}}"],
            dependencies=[
                "process_approved_branch", # The parallel operator
                "escalate_to_finance",     # The timeout task
                "notify_employee_denied"   # The denied task
            ]
        )

        workflow = builder.build()

        workflow.set_variables({
            "report_data": {
                "id": "exp-test-123",
                "amount": 500,
                "user_email": "employee@company.com",
                "manager_email": "manager@company.com"
            }
        })

        return workflow


    if __name__ == "__main__":
        expense_workflow = create_expense_workflow()
        print(expense_workflow.to_yaml())
    ```

-----

### **Phase 8: Update CLI to Run Services**

**File: `cli.py`**

  * This file needs to be significantly updated to launch the new services.

  * *Add* these imports at the top:

    ```python
    import threading
    import subprocess
    import json
    import time
    ```

  * *Replace* the `main()` function with this new version, which includes the new `durable` command group.

    ```python
    def main():
        parser = argparse.ArgumentParser(
            description="Highway Core - Run workflows from YAML files or highway_dsl Python files"
        )
        
        subparsers = parser.add_subparsers(dest="command", help="Available commands", required=True)
        
        # --- 'run' command (LOCAL MODE) ---
        run_parser = subparsers.add_parser(
            "run", 
            help="Run a workflow in-memory (LOCAL mode)"
        )
        run_parser.add_argument(
            "workflow_path", help="Path to the workflow file (YAML or Python)"
        )
        run_parser.add_argument(
            "--run-id",
            help="Unique ID for this workflow run (default: auto-generated UUID)",
        )
        run_parser.add_argument(
            "--verbose", "-v", action="store_true", help="Enable verbose logging"
        )
        
        # --- 'durable' command group (DURABLE MODE) ---
        durable_parser = subparsers.add_parser(
            "durable", 
            help="Run durable services (Scheduler, Worker, API)"
        )
        durable_sub = durable_parser.add_subparsers(dest="durable_cmd", help="Durable mode commands", required=True)
        
        # 'durable start-services'
        durable_sub.add_parser(
            "start-services", 
            help="Starts the Scheduler and Worker daemons"
        )
        
        # 'durable start-api'
        durable_sub.add_parser(
            "start-api",
            help="Starts the Flask API server"
        )
        
        # 'durable submit'
        submit_parser = durable_sub.add_parser(
            "submit",
            help="Submits a new durable workflow via the API"
        )
        submit_parser.add_argument(
            "workflow_name", 
            help="Name of the workflow file (e.g., 'long_running_with_pause_and_resume_workflow')"
        )
        
        # 'durable resume'
        resume_parser = durable_sub.add_parser(
            "resume",
            help="Resumes a waiting human task via the API"
        )
        resume_parser.add_argument("--token", required=True, help="The event token")
        resume_parser.add_argument("--decision", required=True, choices=["APPROVED", "DENIED"], help="The decision")

        # --- 'webhooks' command group (unchanged) ---
        webhook_parser = subparsers.add_parser("webhooks", help="Webhook management commands")
        webhook_subparsers = webhook_parser.add_subparsers(dest="webhook_cmd", help="Webhook commands")
        webhook_run_parser = webhook_subparsers.add_parser("run", help="Run the webhook runner")
        webhook_run_parser.add_argument(
            "--batch-size", type=int, default=100, help="Number of webhooks to process in each batch (default: 100)"
        )
        webhook_run_parser.add_argument(
            "--concurrency", type=int, default=20, help="Number of concurrent webhook requests (default: 20)"
        )
        webhook_status_parser = webhook_subparsers.add_parser("status", help="Show webhook processing status")
        webhook_cleanup_parser = webhook_subparsers.add_parser("cleanup", help="Clean up old completed webhooks")
        webhook_cleanup_parser.add_argument(
            "--days-old", type=int, default=30, help="Clean webhooks older than this many days (default: 30)"
        )
        
        args = parser.parse_args()

        if args.command == "run":
            run_local_workflow(args)
        elif args.command == "durable":
            if args.durable_cmd == "start-services":
                run_durable_services()
            elif args.durable_cmd == "start-api":
                run_durable_api()
            elif args.durable_cmd == "submit":
                submit_durable_workflow(args)
            elif args.durable_cmd == "resume":
                resume_durable_workflow(args)
        elif args.command == "webhooks":
            if args.webhook_cmd == "run":
                run_webhooks_command(args)
            elif args.webhook_cmd == "status":
                webhook_status_command(args)
            elif args.webhook_cmd == "cleanup":
                webhook_cleanup_command(args)
            else:
                webhook_parser.print_help()
    ```

  * *Add* these new functions *above* the `main()` function in `cli.py`:

    ```python
    def run_local_workflow(args):
        """Existing 'run' command logic for LOCAL mode."""
        workflow_path = Path(args.workflow_path)
        if not workflow_path.exists():
            print(f"Error: Workflow file '{workflow_path}' does not exist.", file=sys.stderr)
            sys.exit(1)
        
        run_id = args.run_id or f"cli-run-{str(uuid.uuid4())}"
        print(f"Running LOCAL workflow from: {workflow_path} (Run ID: {run_id})")
        print("-" * 50)
        
        try:
            if workflow_path.suffix.lower() in [".yaml", ".yml"]:
                result = run_workflow_from_yaml(
                    yaml_path=str(workflow_path), workflow_run_id=run_id
                )
            elif workflow_path.suffix.lower() == ".py":
                workflow = load_workflow_from_python(str(workflow_path))
                yaml_content = workflow.to_yaml()
                import tempfile
                with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as temp_file:
                    temp_file.write(yaml_content)
                    temp_file_path = temp_file.name
                try:
                    result = run_workflow_from_yaml(
                        yaml_path=temp_file_path, workflow_run_id=run_id
                    )
                finally:
                    os.unlink(temp_file_path)
            else:
                print(f"Error: Unsupported file format '{workflow_path.suffix}'", file=sys.stderr)
                sys.exit(1)
            
            print("-" * 50)
            if result["status"] == "completed":
                print("✅ LOCAL Workflow completed successfully!")
            elif result["status"] == "failed":
                print(f"❌ LOCAL Workflow failed: {result.get('error', 'Unknown error')}")
                sys.exit(1)
            
            # ... (rest of task summary logic as before) ...
        
        except Exception as e:
            print(f"❌ Error running LOCAL workflow: {e}", file=sys.stderr, exc_info=True)
            sys.exit(1)

    def run_durable_services():
        """Starts Scheduler and Worker in separate threads."""
        print("Starting durable services (Scheduler + Worker)... Press Ctrl+C to stop.")
        
        def start_scheduler():
            from highway_core.services.scheduler import Scheduler
            Scheduler().run()
            
        def start_worker():
            from highway_core.services.worker import Worker
            Worker().run()
            
        scheduler_thread = threading.Thread(target=start_scheduler, daemon=True, name="SchedulerThread")
        worker_thread = threading.Thread(target=start_worker, daemon=True, name="WorkerThread")
        
        try:
            scheduler_thread.start()
            worker_thread.start()
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down services...")

    def run_durable_api():
        """Starts the Flask API server."""
        print("Starting Highway API server on http://127.0.0.1:5000")
        try:
            # We assume cli.py is in the root, so highway_core/api/app.py is the path
            api_path = os.path.join(os.path.dirname(__file__), "highway_core", "api", "app.py")
            if not os.path.exists(api_path):
                print(f"Error: Could not find API app at {api_path}")
                sys.exit(1)
            
            # Use sys.executable to ensure we use the same python env
            subprocess.run([sys.executable, "-m", "flask", "--app", api_path, "run", "--host=0.0.0.0", "--port=5000"], check=True)
        except KeyboardInterrupt:
            print("\nAPI server stopped.")
            
    def submit_durable_workflow(args):
        """Client command to submit a new workflow to the API."""
        import requests
        try:
            url = f"http://127.0.0.1:5000/workflow/start/{args.workflow_name}"
            response = requests.post(url)
            print(json.dumps(response.json(), indent=2))
        except requests.ConnectionError:
            print(f"Error: Could not connect to Highway API at {url}. Is it running?")
            print("You can run it with: python cli.py durable start-api")
        except Exception as e:
            print(f"Error submitting workflow: {e}")

    def resume_durable_workflow(args):
        """Client command to resume a waiting task."""
        import requests
        try:
            url = f"http://127.0.0.1:5000/workflow/resume?token={args.token}&decision={args.decision}"
            response = requests.get(url)
            print(json.dumps(response.json(), indent=2))
        except requests.ConnectionError:
            print(f"Error: Could not connect to Highway API at {url}. Is it running?")
            print("You can run it with: python cli.py durable start-api")
        except Exception as e:
            print(f"Error resuming workflow: {e}")
    ```

  * **File: `cli.py`**

      * *At the very top of the file*, modify the `sys.path.insert` to be more robust:

    <!-- end list -->

    ```python
    #!/usr/bin/env python3
    """
    Highway Core CLI - Run workflows from YAML files or highway_dsl Python files
    """

    import argparse
    import importlib.util
    import os
    import sys
    import uuid
    from pathlib import Path
    import asyncio

    # --- THIS IS THE MODIFIED BLOCK ---
    # Add the project root to the path
    # This ensures that `from highway_core...` imports work
    # when running `python cli.py` from the root directory.
    project_root = os.path.dirname(os.path.abspath(__file__))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    # --------------------------------------

    from highway_core.engine.engine import run_workflow_from_yaml
    # ... (rest of the imports)
    ```
