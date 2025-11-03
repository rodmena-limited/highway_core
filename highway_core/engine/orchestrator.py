import graphlib
import logging
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional, Set

# NEW IMPORTS
from highway_core.engine.executors.base import BaseExecutor
from highway_core.engine.executors.docker import DockerExecutor  # <-- NEW
from highway_core.engine.executors.local_python import LocalPythonExecutor
from highway_core.engine.models import (  # <-- NEW: Import TaskOperatorModel
    AnyOperatorModel,
    TaskOperatorModel,
    WorkflowModel,
)
from highway_core.engine.operator_handlers import (
    condition_handler,
    foreach_handler,
    parallel_handler,
    task_handler,
    wait_handler,
    while_handler,
)
from highway_core.engine.resource_manager import ContainerResourceManager
from highway_core.engine.state import WorkflowState
from highway_core.persistence.manager import PersistenceManager
from highway_core.persistence.sql_persistence import SQLPersistence
from highway_core.tools.bulkhead import BulkheadManager
from highway_core.tools.registry import ToolRegistry

logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(
        self,
        workflow_run_id: str,
        workflow: WorkflowModel,
        persistence_manager: PersistenceManager,
        registry: ToolRegistry,
    ) -> None:
        logger.debug("Orchestrator.__init__ called")
        logger.info("Orchestrator: Initializing orchestrator instance")
        self.run_id = workflow_run_id
        self.workflow = workflow
        self.registry = registry
        self.resource_manager = ContainerResourceManager(workflow_run_id)
        self.persistence = persistence_manager

        loaded_state, self.completed_tasks = self.persistence.load_workflow_state(
            self.run_id
        )

        if loaded_state:
            self.state = loaded_state
        else:
            self.state = WorkflowState(workflow.variables)
            self.completed_tasks = set()
            self.persistence.start_workflow(
                self.run_id, self.workflow.name, self.workflow.variables
            )

        # 1. Build the dependency graph for ONLY top-level tasks
        self.graph: Dict[str, Set[str]] = {
            task_id: set(task.dependencies) for task_id, task in workflow.tasks.items()
        }
        logger.info("Orchestrator: Built dependency graph: %s", self.graph)
        logger.info(
            "Orchestrator: Completed tasks from persistence: %s",
            self.completed_tasks,
        )

        self.sorter = graphlib.TopologicalSorter(self.graph)

        # Prepare the sorter
        self.sorter.prepare()
        logger.info(
            "Orchestrator: Sorter prepared, is_active: %s",
            self.sorter.is_active(),
        )

        # 2. Update handler map
        self.handler_map: Dict[str, Callable[..., Any]] = {
            "task": task_handler.execute,
            "condition": condition_handler.execute,
            "parallel": parallel_handler.execute,
            "wait": wait_handler.execute,
            "while": while_handler.execute,
            "foreach": foreach_handler.execute,
        }

        # 3. Initialize Executors based on environment
        #    Check if running inside Docker to avoid nested container issues
        from highway_core.utils.docker_detector import is_running_in_docker

        self.executors: Dict[str, BaseExecutor] = {
            "python": LocalPythonExecutor(),
        }

        # Only add Docker executor if not running inside Docker
        if not is_running_in_docker():
            self.executors["docker"] = DockerExecutor()
            logger.info(
                "Orchestrator: Initialized executors for: %s",
                list(self.executors.keys()),
            )
        else:
            logger.info(
                "Orchestrator: Running inside Docker, only initializing Python executor (Docker executor skipped to prevent nested containers)"
            )
            logger.info(
                "Orchestrator: Initialized executors for: %s",
                list(self.executors.keys()),
            )

        self.bulkhead_manager = BulkheadManager()
        self.executor_pool = ThreadPoolExecutor(
            max_workers=10, thread_name_prefix="HighwayWorker"
        )
        logger.info(
            "Orchestrator initialized with Executors for: %s",
            list(self.executors.keys()),
        )

    def run(self) -> None:
        """
        Runs the workflow execution loop using TopologicalSorter.
        """
        logger.info("Orchestrator: Starting workflow '%s'", self.workflow.name)

        with self.resource_manager:
            try:
                # Get the initial set of runnable tasks
                runnable_tasks = self.sorter.get_ready()

                logger.info(
                    "Orchestrator: Initial runnable tasks: %s, is_active: %s",
                    list(runnable_tasks),
                    self.sorter.is_active(),
                )

                # Check if all tasks have been completed from the loaded state
                # If no tasks are ready and the sorter is not active, workflow is complete
                if not runnable_tasks and not self.sorter.is_active():
                    logger.info(
                        "Orchestrator: All tasks already completed from persisted state. Finishing workflow."
                    )
                    self.persistence.complete_workflow(self.run_id)
                    return

                while self.sorter.is_active():
                    if not runnable_tasks:
                        # If no tasks are ready but the sorter is still active, this could indicate a deadlock
                        # due to circular dependencies or other issues in the workflow
                        logger.warning(
                            "Orchestrator: No runnable tasks but sorter is still active. "
                            "This may indicate a potential circular dependency or unmet dependencies. "
                            "Checking remaining dependencies..."
                        )

                        # Log remaining dependencies for debugging
                        remaining_tasks = set(self.graph.keys()) - self.completed_tasks
                        if remaining_tasks:
                            logger.warning(
                                "Orchestrator: Remaining tasks: %s. "
                                "Their dependencies: %s",
                                remaining_tasks,
                                {
                                    task_id: self.graph[task_id]
                                    for task_id in remaining_tasks
                                },
                            )

                        # Instead of breaking, let's see if dependencies can eventually be resolved
                        # For now, just log the issue and continue
                        logger.info(
                            "Orchestrator: Stopping workflow execution due to unmet dependencies."
                        )
                        self.persistence.fail_workflow(
                            self.run_id,
                            "Workflow stopped due to unmet dependencies - possible circular dependency",
                        )
                        break

                    # Separate tasks that need to be executed from ones that are already completed
                    tasks_to_execute = []
                    for task_id in runnable_tasks:
                        if task_id in self.completed_tasks:
                            # This task was already completed, so just mark it as done in the sorter
                            logger.info(
                                "Orchestrator: Task %s already completed, marking as done.",
                                task_id,
                            )
                            self.sorter.done(task_id)
                        else:
                            # This task needs to be executed
                            tasks_to_execute.append(task_id)

                    # Execute only the tasks that haven't been completed yet
                    if tasks_to_execute:
                        logger.info(
                            "Orchestrator: Submitting %s tasks to executor: %s",
                            len(tasks_to_execute),
                            list(tasks_to_execute),
                        )
                        futures: Dict[Future[str], str] = {}
                        # Submit each task with more specific error handling
                        for task_id in tasks_to_execute:
                            try:
                                future = self.executor_pool.submit(
                                    self._execute_task, task_id
                                )
                                futures[future] = task_id
                            except Exception as e:
                                logger.error(
                                    "Orchestrator: Failed to submit task '%s' to executor: %s",
                                    task_id,
                                    e,
                                )
                                self.persistence.fail_workflow(
                                    self.run_id,
                                    f"Failed to submit task '{task_id}': {str(e)}",
                                )
                                raise e

                        # Wait for all submitted tasks to complete
                        for future in futures:
                            task_id = futures[future]
                            try:
                                # This function now returns the task_id if executed, otherwise None
                                result_task_id = future.result()
                                if result_task_id:
                                    logger.info(
                                        "Orchestrator: Task %s completed.",
                                        result_task_id,
                                    )
                                    self.sorter.done(result_task_id)
                                    self.completed_tasks.add(result_task_id)
                                else:
                                    logger.info(
                                        "Orchestrator: Task %s was not executed because it was locked.",
                                        task_id,
                                    )
                            except Exception as e:
                                logger.error(
                                    "Orchestrator: FATAL ERROR executing task '%s': %s",
                                    task_id,
                                    e,
                                )
                                self.persistence.fail_workflow(self.run_id, str(e))
                                # Ensure task is marked as done to prevent workflow from hanging
                                try:
                                    self.sorter.done(task_id)
                                    self.completed_tasks.add(task_id)
                                except Exception:
                                    # If we can't mark the task as done, the workflow may hang
                                    logger.error(
                                        "Orchestrator: Could not mark failed task %s as done. "
                                        "This may cause workflow to hang.",
                                        task_id,
                                    )
                                raise e
                    else:
                        logger.info(
                            "Orchestrator: No new tasks to execute, continuing to next batch."
                        )

                    # After processing current batch, get next set of ready tasks
                    runnable_tasks = self.sorter.get_ready()
                self.persistence.complete_workflow(self.run_id)

            except Exception as e:
                logger.error("Orchestrator: FATAL ERROR in workflow execution: %s", e)
                self.persistence.fail_workflow(self.run_id, str(e))
            finally:
                self.executor_pool.shutdown(wait=True)  # <-- Use executor_pool
                self.bulkhead_manager.shutdown_all()
                self.close()  # Call the new close method

        logger.info("Orchestrator: Workflow '%s' finished.", self.workflow.name)

    def close(self) -> None:
        """
        Closes any open resources, such as database connections.
        """
        logger.info("Orchestrator: Closing resources.")
        if self.persistence:
            self.persistence.close()

    def _execute_task(self, task_id: str) -> Optional[str]:
        """Runs a single task and returns its task_id if executed, otherwise None."""
        task_model = self.workflow.tasks.get(task_id)
        if not task_model:
            error_msg = f"Task ID '{task_id}' not found in workflow definition."
            logger.error("Orchestrator: %s", error_msg)
            raise ValueError(error_msg)

        logger.info(
            "Orchestrator: Attempting to execute task %s (Type: %s)",
            task_id,
            task_model.operator_type,
        )

        # Attempt to start the task and acquire a lock
        if not self.persistence.start_task(self.run_id, task_model):
            logger.info("Orchestrator: Task %s is locked by another worker, skipping.", task_id)
            return None  # Task is locked, so we don't execute it

        logger.info(
            "Orchestrator: Executing task %s (Type: %s)",
            task_id,
            task_model.operator_type,
        )

        handler_func = self.handler_map.get(task_model.operator_type)
        if not handler_func:
            error_msg = f"No handler for operator type: {task_model.operator_type}"
            logger.error("Orchestrator: %s", error_msg)
            raise ValueError(error_msg)

        # Select the correct executor based on the task's runtime
        selected_executor: Optional[BaseExecutor] = None
        if isinstance(task_model, TaskOperatorModel):
            runtime = task_model.runtime
            selected_executor = self.executors.get(runtime)
            if not selected_executor:
                error_msg = f"No executor registered for runtime: {runtime}"
                logger.error("Orchestrator: %s", error_msg)
                raise ValueError(error_msg)

        try:
            # All handlers now share this signature
            handler_func(
                task=task_model,
                state=self.state,
                orchestrator=self,
                registry=self.registry,
                bulkhead_manager=self.bulkhead_manager,
                executor=selected_executor,  # <-- Pass the selected executor
                resource_manager=self.resource_manager,
                workflow_run_id=self.run_id,
            )
            # After successful execution, mark as complete in persistence
            actual_result = None
            if isinstance(task_model, TaskOperatorModel) and task_model.result_key:
                actual_result = self.state.get_result(task_model.result_key)
            self.persistence.complete_task(self.run_id, task_id, actual_result)
        except Exception as e:
            # Mark the task as failed in persistence
            self.persistence.fail_task(self.run_id, task_id, str(e))
            logger.error(
                "Orchestrator: Handler function failed for task '%s': %s",
                task_id,
                e,
            )
            raise e

        return task_id

    # Avoid using __del__ for cleanup because it can be called multiple times
    # and it can cause issues. Cleanup is handled in the run method's finally block.
