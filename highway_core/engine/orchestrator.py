import graphlib
from concurrent.futures import ThreadPoolExecutor, Future
from highway_core.engine.models import WorkflowModel, AnyOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
from highway_core.engine.operator_handlers import (
    task_handler,
    condition_handler,
    parallel_handler,
    wait_handler,
    while_handler,
    foreach_handler,
)
from highway_core.tools.bulkhead import BulkheadManager
from typing import Dict, Set, Callable, Any, List
import logging

logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(
        self,
        workflow_run_id: str,
        workflow: WorkflowModel,
        persistence_manager,
        registry: ToolRegistry,
    ):
        self.run_id = workflow_run_id
        self.workflow = workflow
        self.registry = registry
        self.persistence = persistence_manager

        loaded_state, self.completed_tasks = self.persistence.load_workflow_state(
            self.run_id
        )

        if loaded_state:
            self.state = loaded_state
        else:
            self.state = WorkflowState(workflow.variables)
            self.completed_tasks = set()

        # 1. Build the dependency graph for ONLY top-level tasks
        self.graph: Dict[str, Set[str]] = {
            task_id: set(task.dependencies) for task_id, task in workflow.tasks.items()
        }
        logger.info("Orchestrator: Built dependency graph: %s", self.graph)
        logger.info(
            "Orchestrator: Completed tasks from persistence: %s", self.completed_tasks
        )

        self.sorter = graphlib.TopologicalSorter(self.graph)

        # Prepare the sorter
        self.sorter.prepare()
        logger.info(
            "Orchestrator: Sorter prepared, is_active: %s", self.sorter.is_active()
        )

        # 2. Update handler map
        self.handler_map: Dict[str, Callable] = {
            "task": task_handler.execute,
            "condition": condition_handler.execute,
            "parallel": parallel_handler.execute,
            "wait": wait_handler.execute,
            "while": while_handler.execute,
            "foreach": foreach_handler.execute,
        }
        self.bulkhead_manager = BulkheadManager()
        self.executor = ThreadPoolExecutor(
            max_workers=10, thread_name_prefix="HighwayWorker"
        )
        logger.info("Orchestrator initialized with TopologicalSorter.")

    def run(self):
        """
        Runs the workflow execution loop using TopologicalSorter.
        """
        logger.info("Orchestrator: Starting workflow '%s'", self.workflow.name)

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
                return

            while self.sorter.is_active():
                if (
                    not runnable_tasks
                ):  # If no tasks are ready, break to avoid infinite loop
                    logger.info(
                        "Orchestrator: No runnable tasks but sorter is still active. Breaking to avoid infinite loop."
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
                    futures: Dict[Future, str] = {
                        self.executor.submit(self._execute_task, task_id): task_id
                        for task_id in tasks_to_execute
                    }
                    for future in futures:
                        task_id = futures[future]
                        try:
                            # This function now just returns the task_id
                            result_task_id = future.result()
                            logger.info(
                                "Orchestrator: Task %s completed.", result_task_id
                            )
                        except Exception as e:
                            logger.error(
                                "Orchestrator: FATAL ERROR executing task '%s': %s",
                                task_id,
                                e,
                            )
                            raise e
                else:
                    logger.info(
                        "Orchestrator: No new tasks to execute, continuing to next batch."
                    )

                # After processing current batch, get next set of ready tasks
                runnable_tasks = self.sorter.get_ready()

        except Exception as e:
            logger.error("Orchestrator: FATAL ERROR in workflow execution: %s", e)
        finally:
            self.executor.shutdown(wait=True)
            self.bulkhead_manager.shutdown_all()

        logger.info("Orchestrator: Workflow '%s' finished.", self.workflow.name)

    def _execute_task(self, task_id: str) -> str:
        """Runs a single task and returns its task_id."""
        task_model = self.workflow.tasks.get(task_id)
        if not task_model:
            raise ValueError(f"Task ID '{task_id}' not found.")

        logger.info(
            "Orchestrator: Executing task %s (Type: %s)",
            task_id,
            task_model.operator_type,
        )

        handler_func = self.handler_map.get(task_model.operator_type)
        if not handler_func:
            logger.error(
                "Orchestrator: Error - No handler for %s", task_model.operator_type
            )
            raise ValueError(
                f"No handler for operator type: {task_model.operator_type}"
            )

        # All handlers now share this signature
        handler_func(
            task=task_model,
            state=self.state,
            orchestrator=self,
            registry=self.registry,
            bulkhead_manager=self.bulkhead_manager,
        )

        # At the end of _execute_task, before returning
        # Mark the task as done in the sorter (this allows dependent tasks to become ready)
        self.sorter.done(task_id)  # Tell the sorter this task is done
        self.completed_tasks.add(task_id)  # Add to our set
        self.persistence.save_workflow_state(
            self.run_id, self.state, self.completed_tasks
        )
        return task_id

    # Avoid using __del__ for cleanup because it can be called multiple times
    # and it can cause issues. Cleanup is handled in the run method's finally block.
