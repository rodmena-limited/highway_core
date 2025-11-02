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


class Orchestrator:
    def __init__(
        self, workflow: WorkflowModel, state: WorkflowState, registry: ToolRegistry
    ):
        self.workflow = workflow
        self.state = state
        self.registry = registry

        # 1. Build the dependency graph for ONLY top-level tasks
        self.graph: Dict[str, Set[str]] = {
            task_id: set(task.dependencies) for task_id, task in workflow.tasks.items()
        }
        self.sorter = graphlib.TopologicalSorter(self.graph)

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
        print("Orchestrator initialized with TopologicalSorter.")

    def run(self):
        """
        Runs the workflow execution loop using TopologicalSorter.
        """
        print(f"Orchestrator: Starting workflow '{self.workflow.name}'")

        try:
            self.sorter.prepare()

            while self.sorter.is_active():
                runnable_tasks = self.sorter.get_ready()
                print(
                    f"Orchestrator: Submitting {len(runnable_tasks)} tasks to executor: {list(runnable_tasks)}"
                )
                futures: Dict[Future, str] = {
                    self.executor.submit(self._execute_task, task_id): task_id
                    for task_id in runnable_tasks
                }
                for future in futures:
                    task_id = futures[future]
                    try:
                        # This function now just returns the task_id
                        result_task_id = future.result()
                        self.sorter.done(result_task_id)
                        print(f"Orchestrator: Task {result_task_id} completed.")
                    except Exception as e:
                        print(
                            f"Orchestrator: FATAL ERROR executing task '{task_id}': {e}"
                        )
                        self.sorter.done(task_id)
                        raise e

        except Exception as e:
            print(f"Orchestrator: FATAL ERROR in workflow execution: {e}")
        finally:
            self.executor.shutdown(wait=True)
            self.bulkhead_manager.shutdown_all()

        print(f"Orchestrator: Workflow '{self.workflow.name}' finished.")

    def _execute_task(self, task_id: str) -> str:
        """Runs a single task and returns its task_id."""
        task_model = self.workflow.tasks.get(task_id)
        if not task_model:
            raise ValueError(f"Task ID '{task_id}' not found.")

        print(
            f"Orchestrator: Executing task {task_id} (Type: {task_model.operator_type})"
        )

        handler_func = self.handler_map.get(task_model.operator_type)
        if not handler_func:
            print(f"Orchestrator: Error - No handler for {task_model.operator_type}")
            return []

        # All handlers now share this signature
        handler_func(
            task=task_model,
            state=self.state,
            orchestrator=self,
            registry=self.registry,
            bulkhead_manager=self.bulkhead_manager,
        )
        return task_id

    # Avoid using __del__ for cleanup because it can be called multiple times
    # and it can cause issues. Cleanup is handled in the run method's finally block.
