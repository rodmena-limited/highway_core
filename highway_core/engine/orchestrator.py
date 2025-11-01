import graphlib
from concurrent.futures import ThreadPoolExecutor, Future
from highway_core.engine.models import (
    WorkflowModel,
    AnyOperatorModel,
    TaskOperatorModel,
    ConditionOperatorModel,
    ParallelOperatorModel,
    WaitOperatorModel,
)
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
from highway_core.engine.operator_handlers import (
    task_handler,
    condition_handler,
    parallel_handler,
    wait_handler,
)
from highway_core.tools.bulkhead import BulkheadManager
from typing import Dict, Set, Callable, Optional, Any, Protocol, Union


class TaskHandlerFunc(Protocol):
    def __call__(
        self,
        task: TaskOperatorModel,
        state: WorkflowState,
        registry: ToolRegistry,
        bulkhead_manager: Optional[BulkheadManager] = None,
    ) -> None: ...


class ConditionHandlerFunc(Protocol):
    def __call__(
        self,
        task: ConditionOperatorModel,
        state: WorkflowState,
        orchestrator: "Orchestrator",
        registry: ToolRegistry,
    ) -> None: ...


class ParallelHandlerFunc(Protocol):
    def __call__(
        self,
        task: ParallelOperatorModel,
        state: WorkflowState,
        registry: ToolRegistry,
        bulkhead_manager: BulkheadManager,
    ) -> None: ...


class WaitHandlerFunc(Protocol):
    def __call__(
        self,
        task: WaitOperatorModel,
        state: WorkflowState,
        registry: ToolRegistry,
        bulkhead_manager: BulkheadManager,
    ) -> None: ...


# Type alias for a generic handler function
HandlerFunc = Union[
    TaskHandlerFunc, ConditionHandlerFunc, ParallelHandlerFunc, WaitHandlerFunc
]


class Orchestrator:
    def __init__(
        self, workflow: WorkflowModel, state: WorkflowState, registry: ToolRegistry
    ):
        self.workflow = workflow
        self.state = state
        self.registry = registry

        # 1. Build the dependency graph for TopologicalSorter
        self.graph: Dict[str, Set[str]] = {
            task_id: set(task.dependencies) for task_id, task in workflow.tasks.items()
        }
        self.sorter = graphlib.TopologicalSorter(self.graph)

        # This is what tracks completion, NOT completed_tasks
        # self.completed_tasks is no longer the source of truth

        # 2. Initialize handler map
        self.handler_map: Dict[str, HandlerFunc] = {
            "task": task_handler.execute,
            "condition": condition_handler.execute,
            "parallel": parallel_handler.execute,
            "wait": wait_handler.execute,
            # Add stubs for future handlers
            "foreach": lambda *args, **kwargs: print(
                "Orchestrator: STUB: foreach handler"
            ),
            "while": lambda *args, **kwargs: print("Orchestrator: STUB: while handler"),
        }

        # 3. Create a single bulkhead manager
        self.bulkhead_manager = BulkheadManager()

        # 4. Create a thread pool for parallel execution
        # We'll use a moderate number of threads
        self.executor = ThreadPoolExecutor(
            max_workers=10, thread_name_prefix="HighwayWorker"
        )

        print("Orchestrator initialized with TopologicalSorter.")

    def _execute_task(self, task_id: str) -> str:
        """
        Internal function to execute a single task.
        This runs in a thread from the ThreadPoolExecutor.
        It must return the task_id on success or raise an exception on failure.
        """
        task_model = self.workflow.tasks.get(task_id)
        if not task_model:
            raise ValueError(f"Task ID '{task_id}' not found in workflow tasks.")

        print(
            f"Orchestrator: Executing task {task_id} (Type: {task_model.operator_type})"
        )

        handler_func = self.handler_map.get(task_model.operator_type)
        if not handler_func:
            print(f"Orchestrator: Error - No handler for {task_model.operator_type}")
            # This is a non-fatal error, we mark as done to unblock others
            return task_id

        # Execute the task
        # Note: 'condition_handler' now needs the 'orchestrator' (self)
        # to call sorter.done() on the skipped branch.
        if task_model.operator_type == "condition":
            # condition_handler expects (task, state, orchestrator, registry)
            handler_func(task_model, self.state, self, self.registry)  # type: ignore
        elif task_model.operator_type in ["task", "parallel", "wait"]:
            # task_handler, parallel_handler, wait_handler expect (task, state, registry, bulkhead_manager)
            handler_func(task_model, self.state, self.registry, self.bulkhead_manager)  # type: ignore
        else:
            # For stub handlers or other types
            handler_func(task_model, self.state, self.registry, self.bulkhead_manager)  # type: ignore

        return task_id

    def run(self):
        """
        Runs the workflow execution loop using TopologicalSorter.
        """
        print(f"Orchestrator: Starting workflow '{self.workflow.name}'")

        try:
            # 1. Prepare the sorter
            self.sorter.prepare()

            while self.sorter.is_active():
                # 2. Get all tasks that are ready to run
                runnable_tasks = self.sorter.get_ready()
                if not runnable_tasks:
                    # This can happen if the graph is stalled, e.g.,
                    # a condition branch was taken but not marked done.
                    # Or it's the end of the workflow.
                    if self.sorter.is_active():
                        print(
                            "Orchestrator: No tasks are ready, but sorter is still active. Checking graph..."
                        )
                        # This might indicate a cycle or a failed conditional branch
                        # For now, we'll just break
                        break
                    continue

                print(
                    f"Orchestrator: Submitting {len(runnable_tasks)} tasks to executor: {list(runnable_tasks)}"
                )

                # 3. Submit all runnable tasks to the thread pool
                futures: Dict[Future, str] = {
                    self.executor.submit(self._execute_task, task_id): task_id
                    for task_id in runnable_tasks
                }

                # 4. Wait for tasks to complete and process results
                for future in futures:
                    task_id = futures[future]
                    try:
                        # Wait for the task to finish
                        result_task_id = future.result()

                        # 5. Mark the task as "done" in the sorter
                        self.sorter.done(result_task_id)
                        print(f"Orchestrator: Task {result_task_id} completed.")

                    except Exception as e:
                        # 6. Handle task failure
                        print(
                            f"Orchestrator: FATAL ERROR executing task '{task_id}': {e}"
                        )
                        # In a real engine, this would trigger failure/retry logic
                        # For now, we stop the whole workflow
                        self.sorter.done(task_id)  # Mark as done to avoid cycle errors
                        raise e  # Re-raise to stop the 'run'

        except Exception as e:
            print(f"Orchestrator: FATAL ERROR in workflow execution: {e}")
        finally:
            # 7. Clean up
            self.executor.shutdown(wait=True)
            self.bulkhead_manager.shutdown_all()

        print(f"Orchestrator: Workflow '{self.workflow.name}' finished.")

    def __del__(self):
        """Destructor to ensure cleanup."""
        try:
            self.executor.shutdown(wait=False)
            self.bulkhead_manager.shutdown_all()
        except:
            pass
