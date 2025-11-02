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
            task_id: set(task.dependencies)
            for task_id, task in workflow.tasks.items()
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

        print(f"Orchestrator: Executing task {task_id} (Type: {task_model.operator_type})")
        
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
            bulkhead_manager=self.bulkhead_manager
        )
        return task_id

    def __del__(self):
        """Destructor to ensure cleanup."""
        try:
            self.executor.shutdown(wait=False)
            self.bulkhead_manager.shutdown_all()
        except:
            pass


# --- ADD THIS FUNCTION OUTSIDE THE CLASS ---
# This is a shared, static helper function for loops

def _run_sub_workflow(
    sub_graph_tasks: Dict[str, AnyOperatorModel],
    sub_graph: Dict[str, set],
    state: WorkflowState,
    registry: ToolRegistry,
    bulkhead_manager: BulkheadManager
):
    """
    Runs a sub-workflow (like a loop body) to completion.
    This is a blocking, sequential, "mini-orchestrator".
    """
    sub_sorter = graphlib.TopologicalSorter(sub_graph)
    sub_sorter.prepare()
    
    # We only support 'task' for now in sub-workflows.
    # This can be expanded later.
    sub_handler_map = {
        "task": task_handler.execute
    }

    while sub_sorter.is_active():
        runnable_sub_tasks = sub_sorter.get_ready()
        if not runnable_sub_tasks:
            break
            
        for task_id in runnable_sub_tasks:
            task_model = sub_graph_tasks[task_id]
            
            # We must clone the task to resolve templating
            # This is the fix for the `log_user` problem
            task_clone = task_model.model_copy(deep=True)
            task_clone.args = state.resolve_templating(task_clone.args)
            
            handler_func = sub_handler_map.get(task_clone.operator_type)
            if handler_func:
                # Note: sub-workflows don't get the orchestrator
                handler_func(task_clone, state, None, registry, bulkhead_manager) # Pass None for orchestrator
            
            sub_sorter.done(task_id)

