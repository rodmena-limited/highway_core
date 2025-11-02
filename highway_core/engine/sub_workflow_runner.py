import graphlib
from typing import Dict, Set
from highway_core.engine.models import AnyOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
from highway_core.tools.bulkhead import BulkheadManager
from highway_core.engine.operator_handlers import task_handler


def _run_sub_workflow(
    sub_graph_tasks: Dict[str, AnyOperatorModel],
    sub_graph: Dict[str, Set[str]],
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