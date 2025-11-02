# highway_core/engine/operator_handlers/foreach_handler.py
import graphlib
from concurrent.futures import ThreadPoolExecutor
from highway_core.engine.common import ForEachOperatorModel, AnyOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
from highway_core.tools.bulkhead import BulkheadManager
from highway_core.engine.operator_handlers import task_handler
from typing import Any, Dict


def execute(
    task: ForEachOperatorModel,
    state: WorkflowState,
    orchestrator,
    registry: ToolRegistry,
    bulkhead_manager: BulkheadManager,
):
    items = state.resolve_templating(task.items)
    if not isinstance(items, list):
        print(f"ForEachHandler: Error - 'items' did not resolve to a list.")
        return

    print(f"ForEachHandler: Starting parallel processing of {len(items)} items.")

    # Build the sub-graph graph from the main workflow.tasks dict
    sub_graph_tasks = {
        task_id: orchestrator.workflow.tasks[task_id] for task_id in task.loop_body
    }

    # Run each item in a separate thread
    with ThreadPoolExecutor(
        max_workers=5, thread_name_prefix="ForEachWorker"
    ) as executor:
        futures = [
            executor.submit(
                _run_foreach_item,
                item,
                sub_graph_tasks,  # Pass the dict of tasks
                state,  # Pass the *main* state
                registry,
                bulkhead_manager,
            )
            for item in items
        ]
        for future in futures:
            future.result()  # Wait for all to complete

    print(f"ForEachHandler: All {len(items)} items processed.")


def _run_foreach_item(
    item: Any,
    sub_graph_tasks: Dict[str, AnyOperatorModel],
    main_state: WorkflowState,
    registry: ToolRegistry,
    bulkhead_manager: BulkheadManager,
):
    """
    Runs a single iteration of a foreach loop.
    This function acts as a "mini-orchestrator".
    """
    print(f"ForEachHandler: [Item: {item}] Starting sub-workflow...")

    # 1. Create an ISOLATED state, but copy initial variables
    item_state = WorkflowState(main_state._data["variables"].copy())
    item_state._data["loop_context"]["item"] = item

    # 2. Create a graph for *only* the loop body
    loop_graph = {tid: set(t.dependencies) for tid, t in sub_graph_tasks.items()}
    loop_sorter = graphlib.TopologicalSorter(loop_graph)
    loop_sorter.prepare()

    handler_map = {"task": task_handler.execute}  # Can expand this later

    # 3. Run the sub-workflow
    while loop_sorter.is_active():
        for task_id in loop_sorter.get_ready():
            task_model = sub_graph_tasks[task_id]

            # We must clone the task to resolve templating without
            # modifying the original
            task_clone = task_model.model_copy(deep=True)
            task_clone.args = item_state.resolve_templating(task_clone.args)

            handler_func = handler_map.get(task_clone.operator_type)
            if handler_func:
                handler_func(task_clone, item_state, registry, bulkhead_manager)

            loop_sorter.done(task_id)

            # CRITICAL: If this sub-task had a result_key,
            # we must copy its result back to the *item_state*
            # (task_handler already does this)

    print(f"ForEachHandler: [Item: {item}] Sub-workflow completed.")
