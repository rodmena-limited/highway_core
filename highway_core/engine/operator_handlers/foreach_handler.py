# highway_core/engine/operator_handlers/foreach_handler.py
import graphlib
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from highway_core.engine.models import AnyOperatorModel, ForEachOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.engine.sub_workflow_runner import _run_sub_workflow
from highway_core.tools.bulkhead import BulkheadManager
from highway_core.tools.registry import ToolRegistry

if TYPE_CHECKING:
    from highway_core.engine.executors.base import BaseExecutor
    from highway_core.engine.orchestrator import Orchestrator
    from highway_core.engine.resource_manager import ContainerResourceManager

logger = logging.getLogger(__name__)


def execute(
    task: ForEachOperatorModel,
    state: WorkflowState,
    orchestrator: "Orchestrator",  # We pass 'self' from Orchestrator
    registry: Optional["ToolRegistry"],  # <-- Make registry optional
    bulkhead_manager: Optional["BulkheadManager"],  # <-- Make optional
    executor: Optional["BaseExecutor"] = None,  # <-- Add this argument
    resource_manager: Optional[
        "ContainerResourceManager"
    ] = None,  # <-- Add this argument to match orchestrator signature
    workflow_run_id: Optional[
        str
    ] = None,  # <-- Add this argument to match orchestrator signature
) -> List[str]:
    """
    Executes a ForEachOperator by running a sub-workflow for each
    item in parallel using the orchestrator's thread pool.
    """

    items = state.resolve_templating(task.items)
    if not isinstance(items, list):
        logger.error("ForEachHandler: Error - 'items' did not resolve to a list.")
        return []

    logger.info("ForEachHandler: Starting parallel processing of %s items.", len(items))

    sub_graph_tasks = {t.task_id: t for t in task.loop_body}
    sub_graph = {t.task_id: set(t.dependencies) for t in task.loop_body}

    # Use the orchestrator's main executor
    futures = []
    for item in items:
        # This function will run in a separate thread
        futures.append(
            orchestrator.executor_pool.submit(
                _run_foreach_item,
                item,
                sub_graph_tasks,
                sub_graph,
                state,  # Pass the main state
                registry,
                bulkhead_manager,
                executor,  # Pass the executor
                available_executors=orchestrator.executors,  # Pass available executors from orchestrator
            )
        )

    # Wait for all sub-workflows to complete
    for future in futures:
        try:
            future.result()  # Wait for it to finish and raise any errors
        except Exception as e:
            logger.error("ForEachHandler: Sub-workflow failed: %s", e)
            raise  # Propagate failure

    logger.info("ForEachHandler: All %s items processed.", len(items))
    return []  # This operator adds no new tasks to the main graph


def _run_foreach_item(
    item: Any,
    sub_graph_tasks: Dict[str, AnyOperatorModel],
    sub_graph: Dict[str, set[str]],
    main_state: WorkflowState,
    registry: Optional["ToolRegistry"],
    bulkhead_manager: Optional["BulkheadManager"],
    executor: Optional["BaseExecutor"] = None,
    available_executors: Optional[Dict[str, "BaseExecutor"]] = None,
) -> None:
    """
    Runs a single iteration of a foreach loop in a separate thread.
    This function acts as a "mini-orchestrator".
    """
    logger.info("ForEachHandler: [Item: %s] Starting sub-workflow...", item)

    # 1. Create an ISOLATED state for this item
    # This is the fix for the race condition
    item_state = WorkflowState(main_state.variables.copy())
    item_state.loop_context["item"] = item

    # 2. Run the sub-workflow
    _run_sub_workflow(
        sub_graph_tasks=sub_graph_tasks,
        sub_graph=sub_graph,
        state=item_state,  # Use the isolated state
        registry=registry,  # type: ignore
        bulkhead_manager=bulkhead_manager,  # type: ignore
        executor=executor,
        available_executors=available_executors,  # Use the available executors passed to this function
    )

    logger.info("ForEachHandler: [Item: %s] Sub-workflow completed.", item)
