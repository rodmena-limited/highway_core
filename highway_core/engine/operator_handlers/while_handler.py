# highway_core/engine/operator_handlers/while_handler.py
import logging
import graphlib
from highway_core.engine.models import WhileOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.engine.sub_workflow_runner import _run_sub_workflow
from highway_core.engine.operator_handlers.condition_handler import eval_condition
from highway_core.tools.registry import ToolRegistry
from highway_core.tools.bulkhead import BulkheadManager
from typing import List

logger = logging.getLogger(__name__)


def execute(
    task: WhileOperatorModel,
    state: WorkflowState,
    orchestrator,  # We pass 'self' from Orchestrator
    registry: ToolRegistry,
    bulkhead_manager: BulkheadManager,
) -> List[str]:
    """
    Executes a WhileOperator by running its own internal loop.
    This entire function blocks the main orchestrator's thread
    until the loop is complete.
    """

    loop_body_tasks = {t.task_id: t for t in task.loop_body}
    loop_graph = {t.task_id: set(t.dependencies) for t in task.loop_body}

    iteration = 1
    while True:
        # 1. Evaluate condition
        resolved_condition = str(state.resolve_templating(task.condition))
        is_true = eval_condition(resolved_condition)
        logger.info(
            "WhileHandler: Iteration %s - Resolved '%s'. Result: %s",
            iteration,
            resolved_condition,
            is_true,
        )

        if not is_true:
            logger.info("WhileHandler: Condition is False. Exiting loop.")
            break

        logger.info("WhileHandler: Condition True, executing loop body...")

        # 2. Run the sub-workflow
        try:
            _run_sub_workflow(
                sub_graph_tasks=loop_body_tasks,
                sub_graph=loop_graph,
                state=state,  # Use the *main* state
                registry=registry,
                bulkhead_manager=bulkhead_manager,
            )
        except Exception as e:
            logger.error("WhileHandler: Sub-workflow failed: %s", e)
            raise  # Propagate failure to the main orchestrator

        iteration += 1

    # The loop is finished, return no new tasks
    return []
