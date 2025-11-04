# highway_core/engine/operator_handlers/while_handler.py
import graphlib
import logging
from typing import TYPE_CHECKING, List, Optional

from highway_core.engine.models import WhileOperatorModel
from highway_core.engine.operator_handlers.condition_handler import (
    eval_condition,
)
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
    task: WhileOperatorModel,
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
    Executes a WhileOperator by running its own internal loop.
    This entire function blocks the main orchestrator's thread
    until the loop is complete.
    """

    # Get the original loop body task IDs to mark them as conceptually completed later
    loop_body_task_ids = [t.task_id for t in task.loop_body]

    loop_body_tasks = {t.task_id: t for t in task.loop_body}
    # Remove dependency on the parent while loop task from loop body tasks
    # This is necessary to avoid circular dependencies when executing the sub-workflow
    loop_graph = {}
    for t in task.loop_body:
        # Create a copy of dependencies but exclude the parent while loop task
        filtered_deps = [dep for dep in t.dependencies if dep != task.task_id]
        loop_graph[t.task_id] = set(filtered_deps)

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

        # 2. Create an isolated state for this iteration
        from copy import deepcopy

        iteration_state = deepcopy(
            state
        )  # Create a deep copy of the current state to ensure complete isolation
        iteration_state.loop_context["iteration"] = iteration

        # 3. Run the sub-workflow with the isolated state
        try:
            _run_sub_workflow(
                sub_graph_tasks=loop_body_tasks,
                sub_graph=loop_graph,
                state=iteration_state,  # Use the isolated state
                registry=registry,  # type: ignore
                bulkhead_manager=bulkhead_manager,  # type: ignore
                executor=executor,  # Pass the executor to the sub-workflow
                available_executors=orchestrator.executors,  # Pass available executors from orchestrator
                orchestrator=orchestrator,  # Pass the orchestrator for proper error handling
            )
            # Merge relevant changes back to the main state
            state.memory.update(iteration_state.memory)
            state.results.update(iteration_state.results)
        except Exception as e:
            logger.error("WhileHandler: Sub-workflow failed: %s", e)
            raise  # Propagate failure to the main orchestrator

        iteration += 1

    # Mark all loop body tasks as conceptually completed so the main orchestrator
    # doesn't try to execute them again, and update their status in the database
    for task_id in loop_body_task_ids:
        if hasattr(orchestrator, "completed_tasks"):
            # Add to completed tasks so the main orchestrator will know these are done
            orchestrator.completed_tasks.add(task_id)
            logger.info(
                "WhileHandler: Marked loop body task '%s' as conceptually completed",
                task_id,
            )

        # Update task status in the database from 'executing' to 'completed'
        if hasattr(orchestrator, "persistence") and orchestrator.persistence:
            try:
                orchestrator.persistence.complete_task(
                    orchestrator.run_id, task_id, result=None
                )
                logger.debug(
                    "WhileHandler: Updated task '%s' status to completed in database",
                    task_id,
                )
            except Exception as e:
                logger.error(
                    "WhileHandler: Failed to update task '%s' status in database: %s",
                    task_id,
                    e,
                )

    # The loop is finished, return no new tasks
    return []
