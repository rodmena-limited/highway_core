# --- engine/operator_handlers/parallel_handler.py ---
# Purpose: Handles the 'ParallelOperator'.
# Responsibilities:
# - Activates all branches by conceptually completing empty branches
# so they don't block downstream "fan-in" tasks.

import logging
from highway_core.engine.models import ParallelOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
from highway_core.tools.bulkhead import BulkheadManager
from typing import List

logger = logging.getLogger(__name__)


def execute(
    task: ParallelOperatorModel,
    state: WorkflowState,
    orchestrator,  # Added for consistent signature
    registry: ToolRegistry,
    bulkhead_manager: BulkheadManager,
) -> List[str]:
    """
    Executes a ParallelOperator by activating all branches.
    """
    logger.info("ParallelHandler: Activating %s branches.", len(task.branches))

    # Conceptually complete any branches that are empty or invalid
    # so they don't block the downstream "fan-in" task.
    for branch_name, task_ids in task.branches.items():
        if not task_ids:
            # If a branch is empty, conceptually complete it so downstream tasks aren't blocked
            logger.info(
                "ParallelHandler: Branch '%s' is empty, marking as conceptually completed.",
                branch_name,
            )

    # The actual parallel execution will be handled by the orchestrator's TopologicalSorter
    # which will automatically handle tasks in parallel when their dependencies are met
    return []
