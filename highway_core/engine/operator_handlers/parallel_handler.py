# --- engine/operator_handlers/parallel_handler.py ---
# Purpose: Handles the 'ParallelOperator'.
# Responsibilities:
# - Gets all the starting tasks for all 'branches'.
# - Returns all of them to the orchestrator to be run concurrently.
# Note: A real async engine would handle this differently, but in a simple
# linear orchestrator, we just add all branch-starting tasks to the queue.

from highway_dsl import ParallelOperator
from engine.state import WorkflowState


def execute(task: ParallelOperator, state: WorkflowState) -> list[str]:
    """
    Executes a ParallelOperator.
    """
    next_tasks = []
    for branch_name, task_id_list in task.branches.items():
        if task_id_list:
            next_tasks.append(task_id_list[0])  # Add the first task of each branch

    print(f"  [ParallelHandler] Queuing {len(next_tasks)} parallel branches.")
    return next_tasks
