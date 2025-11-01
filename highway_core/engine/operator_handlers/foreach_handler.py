# --- engine/operator_handlers/foreach_handler.py ---
# Purpose: Handles the 'ForEachOperator' (loop).
# Responsibilities:
# - A real implementation is very complex.
# - This stub will just log and move on.

from highway_dsl import ForEachOperator
from engine.state import WorkflowState


def execute(task: ForEachOperator, state: WorkflowState) -> list[str]:
    """
    Executes a ForEachOperator.
    A real engine would need to instantiate a sub-orchestrator
    for the loop_body and iterate over the resolved 'items'.
    """
    items = state.resolve_templating(task.items)
    print(f"  [ForEachHandler] STUB: Would iterate over {len(items)} items.")

    # For now, just return the first task of the loop body
    if task.loop_body:
        first_task_in_loop = task.loop_body[0].task_id
        # A real engine needs to set state.loop_context['item']
        print(
            f"  [ForEachHandler] STUB: Returning first task of loop: {first_task_in_loop}"
        )
        return [first_task_in_loop]

    return []
