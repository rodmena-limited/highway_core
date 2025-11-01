# --- engine/operator_handlers/while_handler.py ---
# Purpose: Handles the 'WhileOperator' (loop).
# Responsibilities:
# - Evaluates the 'condition'.
# - If true, returns the first task of the 'loop_body'.
# - If false, returns an empty list (loop is finished).
# - The last task in the 'loop_body' must depend on this task to loop.

from highway_dsl import WhileOperator
from engine.state import WorkflowState


def execute(task: WhileOperator, state: WorkflowState) -> list[str]:
    """
    Executes one iteration check of a WhileOperator.
    """
    # WARNING: Unsafe eval(), see condition_handler.py for notes.
    resolved_condition_str = state.resolve_templating(task.condition)
    is_true = False
    try:
        is_true = eval(resolved_condition_str)
        print(
            f"  [WhileHandler] Evaluated loop condition '{resolved_condition_str}': {is_true}"
        )
    except Exception as e:
        print(
            f"  [WhileHandler] FAILED to evaluate condition '{resolved_condition_str}': {e}"
        )

    if is_true:
        if task.loop_body:
            first_task_in_loop = task.loop_body[0].task_id
            print(
                f"  [WhileHandler] Condition is True. Entering loop at: {first_task_in_loop}"
            )
            return [first_task_in_loop]

    print("  [WhileHandler] Condition is False. Exiting loop.")
    return []
