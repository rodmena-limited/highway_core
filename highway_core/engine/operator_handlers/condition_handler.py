# --- engine/operator_handlers/condition_handler.py ---
# Purpose: Handles the 'ConditionOperator' (if/else).
# Responsibilities:
# - Resolves the 'condition' string.
# - Evaluates the condition.
# - Returns the 'if_true' or 'if_false' task ID to the orchestrator.

from highway_dsl import ConditionOperator
from engine.state import WorkflowState


def execute(task: ConditionOperator, state: WorkflowState) -> list[str]:
    """
    Evaluates a ConditionOperator.
    """
    # 1. Resolve the condition string
    # This is a simple implementation. A safe evaluator is needed.
    # WARNING: eval() is unsafe. A real implementation MUST use
    # a safe expression evaluator (e.g., 'py_expression_eval' library).
    resolved_condition_str = state.resolve_templating(task.condition)

    # Simple replacement for common boolean checks
    resolved_condition_str = resolved_condition_str.replace(
        "== true", "== True"
    ).replace("== false", "== False")

    # In a real engine, you would use a safe evaluator.
    # For this stub, we'll use a very limited, unsafe eval.
    is_true = False
    try:
        # This is a security risk. DO NOT use in production.
        # Use a library like 'asteval' or 'py_expression_eval'.
        is_true = eval(resolved_condition_str)
        print(f"  [ConditionHandler] Evaluated '{resolved_condition_str}': {is_true}")
    except Exception as e:
        print(
            f"  [ConditionHandler] FAILED to evaluate condition '{resolved_condition_str}': {e}"
        )

    # 2. Return the next task ID
    if is_true:
        return [task.if_true] if task.if_true else []
    else:
        return [task.if_false] if task.if_false else []
