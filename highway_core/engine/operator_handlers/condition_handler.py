# --- engine/operator_handlers/condition_handler.py ---
# Purpose: Handles the 'ConditionOperator' (if/else).
# Responsibilities:
# - Resolves the 'condition' string.
# - Evaluates the condition.
# - Returns the 'if_true' or 'if_false' task ID to the orchestrator.

from highway_core.engine.models import ConditionOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
import ast
import operator


def execute(
    task: ConditionOperatorModel,
    state: WorkflowState,
    orchestrator,
    registry: ToolRegistry,
) -> list[str]:
    """
    Evaluates a ConditionOperator.
    Returns the next task ID based on the condition result.
    Also marks the non-taken branch as conceptually completed to satisfy dependencies.
    """
    print(f"ConditionHandler: Evaluating '{task.condition}'")

    # 1. Resolve the condition string
    resolved_condition_str = state.resolve_templating(task.condition)
    result = eval_condition(resolved_condition_str)
    print(f"ConditionHandler: Resolved to '{resolved_condition_str}'. Result: {result}")

    # 2. Determine which path to take and mark the other as conceptually completed
    if result:
        next_task_id = task.if_true
        skipped_task_id = task.if_false
        print(
            f"Orchestrator: Task {task.task_id} completed. Adding '{next_task_id}' to queue."
        )
    else:
        next_task_id = task.if_false
        skipped_task_id = task.if_true
        print(
            f"Orchestrator: Task {task.task_id} completed. Adding '{next_task_id}' to queue."
        )

    # Mark the skipped branch as conceptually completed to satisfy dependencies
    # This is needed for tasks that depend on both conditional branches
    if skipped_task_id:
        print(
            f"ConditionHandler: Marking '{skipped_task_id}' as conceptually completed."
        )
        orchestrator.completed_tasks.add(skipped_task_id)

    # Return the single next task to execute
    return [next_task_id] if next_task_id else []


def eval_condition(condition_str: str):
    """
    Safely evaluate a condition string using AST.
    Supports basic comparisons like '200 == 200', 'True', 'False', etc.
    """
    try:
        # Parse the condition into an AST
        tree = ast.parse(condition_str.strip(), mode="eval")
        return _eval_node(tree.body)
    except Exception as e:
        print(f"ConditionHandler: Error evaluating condition '{condition_str}': {e}")
        return False


def _eval_node(node):
    """
    Recursively evaluate an AST node.
    """
    if isinstance(node, ast.Constant):  # Numbers, strings, booleans
        return node.value
    elif isinstance(node, ast.Num):  # Python < 3.8 compatibility
        return node.n
    elif isinstance(node, ast.Str):  # Python < 3.8 compatibility
        return node.s
    elif isinstance(node, ast.NameConstant):  # True, False, None
        return node.value

    # Handle comparison operations
    elif isinstance(node, ast.Compare):
        left = _eval_node(node.left)
        right = _eval_node(node.comparators[0])

        op = node.ops[0]
        if isinstance(op, ast.Eq):
            return operator.eq(left, right)
        elif isinstance(op, ast.NotEq):
            return operator.ne(left, right)
        elif isinstance(op, ast.Lt):
            return operator.lt(left, right)
        elif isinstance(op, ast.LtE):
            return operator.le(left, right)
        elif isinstance(op, ast.Gt):
            return operator.gt(left, right)
        elif isinstance(op, ast.GtE):
            return operator.ge(left, right)

    # Handle boolean operations
    elif isinstance(node, ast.BoolOp):
        op = node.op
        if isinstance(op, ast.And):
            return all(_eval_node(value) for value in node.values)
        elif isinstance(op, ast.Or):
            return any(_eval_node(value) for value in node.values)

    # Handle unary operations
    elif isinstance(node, ast.UnaryOp):
        op = node.op
        if isinstance(op, ast.Not):
            return not _eval_node(node.operand)
        elif isinstance(op, ast.USub):
            return -_eval_node(node.operand)
        elif isinstance(op, ast.UAdd):
            return +_eval_node(node.operand)

    # Unsupported operation
    raise ValueError(f"Unsupported operation: {type(node)}")
