# --- engine/operator_handlers/condition_handler.py ---
# Purpose: Handles the 'ConditionOperator' (if/else).
# Responsibilities:
# - Resolves the 'condition' string.
# - Evaluates the condition.
# - Updates orchestrator state to handle conditional flow.

import ast
import logging
import operator
import warnings
from typing import TYPE_CHECKING, Any, List, Optional

from highway_core.engine.models import ConditionOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry

if TYPE_CHECKING:
    from highway_core.engine.executors.base import (  # <-- Add this import
        BaseExecutor,
    )
    from highway_core.engine.orchestrator import Orchestrator
    from highway_core.tools.bulkhead import BulkheadManager

logger = logging.getLogger(__name__)


def execute(
    task: ConditionOperatorModel,
    state: WorkflowState,
    orchestrator: "Orchestrator",
    registry: Optional["ToolRegistry"],  # <-- Make registry optional
    bulkhead_manager: Optional["BulkheadManager"],  # <-- Make optional
    executor: Optional["BaseExecutor"] = None,  # <-- Add this argument
    resource_manager: Optional[
        Any
    ] = None,  # <-- Add this argument to match orchestrator signature
    workflow_run_id: str = "",  # <-- Add this argument to match orchestrator signature
) -> List[str]:
    """
    Evaluates a ConditionOperator.
    Updates the orchestrator's completed tasks to handle conditional branches.
    """
    logger.info("ConditionHandler: Evaluating '%s'", task.condition)

    # 1. Resolve the condition string
    resolved_condition_value = state.resolve_templating(task.condition)
    # Ensure it's a string for eval_condition
    resolved_condition_str = str(resolved_condition_value)
    result = eval_condition(resolved_condition_str)
    logger.info(
        "ConditionHandler: Resolved to '%s'. Result: %s",
        resolved_condition_str,
        result,
    )

    # 2. Determine which path to take and mark the other as conceptually completed
    next_task_id: Optional[str] = None
    skipped_task_id: Optional[str] = None
    if result:
        next_task_id = task.if_true
        skipped_task_id = task.if_false
        logger.info("ConditionHandler: Taking 'if_true' path to '%s'", next_task_id)
    else:
        next_task_id = task.if_false
        skipped_task_id = task.if_true
        logger.info("ConditionHandler: Taking 'if_false' path to '%s'", next_task_id)

    # 3. Mark the skipped branch as conceptually completed to satisfy dependencies
    # This is needed for tasks that depend on both conditional branches (like log_end in the test)
    if skipped_task_id:
        logger.info(
            "ConditionHandler: Marking '%s' as conceptually completed.",
            skipped_task_id,
        )
        # Mark the skipped task as done in the sorter so that tasks
        # depending on BOTH conditional branches can proceed
        orchestrator.sorter.done(skipped_task_id)
        orchestrator.completed_tasks.add(skipped_task_id)  # <-- ADD THIS LINE

    return []


def eval_condition(condition_str: str) -> bool:
    """
    Safely evaluate a condition string using AST.
    Supports basic comparisons like '200 == 200', 'True', 'False', etc.
    """
    try:
        # Parse the condition into an AST
        tree = ast.parse(condition_str.strip(), mode="eval")
        result = _eval_node(tree.body)
        # Ensure the result is converted to boolean
        return bool(result)
    except Exception as e:
        logger.error(
            "ConditionHandler: Error evaluating condition '%s': %s",
            condition_str,
            e,
        )
        return False


def _eval_node(node: ast.AST) -> Any:
    """
    Recursively evaluate an AST node.
    """
    # Handle constants (ast.Constant is used in Python 3.8+,
    # older versions used ast.Num, ast.Str, ast.NameConstant which are removed in Python 3.14+)
    if isinstance(node, ast.Constant):  # Python 3.8+
        return node.value
    # The following are removed in Python 3.14+ and covered by ast.Constant
    # elif isinstance(node, ast.Num):  # Python < 3.8 compatibility - deprecated & removed
    #     return node.n
    # elif isinstance(node, ast.Str):  # Python < 3.8 compatibility - deprecated & removed
    #     return node.s
    # elif isinstance(
    #     node, ast.NameConstant
    # ):  # True, False, None (Python < 3.8) - deprecated & removed
    #     return node.value

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
        bool_op = node.op
        if isinstance(bool_op, ast.And):
            return all(_eval_node(value) for value in node.values)
        elif isinstance(bool_op, ast.Or):
            return any(_eval_node(value) for value in node.values)

    # Handle unary operations
    elif isinstance(node, ast.UnaryOp):
        unary_op = node.op
        if isinstance(unary_op, ast.Not):
            return not _eval_node(node.operand)
        elif isinstance(unary_op, ast.USub):
            return -_eval_node(node.operand)
        elif isinstance(unary_op, ast.UAdd):
            return +_eval_node(node.operand)

    # Unsupported operation
    raise ValueError(f"Unsupported operation: {type(node)}")
