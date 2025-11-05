import ast
import logging
import operator
import warnings
from typing import TYPE_CHECKING, Any, Optional

from highway_core.engine.models import ConditionOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry

if TYPE_CHECKING:
    from highway_core.engine.executors.base import BaseExecutor
    from highway_core.engine.orchestrator import Orchestrator
    from highway_core.tools.bulkhead import BulkheadManager

logger = logging.getLogger(__name__)


def execute(
    task: ConditionOperatorModel,
    state: WorkflowState,
    orchestrator: "Orchestrator",
    registry: Optional["ToolRegistry"],
    bulkhead_manager: Optional["BulkheadManager"],
    executor: Optional["BaseExecutor"] = None,
    resource_manager=None,
    workflow_run_id: str = "",
) -> None:
    """
    Evaluates a ConditionOperator.
    - In 'LOCAL' mode, updates the in-memory sorter.
    - In 'DURABLE' mode, updates the skipped task's status in the DB.
    """
    logger.info("ConditionHandler: Evaluating '%s'", task.condition)
    
    workflow_mode = getattr(orchestrator.workflow, 'mode', 'LOCAL')

    # 1. Resolve and evaluate the condition
    resolved_condition_value = state.resolve_templating(task.condition)
    resolved_condition_str = str(resolved_condition_value)
    
    # DEBUG: Log the resolved condition
    logger.info("DEBUG: ConditionHandler: Resolved condition to '%s'", resolved_condition_str)
    
    # Fix: If the resolved condition still contains template syntax, try to resolve it manually
    if "{{" in resolved_condition_str and "}}" in resolved_condition_str:
        # Manual template resolution for condition evaluation
        resolved_condition_str = _resolve_condition_manually(resolved_condition_str, state)
        logger.info("DEBUG: ConditionHandler: After manual resolution: '%s'", resolved_condition_str)
    
    result = eval_condition(resolved_condition_str)
    logger.info(
        "ConditionHandler: Resolved to '%s'. Result: %s",
        resolved_condition_str,
        result,
    )

    # 2. Determine which task to skip
    skipped_task_id: Optional[str] = None
    if result:
        skipped_task_id = task.if_false
        logger.info("ConditionHandler: Taking 'if_true' path to '%s'", task.if_true)
    else:
        skipped_task_id = task.if_true
        logger.info("ConditionHandler: Taking 'if_false' path to '%s'", task.if_false)

    # 3. Mark the skipped branch as completed
    if skipped_task_id:
        logger.info(
            "ConditionHandler: Marking '%s' as conceptually completed.",
            skipped_task_id,
        )
        
        if workflow_mode == "DURABLE":
            # --- DURABLE MODE ---
            # Set status to COMPLETED in the DB. This unblocks
            # any dependent tasks for the Scheduler.
            db = orchestrator.persistence.db_manager
            db.update_task_status_by_workflow(workflow_run_id, skipped_task_id, "completed")
        else:
            # --- LOCAL MODE ---
            # Mark as done in the in-memory sorter.
            orchestrator.sorter.done(skipped_task_id)
            orchestrator.completed_tasks.add(skipped_task_id)


def _resolve_condition_manually(condition_str: str, state: WorkflowState) -> str:
    """
    Manually resolve template variables in condition strings.
    Handles both {{results.approval.status}} and {{approval.status}} formats.
    """
    import re
    
    # Regex to find template variables
    template_regex = re.compile(r'\{\{([^}]+)\}\}')
    
    def replacer(match):
        path = match.group(1).strip()
        logger.info(f"DEBUG: Resolving path '{path}' from condition")
        
        # Try different path formats in order of preference
        value = None
        
        # 1. Try the path as-is (for explicit paths like results.approval.status)
        value = state.get_value_from_path(path)
        logger.info(f"DEBUG: Direct path '{path}' -> {value}")
        
        # 2. If not found and path doesn't start with a namespace, try results. prefix
        if value is None and not path.startswith(('variables.', 'results.', 'memory.')):
            results_path = f"results.{path}"
            value = state.get_value_from_path(results_path)
            logger.info(f"DEBUG: Results path '{results_path}' -> {value}")
        
        # 3. If still not found, try variables. prefix
        if value is None and not path.startswith(('variables.', 'results.', 'memory.')):
            variables_path = f"variables.{path}"
            value = state.get_value_from_path(variables_path)
            logger.info(f"DEBUG: Variables path '{variables_path}' -> {value}")
        
        if value is not None:
            # For string values, wrap in quotes for proper condition evaluation
            if isinstance(value, str):
                result = f"'{value}'"
                logger.info(f"DEBUG: Found value '{value}', returning '{result}'")
                return result
            else:
                result = str(value)
                logger.info(f"DEBUG: Found value {value}, returning {result}")
                return result
        else:
            # If not found, return the original template
            logger.info(f"DEBUG: Value not found for '{path}', returning original template")
            return match.group(0)
    
    result = template_regex.sub(replacer, condition_str)
    logger.info(f"DEBUG: Final resolved condition: '{result}'")
    return result


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
    
    # Handle Name nodes (variable names like True, False, None, or other identifiers)
    elif isinstance(node, ast.Name):
        if node.id == "True":
            return True
        elif node.id == "False":
            return False
        elif node.id == "None":
            return None
        else:
            # For other names, treat them as string literals (for backward compatibility)
            return node.id
    
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