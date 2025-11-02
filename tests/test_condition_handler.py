import pytest
from unittest.mock import MagicMock
from highway_core.engine.operator_handlers.condition_handler import (
    execute,
    eval_condition,
    _eval_node,
)
from highway_core.engine.models import ConditionOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
from highway_core.tools.bulkhead import BulkheadManager
from highway_core.engine.executors.base import BaseExecutor


def test_execute_condition_true():
    """Test executing a condition that evaluates to True."""
    # Create a mock task - using a simple condition that resolves to a boolean
    # This will test the state resolution and comparison functionality
    task = ConditionOperatorModel(
        task_id="condition_task",
        operator_type="condition",
        condition="'{{variables.test_var}}' == 'test_value'",
        if_true="task_a",
        if_false="task_b",
        dependencies=[],
    )

    # Create a state with a test variable
    state = WorkflowState({"test_var": "test_value"})

    # Create a mock orchestrator
    orchestrator = MagicMock()

    # Create a mock registry
    registry = ToolRegistry()

    # Create a mock bulkhead manager
    bulkhead_manager = MagicMock()

    # Create a mock executor
    executor = MagicMock()

    # Execute the condition handler
    execute(task, state, orchestrator, registry, bulkhead_manager, executor)

    # Check that the orchestrator's sorter.done() was called with the correct task id
    orchestrator.sorter.done.assert_called_once_with("task_b")


def test_execute_condition_false():
    """Test executing a condition that evaluates to False."""
    # Create a mock task
    task = ConditionOperatorModel(
        task_id="condition_task",
        operator_type="condition",
        condition="'{{variables.test_var}}' == 'different_value'",
        if_true="task_a",
        if_false="task_b",
        dependencies=[],
    )

    # Create a state with a test variable
    state = WorkflowState({"test_var": "test_value"})

    # Create a mock orchestrator
    orchestrator = MagicMock()

    # Create a mock registry
    registry = ToolRegistry()

    # Create a mock bulkhead manager
    bulkhead_manager = MagicMock()

    # Create a mock executor
    executor = MagicMock()

    # Execute the condition handler
    execute(task, state, orchestrator, registry, bulkhead_manager, executor)

    # Check that the orchestrator's sorter.done() was called with the correct task id
    orchestrator.sorter.done.assert_called_once_with("task_a")


def test_execute_condition_no_false_branch():
    """Test executing a condition with no false branch."""
    # Create a mock task with if_false as None
    # Use a more direct boolean condition
    task = ConditionOperatorModel(
        task_id="condition_task",
        operator_type="condition",
        condition="{{variables.test_condition}}",  # This will resolve to a Boolean value
        if_true="task_a",
        if_false=None,  # No false branch
        dependencies=[],
    )

    # Create a state with a test variable that is True
    state = WorkflowState({"test_condition": True})

    # Create a mock orchestrator
    orchestrator = MagicMock()

    # Create a mock registry
    registry = ToolRegistry()

    # Create a mock bulkhead manager
    bulkhead_manager = MagicMock()

    # Create a mock executor
    executor = MagicMock()

    # Execute the condition handler
    execute(task, state, orchestrator, registry, bulkhead_manager, executor)

    # Check that the orchestrator's sorter.done() was not called since there's no false branch to skip
    orchestrator.sorter.done.assert_not_called()


def test_eval_condition_true():
    """Test eval_condition function with a true condition."""
    result = eval_condition("200 == 200")
    assert result is True


def test_eval_condition_false():
    """Test eval_condition function with a false condition."""
    result = eval_condition("200 == 300")
    assert result is False


def test_eval_condition_with_boolean():
    """Test eval_condition function with boolean values."""
    result = eval_condition("True")
    assert result is True

    result = eval_condition("False")
    assert result is False


def test_eval_condition_invalid_syntax():
    """Test eval_condition function with invalid syntax."""
    result = eval_condition("invalid syntax")
    assert result is False


def test_eval_node_constant():
    """Test _eval_node function with constant values."""
    import ast

    # Test with string constant
    node = ast.parse("'hello'", mode="eval").body
    result = _eval_node(node)
    assert result == "hello"

    # Test with number constant
    node = ast.parse("42", mode="eval").body
    result = _eval_node(node)
    assert result == 42

    # Test with boolean constant
    node = ast.parse("True", mode="eval").body
    result = _eval_node(node)
    assert result is True


def test_eval_node_comparison():
    """Test _eval_node function with comparison operations."""
    import ast

    # Test equality
    node = ast.parse("200 == 200", mode="eval").body
    result = _eval_node(node)
    assert result is True

    # Test inequality
    node = ast.parse("200 != 300", mode="eval").body
    result = _eval_node(node)
    assert result is True

    # Test less than
    node = ast.parse("2 < 3", mode="eval").body
    result = _eval_node(node)
    assert result is True

    # Test greater than
    node = ast.parse("5 > 3", mode="eval").body
    result = _eval_node(node)
    assert result is True


def test_eval_node_boolean_ops():
    """Test _eval_node function with boolean operations."""
    import ast

    # Test AND operation
    node = ast.parse("True and True", mode="eval").body
    result = _eval_node(node)
    assert result is True

    # Test OR operation
    node = ast.parse("True or False", mode="eval").body
    result = _eval_node(node)
    assert result is True

    # Test AND with one False
    node = ast.parse("True and False", mode="eval").body
    result = _eval_node(node)
    assert result is False


def test_eval_node_unary_ops():
    """Test _eval_node function with unary operations."""
    import ast

    # Test NOT operation
    node = ast.parse("not False", mode="eval").body
    result = _eval_node(node)
    assert result is True

    # Test unary minus
    node = ast.parse("-5", mode="eval").body
    result = _eval_node(node)
    assert result == -5

    # Test unary plus
    node = ast.parse("+5", mode="eval").body
    result = _eval_node(node)
    assert result == 5


def test_eval_node_unsupported_op():
    """Test _eval_node function with unsupported operations."""
    import ast

    # Create an unsupported operation (e.g., binary operation like addition)
    node = ast.parse("1 + 2", mode="eval").body

    with pytest.raises(ValueError):
        _eval_node(node)


if __name__ == "__main__":
    pytest.main()
