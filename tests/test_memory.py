"""Unit tests for the memory tool functionality."""

import pytest

from highway_core.engine.state import WorkflowState
from highway_core.tools.memory import add_memory, increment_memory, set_memory


def test_set_memory_basic():
    """Test basic set_memory functionality."""
    state = WorkflowState({"initial": "value"})

    result = set_memory(state, "test_key", "test_value")

    assert result == {"key": "test_key", "status": "ok"}
    assert state.memory["test_key"] == "test_value"


def test_set_memory_override():
    """Test set_memory overwriting existing value."""
    state = WorkflowState({"initial": "value"})
    state.memory["existing_key"] = "old_value"

    result = set_memory(state, "existing_key", "new_value")

    assert result == {"key": "existing_key", "status": "ok"}
    assert state.memory["existing_key"] == "new_value"


def test_increment_memory_new_key():
    """Test increment_memory with a new key (should start at 0)."""
    state = WorkflowState({})

    result = increment_memory(state, "counter")

    assert result == {"key": "counter", "new_value": 1}
    assert state.memory["counter"] == 1


def test_increment_memory_existing_key():
    """Test increment_memory with existing numeric value."""
    state = WorkflowState({})
    state.memory["counter"] = 5

    result = increment_memory(state, "counter")

    assert result == {"key": "counter", "new_value": 6}
    assert state.memory["counter"] == 6


def test_increment_memory_non_numeric_value():
    """Test increment_memory with non-numeric value (should reset to 1)."""
    state = WorkflowState({})
    state.memory["counter"] = "not_a_number"

    result = increment_memory(state, "counter")

    assert result == {"key": "counter", "new_value": 1}
    assert state.memory["counter"] == 1


def test_increment_memory_float_value():
    """Test increment_memory with float value."""
    state = WorkflowState({})
    state.memory["counter"] = 5.5

    result = increment_memory(state, "counter")

    assert result == {"key": "counter", "new_value": 6.5}
    assert state.memory["counter"] == 6.5


def test_add_memory_simple_integer():
    """Test add_memory with simple integer value."""
    state = WorkflowState({})
    state.memory["test_key"] = 10

    result = add_memory(state, "test_key", 5)

    assert result == {
        "key": "test_key",
        "old_value": 10,
        "new_value": 15,
        "status": "ok",
    }
    assert state.memory["test_key"] == 15


def test_add_memory_new_key():
    """Test add_memory with a new key."""
    state = WorkflowState({})

    result = add_memory(state, "new_key", 7)

    assert result == {
        "key": "new_key",
        "old_value": 0,
        "new_value": 7,
        "status": "ok",
    }
    assert state.memory["new_key"] == 7


def test_add_memory_string_expression():
    """Test add_memory with string expression containing '+'."""
    state = WorkflowState({})
    state.memory["test_key"] = 10

    result = add_memory(state, "test_key", "3+2")

    assert result == {
        "key": "test_key",
        "old_value": 10,
        "new_value": 15,
        "status": "ok",
    }
    assert state.memory["test_key"] == 15


def test_add_memory_string_expression_with_non_numeric():
    """Test add_memory with string expression containing non-numeric parts."""
    state = WorkflowState({})

    result = add_memory(state, "test_key", "abc+def")  # Should result in error

    assert result == {"key": "test_key", "status": "error"}


def test_add_memory_non_numeric_current_value():
    """Test add_memory when current value is non-numeric."""
    state = WorkflowState({})
    state.memory["test_key"] = "not_a_number"

    result = add_memory(state, "test_key", 5)

    assert result == {
        "key": "test_key",
        "old_value": 0,  # Non-numeric defaults to 0
        "new_value": 5,
        "status": "ok",
    }
    assert state.memory["test_key"] == 5


def test_add_memory_with_templated_string():
    """Test add_memory with templated string (should be resolved by state)."""
    state = WorkflowState({})
    state.memory["counter"] = 10

    # This simulates a template, but it would need to be resolved by the state first
    # In a real scenario, the templating would be resolved before calling add_memory
    result = add_memory(state, "test_key", "5")

    assert result["status"] == "ok"
    assert result["new_value"] == 5  # Since initial value defaults to 0, 0 + 5 = 5


def test_add_memory_string_with_plus_non_numeric():
    """Test add_memory with string containing '+' but non-numeric parts."""
    state = WorkflowState({})

    result = add_memory(state, "test_key", "hello+world")

    # This should try to evaluate as an expression and fail, returning error
    assert result == {"key": "test_key", "status": "error"}


def test_add_memory_invalid_numeric_conversion():
    """Test add_memory when value can't be converted to integer."""
    state = WorkflowState({})

    result = add_memory(state, "test_key", "not_numeric")

    assert result == {
        "key": "test_key",
        "old_value": 0,
        "new_value": 0,  # Defaults to 0 when conversion fails
        "status": "ok",
    }
    assert state.memory["test_key"] == 0


def test_add_memory_mixed_expression():
    """Test add_memory with mixed numeric and string expression."""
    state = WorkflowState({})
    state.memory["test_key"] = 5

    result = add_memory(state, "test_key", "3+4+2")

    assert result == {
        "key": "test_key",
        "old_value": 5,
        "new_value": 14,  # 5 + (3+4+2)
        "status": "ok",
    }
    assert state.memory["test_key"] == 14


def test_add_memory_empty_string():
    """Test add_memory with empty string value."""
    state = WorkflowState({})

    result = add_memory(state, "test_key", "")

    # Empty string should convert to 0
    assert result == {
        "key": "test_key",
        "old_value": 0,
        "new_value": 0,
        "status": "ok",
    }
    assert state.memory["test_key"] == 0


def test_add_memory_none_value():
    """Test add_memory with None value."""
    state = WorkflowState({})

    result = add_memory(state, "test_key", None)

    # None should convert to 0
    assert result == {
        "key": "test_key",
        "old_value": 0,
        "new_value": 0,
        "status": "ok",
    }
    assert state.memory["test_key"] == 0


def test_add_memory_negative_numbers():
    """Test add_memory with negative numbers."""
    state = WorkflowState({})
    state.memory["test_key"] = 10

    result = add_memory(state, "test_key", -3)

    assert result == {
        "key": "test_key",
        "old_value": 10,
        "new_value": 7,
        "status": "ok",
    }
    assert state.memory["test_key"] == 7


def test_add_memory_zero_value():
    """Test add_memory with zero value."""
    state = WorkflowState({})
    state.memory["test_key"] = 10

    result = add_memory(state, "test_key", 0)

    # Adding zero should keep the value the same
    assert result == {
        "key": "test_key",
        "old_value": 10,
        "new_value": 10,
        "status": "ok",
    }
    assert state.memory["test_key"] == 10
