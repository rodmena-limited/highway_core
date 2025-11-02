import pytest
from highway_core.engine.state import WorkflowState


def test_workflow_state_initialization():
    """Test WorkflowState initialization."""
    initial_vars = {"var1": "value1", "var2": 42}
    state = WorkflowState(initial_vars)

    # Verify initialization
    assert state.variables == initial_vars
    assert state.results == {}
    assert state.memory == {}
    assert state.loop_context == {}


def test_set_result():
    """Test setting a result in the state."""
    state = WorkflowState({})

    # Set a result
    state.set_result("test_key", {"data": "test_value"})

    # Verify the result was set
    assert state.results["test_key"] == {"data": "test_value"}


def test_get_value_variables():
    """Test getting a value from variables."""
    state = WorkflowState(
        {"var1": "value1", "nested": {"key1": "val1", "key2": "val2"}}
    )

    # Get a simple variable
    result = state._get_value("variables.var1")
    assert result == "value1"

    # Get a nested variable
    result = state._get_value("variables.nested.key1")
    assert result == "val1"


def test_get_value_results():
    """Test getting a value from results."""
    state = WorkflowState({})

    # Set a result first
    state.set_result("test_result", {"status": "success", "data": {"id": 123}})

    # Get the result
    result = state._get_value("results.test_result")
    assert result == {"status": "success", "data": {"id": 123}}

    # Get a nested result
    result = state._get_value("results.test_result.data.id")
    assert result == 123


def test_get_value_memory():
    """Test getting a value from memory."""
    state = WorkflowState({})

    # Set a value in memory directly
    state.memory["memory_key"] = "memory_value"

    # Get the memory value
    result = state._get_value("memory.memory_key")
    assert result == "memory_value"

    # Get a nested memory value
    state.memory["nested"] = {"key1": "val1", "key2": "val2"}
    result = state._get_value("memory.nested.key2")
    assert result == "val2"


def test_get_value_loop_context_item():
    """Test getting a value from loop_context item."""
    state = WorkflowState({})

    # Set an item in loop context
    state.loop_context["item"] = "loop_item_value"

    # Get the item
    result = state._get_value("item")
    assert result == "loop_item_value"


def test_get_value_invalid_path():
    """Test getting a value with an invalid path."""
    state = WorkflowState({"var1": "value1"})

    # Try to get a value with an invalid path
    result = state._get_value("invalid.path")
    assert result is None


def test_get_value_nonexistent_variable():
    """Test getting a value that doesn't exist."""
    state = WorkflowState({"var1": "value1"})

    # Try to get a variable that doesn't exist
    result = state._get_value("variables.nonexistent")
    assert result is None


def test_resolve_templating_string():
    """Test resolving templating in a string."""
    state = WorkflowState({"test_var": "test_value"})

    # Set a result to test results path
    state.set_result("test_result", {"status": "success"})

    # Resolve a templated string
    result = state.resolve_templating("{{variables.test_var}}")
    assert result == "test_value"

    # Resolve a template within a larger string
    result = state.resolve_templating("Status is: {{results.test_result.status}}")
    assert result == "Status is: success"

    # Resolve a template with non-existent variable (should return original string)
    result = state.resolve_templating("{{variables.nonexistent}}")
    assert result == "{{variables.nonexistent}}"


def test_resolve_templating_list():
    """Test resolving templating in a list."""
    state = WorkflowState({"var1": "value1", "var2": "value2"})

    # Resolve templating in a list
    input_list = ["{{variables.var1}}", "static_value", "{{variables.var2}}"]
    result = state.resolve_templating(input_list)
    expected = ["value1", "static_value", "value2"]
    assert result == expected


def test_resolve_templating_dict():
    """Test resolving templating in a dictionary."""
    state = WorkflowState({"var1": "value1", "var2": "value2"})

    # Set a result to test nested resolution
    state.set_result("user_data", {"name": "test_user", "id": 123})

    # Resolve templating in a dictionary
    input_dict = {
        "name": "{{variables.var1}}",
        "value": "static",
        "id": "{{results.user_data.id}}",
    }
    result = state.resolve_templating(input_dict)
    expected = {"name": "value1", "value": "static", "id": 123}
    assert result == expected


def test_resolve_templating_nested_dict():
    """Test resolving templating in nested dictionaries."""
    state = WorkflowState({"outer_var": {"inner_var": "nested_value"}})

    # Set nested results
    state.set_result("nested_result", {"level1": {"level2": "deep_value"}})

    # Resolve templating in nested dictionaries
    input_dict = {
        "outer": {"inner": "{{variables.outer_var.inner_var}}", "static": "value"},
        "result": "{{results.nested_result.level1.level2}}",
    }
    result = state.resolve_templating(input_dict)
    expected = {
        "outer": {"inner": "nested_value", "static": "value"},
        "result": "deep_value",
    }
    assert result == expected


def test_resolve_templating_non_templatable_types():
    """Test resolving templating with non-templatable types."""
    state = WorkflowState({})

    # Pass non-templatable types, should return as-is
    result = state.resolve_templating(42)
    assert result == 42

    result = state.resolve_templating(3.14)
    assert result == 3.14

    result = state.resolve_templating(True)
    assert result is True

    result = state.resolve_templating(None)
    assert result is None


if __name__ == "__main__":
    pytest.main()
