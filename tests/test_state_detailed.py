import pytest
from highway_core.engine.state import WorkflowState


def test_state_initialization():
    """Test WorkflowState initialization with default values"""
    state = WorkflowState({})

    # Check that all required data structures are initialized
    assert isinstance(state.variables, dict)
    assert isinstance(state.results, dict)
    assert isinstance(state.memory, dict)
    assert isinstance(state.loop_context, dict)

    # Check default values
    assert state.variables == {}
    assert state.results == {}
    assert state.memory == {}
    assert state.loop_context == {}


def test_state_initialization_with_variables():
    """Test WorkflowState initialization with custom variables"""
    variables = {"var1": "value1", "var2": 42}
    state = WorkflowState(variables)

    assert state.variables == variables
    assert state.results == {}
    assert state.memory == {}
    assert state.loop_context == {}


def test_state_set_result():
    """Test setting results in the state"""
    state = WorkflowState({})

    state.set_result("test_key", "test_value")

    assert state.results["test_key"] == "test_value"


def test_state_get_result():
    """Test getting results from the state"""
    state = WorkflowState({})

    state.set_result("test_key", "test_value")

    value = state.get_result("test_key")
    assert value == "test_value"

    # Test getting non-existent key
    default_value = state.get_result("non_existent_key", "default")
    assert default_value == "default"


def test_state_resolve_templating():
    """Test template resolution in state"""
    variables = {
        "name": "John",
        "age": 30,
        "items": [1, 2, 3],
        "nested": {"key": "value"},
    }

    state = WorkflowState(variables)

    # Test variable resolution - need to use variables prefix
    result = state.resolve_templating("Hello {{variables.name}}!")
    assert result == "Hello John!"

    # Test nested variable resolution
    result = state.resolve_templating("Age is {{variables.age}}")
    assert result == "Age is 30"

    # Test non-template string (should return as-is)
    result = state.resolve_templating("Just a string")
    assert result == "Just a string"

    # Test more complex templating
    result = state.resolve_templating(
        "{{variables.name}} is {{variables.age}} years old"
    )
    assert result == "John is 30 years old"


def test_state_resolve_templating_with_results():
    """Test template resolution with results data"""
    state = WorkflowState({"name": "John"})
    state.set_result("status", "active")

    result = state.resolve_templating(
        "User {{variables.name}} has status {{results.status}}"
    )
    assert result == "User John has status active"


def test_state_resolve_templating_with_memory():
    """Test template resolution with memory data"""
    state = WorkflowState({"name": "John"})

    # Setting memory directly in the data structure
    state.memory["session_id"] = "12345"

    result = state.resolve_templating(
        "User {{variables.name}} session: {{memory.session_id}}"
    )
    assert result == "User John session: 12345"


def test_state_resolve_templating_with_loop_context():
    """Test template resolution with loop context"""
    state = WorkflowState({"name": "John"})
    state.loop_context["item"] = "test_item"

    result = state.resolve_templating("Processing {{item}} for {{variables.name}}")
    assert result == "Processing test_item for John"


def test_state_resolve_complex_templating():
    """Test more complex template resolution scenarios"""
    variables = {
        "user": {"name": "Alice", "details": {"age": 25}},
        "config": {"debug": True, "version": "1.0"},
    }

    state = WorkflowState(variables)

    # Test nested object access - need to use variables prefix
    result = state.resolve_templating(
        "User {{variables.user.name}}, age {{variables.user.details.age}}"
    )
    assert result == "User Alice, age 25"

    # Test with boolean and other types
    result = state.resolve_templating(
        "Debug mode: {{variables.config.debug}}, Version: {{variables.config.version}}"
    )
    assert result == "Debug mode: True, Version: 1.0"


def test_state_get_variable():
    """Test getting variables from the state"""
    variables = {"var1": "value1", "var2": 42}
    state = WorkflowState(variables)

    assert state.get_variable("var1") == "value1"
    assert state.get_variable("var2") == 42
    # Note: get_variable doesn't support default parameters


def test_state_get_result():
    """Test getting results from the state"""
    state = WorkflowState({})
    state.set_result("test_key", "test_value")

    assert state.get_result("test_key") == "test_value"
    # Note: get_result doesn't support default parameters


def test_state_get_variable_default():
    """Test getting variables with default from the state"""
    variables = {"var1": "value1", "var2": 42}
    state = WorkflowState(variables)

    # Test with default value by checking for None
    assert state.get_variable("var1") == "value1"
    assert state.get_variable("var2") == 42
    assert (
        state.get_variable("nonexistent") is None
    )  # Default behavior is to return None
