import pytest
from highway_core.tools.decorators import tool, TOOL_REGISTRY


def test_tool_decorator_registration():
    """Test that the tool decorator properly registers functions."""
    # Clear the registry to start fresh
    TOOL_REGISTRY.clear()

    # Define a test function with the decorator
    @tool("test.tool.function")
    def test_function():
        return "test_result"

    # Verify the function was registered
    assert "test.tool.function" in TOOL_REGISTRY
    assert TOOL_REGISTRY["test.tool.function"] == test_function

    # Test that the function works as expected
    result = test_function()
    assert result == "test_result"


def test_tool_decorator_different_names():
    """Test that the tool decorator works with different names."""
    # Clear the registry
    TOOL_REGISTRY.clear()

    # Define multiple functions with different names
    @tool("tool.one")
    def func_one():
        return "result_one"

    @tool("tool.two")
    def func_two():
        return "result_two"

    # Verify both functions were registered with their respective names
    assert "tool.one" in TOOL_REGISTRY
    assert "tool.two" in TOOL_REGISTRY
    assert TOOL_REGISTRY["tool.one"] == func_one
    assert TOOL_REGISTRY["tool.two"] == func_two


def test_duplicate_tool_name():
    """Test that attempting to register a duplicate tool name raises an error."""
    # Clear the registry
    TOOL_REGISTRY.clear()

    # Register a function with a name
    @tool("duplicate.name")
    def first_function():
        return "first"

    # Try to register another function with the same name, should raise ValueError
    with pytest.raises(ValueError) as excinfo:

        @tool("duplicate.name")
        def second_function():
            return "second"

    assert "Duplicate tool name: duplicate.name" in str(excinfo.value)


def test_tool_decorator_with_args():
    """Test that the tool decorator works with functions that take arguments."""
    # Clear the registry
    TOOL_REGISTRY.clear()

    # Define a function with arguments
    @tool("math.add")
    def add_numbers(a, b):
        return a + b

    # Verify the function was registered
    assert "math.add" in TOOL_REGISTRY
    assert TOOL_REGISTRY["math.add"] == add_numbers

    # Test that the function works with arguments
    result = add_numbers(5, 3)
    assert result == 8


def test_tool_decorator_with_kwargs():
    """Test that the tool decorator works with functions that take keyword arguments."""
    # Clear the registry
    TOOL_REGISTRY.clear()

    # Define a function with keyword arguments
    @tool("data.process")
    def process_data(**kwargs):
        return f"Processed: {kwargs}"

    # Verify the function was registered
    assert "data.process" in TOOL_REGISTRY
    assert TOOL_REGISTRY["data.process"] == process_data

    # Test that the function works with keyword arguments
    result = process_data(name="test", value=42)
    assert result == "Processed: {'name': 'test', 'value': 42}"


def test_tool_registry_persistence():
    """Test that registered tools remain in the registry."""
    # Clear the registry
    TOOL_REGISTRY.clear()

    # Register a function
    @tool("persistent.tool")
    def persistent_function():
        return "persistent_result"

    # Verify it's in the registry
    assert "persistent.tool" in TOOL_REGISTRY

    # Call the function to ensure it still works
    result = TOOL_REGISTRY["persistent.tool"]()
    assert result == "persistent_result"


if __name__ == "__main__":
    pytest.main()
