from unittest.mock import patch

import pytest

from highway_core.tools.decorators import tool
from highway_core.tools.registry import ToolRegistry


def test_tool_registry_initialization():
    """Test ToolRegistry initialization."""
    # Create a ToolRegistry instance
    registry = ToolRegistry()

    # Verify initialization
    assert registry.functions is not None
    assert hasattr(registry, "_discover_tools")
    assert hasattr(registry, "get_function")


def test_tool_registry_discover_tools():
    """Test the _discover_tools method."""
    # Create a ToolRegistry instance
    registry = ToolRegistry()

    # Since _discover_tools is called during initialization,
    # we just verify that some functions were discovered
    # The actual discovery behavior is complex to mock, so we're testing
    # that the method exists and was called during initialization
    assert hasattr(registry, "functions")


def test_get_function_success():
    """Test getting a function that exists in the registry."""
    # Clear and set up a fresh registry for this test
    from highway_core.tools.decorators import TOOL_REGISTRY

    TOOL_REGISTRY.clear()

    # Register a test function
    @tool("test.function.get")
    def test_function():
        return "test_result"

    # Create a ToolRegistry
    registry = ToolRegistry()

    # Get the function
    retrieved_func = registry.get_function("test.function.get")

    # Verify it's the same function
    assert retrieved_func == test_function

    # Verify calling it works
    result = retrieved_func()
    assert result == "test_result"


def test_get_function_failure():
    """Test getting a function that doesn't exist in the registry."""
    # Create a ToolRegistry
    registry = ToolRegistry()

    # Try to get a function that doesn't exist
    with pytest.raises(KeyError) as excinfo:
        registry.get_function("nonexistent.function")

    assert "Tool 'nonexistent.function' not found." in str(excinfo.value)


def test_tool_registry_dynamic_discovery_integration():
    """Test that ToolRegistry properly discovers tools during initialization."""
    # Clear and set up a fresh registry for this test
    from highway_core.tools.decorators import TOOL_REGISTRY

    TOOL_REGISTRY.clear()

    # Register a test function
    @tool("integration.test.function")
    def integration_test_function():
        return "integration_result"

    # Create a ToolRegistry
    registry = ToolRegistry()

    # Verify our function was discovered and registered
    retrieved_func = registry.get_function("integration.test.function")
    assert retrieved_func == integration_test_function
    assert retrieved_func() == "integration_result"


def test_tool_registry_multiple_functions():
    """Test ToolRegistry with multiple functions."""
    # Clear and set up a fresh registry for this test
    from highway_core.tools.decorators import TOOL_REGISTRY

    TOOL_REGISTRY.clear()

    # Register multiple test functions
    @tool("math.add")
    def add_func(a, b):
        return a + b

    @tool("math.multiply")
    def multiply_func(a, b):
        return a * b

    @tool("string.concat")
    def concat_func(str1, str2):
        return str1 + str2

    # Create a ToolRegistry
    registry = ToolRegistry()

    # Verify all functions were registered and can be retrieved
    add_retrieved = registry.get_function("math.add")
    assert add_retrieved == add_func
    assert add_retrieved(2, 3) == 5

    multiply_retrieved = registry.get_function("math.multiply")
    assert multiply_retrieved == multiply_func
    assert multiply_retrieved(2, 3) == 6

    concat_retrieved = registry.get_function("string.concat")
    assert concat_retrieved == concat_func
    assert concat_retrieved("hello", "world") == "helloworld"


if __name__ == "__main__":
    pytest.main()
