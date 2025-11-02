import pytest
from unittest.mock import MagicMock, patch
from highway_core.engine.operator_handlers.task_handler import execute
from highway_core.engine.models import TaskOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
from highway_core.tools.bulkhead import BulkheadManager


def test_execute_task_with_bulkhead():
    """Test executing a task with bulkhead isolation."""
    # Create a mock task
    task = TaskOperatorModel(
        task_id="test_task",
        operator_type="task",
        function="tools.log.info",
        args=["Hello, World!"],
        result_key="test_result",
        dependencies=[],
    )

    # Create a state
    state = WorkflowState({})

    # Create a mock registry
    registry = ToolRegistry()
    registry.functions["tools.log.info"] = lambda msg: {"result": msg}

    # Create a mock bulkhead manager
    bulkhead_manager = BulkheadManager()

    try:
        # Create a mock orchestrator
        orchestrator = MagicMock()

        # Create a mock executor
        executor = MagicMock()

        # Execute the task handler
        execute(task, state, orchestrator, registry, bulkhead_manager, executor)

        # Verify that the result was set in the state
        assert state.get_result("test_result") is not None
    finally:
        # Ensure bulkhead manager is properly shut down
        bulkhead_manager.shutdown_all()


def test_execute_task_without_bulkhead():
    """Test executing a task without bulkhead isolation."""
    # Create a mock task
    task = TaskOperatorModel(
        task_id="test_task",
        operator_type="task",
        function="tools.log.info",
        args=["Hello, World!"],
        result_key="test_result",
        dependencies=[],
    )

    # Create a state
    state = WorkflowState({})

    # Create a mock registry
    registry = ToolRegistry()
    registry.functions["tools.log.info"] = lambda msg: {"result": msg}

    # Create a mock orchestrator
    orchestrator = MagicMock()

    # Create a mock executor
    executor = MagicMock()

    # Execute the task handler without bulkhead manager
    execute(task, state, orchestrator, registry, None, executor)

    # Verify that the result was set in the state
    assert state.get_result("test_result") is not None


def test_execute_task_with_missing_function():
    """Test executing a task with a function that doesn't exist in the registry."""
    # Create a mock task
    task = TaskOperatorModel(
        task_id="test_task",
        operator_type="task",
        function="nonexistent.tool",
        args=["Hello, World!"],
        result_key=None,
        dependencies=[],
    )

    # Create a state
    state = WorkflowState({})

    # Create a mock registry
    registry = ToolRegistry()

    # Create a mock orchestrator
    orchestrator = MagicMock()

    # Create a mock executor that calls the registry.get_function (which will raise KeyError)
    executor = MagicMock()

    # Set up the execute method to call registry.get_function, which will fail for missing function
    def executor_execute(task, state, registry, **kwargs):
        # This will trigger the KeyError from the registry
        func = registry.get_function(task.function)
        return func(*task.args, **task.kwargs)

    executor.execute = executor_execute

    # Execute the task handler and expect a KeyError
    with pytest.raises(KeyError):
        execute(task, state, orchestrator, registry, None, executor)


def test_execute_task_with_templating_resolution():
    """Test executing a task with templating resolution."""
    # Create a mock task with templated args
    task = TaskOperatorModel(
        task_id="test_task",
        operator_type="task",
        function="tools.log.info",
        args=["Value is: {{variables.test_var}}"],
        result_key="test_result",
        dependencies=[],
    )

    # Create a state with variables
    state = WorkflowState({"test_var": "resolved_value"})

    # Create a mock registry
    registry = ToolRegistry()
    registry.functions["tools.log.info"] = lambda msg: {"result": msg}

    # Create a mock bulkhead manager
    bulkhead_manager = BulkheadManager()

    # Create a mock orchestrator
    orchestrator = MagicMock()

    try:
        # Create a mock executor
        executor = MagicMock()

        # Execute the task handler
        execute(task, state, orchestrator, registry, bulkhead_manager, executor)

        # Verify that the result was set in the state
        result = state.get_result("test_result")
        assert result is not None
    finally:
        # Ensure bulkhead manager is properly shut down
        bulkhead_manager.shutdown_all()


def test_execute_task_memory_set():
    """Test executing a task that is tools.memory.set."""
    # Create a mock task for tools.memory.set
    task = TaskOperatorModel(
        task_id="test_task",
        operator_type="task",
        function="tools.memory.set",
        args=["key1", "value1"],
        result_key="test_result",
        dependencies=[],
    )

    # Create a state
    state = WorkflowState({})

    # Create a mock registry with tools.memory.set
    registry = ToolRegistry()

    # Mock the memory.set function
    def mock_memory_set(state_obj, key, value):
        state_obj.set_variable(key, value)
        return {"result": f"Set {key} to {value}"}

    registry.functions["tools.memory.set"] = mock_memory_set

    # Create a mock bulkhead manager
    bulkhead_manager = BulkheadManager()

    try:
        # Create a mock orchestrator
        orchestrator = MagicMock()

        # Create a mock executor that actually executes the function
        executor = MagicMock()

        # Set up the execute method to actually run the function
        def executor_execute(task, state, registry, **kwargs):
            # Get the function from the registry and call it
            func = registry.get_function(task.function)
            return func(state, *task.args, **task.kwargs)

        executor.execute = executor_execute

        # Execute the task handler
        execute(task, state, orchestrator, registry, bulkhead_manager, executor)

        # Verify that the memory was set in the state
        assert state.get_variable("key1") == "value1"
    finally:
        # Ensure bulkhead manager is properly shut down
        bulkhead_manager.shutdown_all()


def test_execute_task_with_kwargs():
    """Test executing a task with keyword arguments."""
    # Create a mock task with kwargs
    task = TaskOperatorModel(
        task_id="test_task",
        operator_type="task",
        function="tools.log.info",
        args=[],
        kwargs={"message": "Hello", "level": "INFO"},
        result_key="test_result",
        dependencies=[],
    )

    # Create a state
    state = WorkflowState({})

    # Create a mock registry
    registry = ToolRegistry()
    registry.functions["tools.log.info"] = lambda message, level: {
        "result": f"{level}: {message}"
    }

    # Create a mock bulkhead manager
    bulkhead_manager = BulkheadManager()

    try:
        # Create a mock orchestrator
        orchestrator = MagicMock()

        # Create a mock executor
        executor = MagicMock()

        # Execute the task handler
        execute(task, state, orchestrator, registry, bulkhead_manager, executor)

        # Verify that the result was set in the state
        result = state.get_result("test_result")
        assert result is not None
    finally:
        # Ensure bulkhead manager is properly shut down
        bulkhead_manager.shutdown_all()


def test_execute_task_exception_in_bulkhead():
    """Test handling an exception during bulkhead execution."""
    # Create a mock task
    task = TaskOperatorModel(
        task_id="test_task",
        operator_type="task",
        function="tools.log.info",
        args=["Hello, World!"],
        result_key="test_result",
        dependencies=[],
    )

    # Create a state
    state = WorkflowState({})

    # Create a mock registry with a function that raises an exception
    registry = ToolRegistry()
    registry.functions["tools.log.info"] = lambda msg: (_ for _ in ()).throw(
        Exception("Test exception")
    )

    # Create a mock bulkhead manager
    bulkhead_manager = BulkheadManager()

    try:
        # Create a mock orchestrator
        orchestrator = MagicMock()

        # Create a mock executor that actually executes the function
        executor = MagicMock()

        # Set up the execute method to actually run the function
        def executor_execute(task, state, registry, **kwargs):
            # Get the function from the registry and call it
            func = registry.get_function(task.function)
            return func(*task.args, **task.kwargs)

        executor.execute = executor_execute

        # Execute the task handler and expect an exception
        with pytest.raises(Exception) as excinfo:
            execute(task, state, orchestrator, registry, bulkhead_manager, executor)
        assert "Test exception" in str(excinfo.value)
    finally:
        # Ensure bulkhead manager is properly shut down
        bulkhead_manager.shutdown_all()


if __name__ == "__main__":
    pytest.main()
