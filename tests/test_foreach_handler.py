import pytest
from highway_core.engine.operator_handlers.foreach_handler import execute
from highway_core.engine.common import ForEachOperatorModel, TaskOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
from highway_core.tools.bulkhead import BulkheadManager
from highway_core.engine.orchestrator import Orchestrator
from unittest.mock import MagicMock
import graphlib


import highway_core.tools.memory as memory_tools


@pytest.fixture
def mock_orchestrator():
    """Mock orchestrator for testing"""

    class MockExecutor:
        def submit(self, func, *args, **kwargs):
            # Execute the function directly for testing purposes
            from concurrent.futures import Future

            future = Future()
            try:
                result = func(*args, **kwargs)
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)
            return future

    class MockOrchestrator:
        def __init__(self):
            self.sorter = graphlib.TopologicalSorter({})
            self.sorter.prepare()
            self.sorter.done = lambda x: True  # Mock done method
            self.executor = MockExecutor()  # Add the executor
            self.registry = ToolRegistry()  # Add the registry
            self.bulkhead_manager = BulkheadManager()  # Add the bulkhead manager

    return MockOrchestrator()


@pytest.fixture
def mock_state():
    """Mock state for testing"""
    state = WorkflowState({})
    return state


@pytest.fixture
def mock_registry():
    """Mock registry for testing"""
    registry = ToolRegistry()  # This will automatically discover and load all tools
    registry.functions["tools.memory.set"] = memory_tools.set_memory
    return registry


@pytest.fixture
def mock_bulkhead_manager():
    """Mock bulkhead manager for testing"""
    return BulkheadManager()


def test_foreach_handler_execute_with_items(
    mock_orchestrator, mock_state, mock_registry, mock_bulkhead_manager
):
    mock_state.set_variable("items", [1, 2, 3])

    loop_task = TaskOperatorModel(
        task_id="loop_task",
        operator_type="task",
        function="tools.memory.set",
        args=["current_item", "{{item}}"],
        dependencies=[],
    )

    mock_orchestrator.workflow = MagicMock()
    mock_orchestrator.workflow.tasks = {"loop_task": loop_task}

    task = ForEachOperatorModel(
        task_id="foreach_test",
        operator_type="foreach",
        items="{{variables.items}}",
        loop_body=[loop_task.model_dump()],
    )

    # Execute the foreach handler.
    execute(task, mock_state, mock_orchestrator, mock_registry, mock_bulkhead_manager)


def test_foreach_handler_execute_with_empty_list(
    mock_orchestrator, mock_state, mock_registry, mock_bulkhead_manager
):
    mock_state.set_variable("items", [])

    loop_task = TaskOperatorModel(
        task_id="loop_task",
        operator_type="task",
        function="tools.memory.set",
        args=["current_item", "{{item}}"],
        dependencies=[],
    )

    mock_orchestrator.workflow = MagicMock()
    mock_orchestrator.workflow.tasks = {"loop_task": loop_task}

    task = ForEachOperatorModel(
        task_id="foreach_test",
        operator_type="foreach",
        items="{{variables.items}}",
        loop_body=[loop_task.model_dump()],
    )

    # Execute the foreach handler
    execute(task, mock_state, mock_orchestrator, mock_registry, mock_bulkhead_manager)


def test_foreach_handler_execute_with_string_items(
    mock_orchestrator, mock_state, mock_registry, mock_bulkhead_manager
):
    """Test foreach handler with string representation of list"""
    # Testing with string list
    mock_state.set_variable("items", ["a", "b", "c"])

    loop_task = TaskOperatorModel(
        task_id="loop_task",
        operator_type="task",
        function="tools.memory.set",
        args=["current_item", "{{item}}"],
        dependencies=[],
    )

    mock_orchestrator.workflow = MagicMock()
    mock_orchestrator.workflow.tasks = {"loop_task": loop_task}

    task = ForEachOperatorModel(
        task_id="foreach_test",
        operator_type="foreach",
        items="{{variables.items}}",
        loop_body=[loop_task.model_dump()],
    )

    # Execute the foreach handler.
    execute(task, mock_state, mock_orchestrator, mock_registry, mock_bulkhead_manager)
