import graphlib
from unittest.mock import MagicMock

import pytest

import highway_core.tools.memory as memory_tools
from highway_core.engine.models import TaskOperatorModel, WhileOperatorModel
from highway_core.engine.operator_handlers.while_handler import execute
from highway_core.engine.orchestrator import Orchestrator
from highway_core.engine.state import WorkflowState
from highway_core.tools.bulkhead import BulkheadManager
from highway_core.tools.registry import ToolRegistry


@pytest.fixture
def mock_orchestrator():
    """Mock orchestrator for testing"""

    class MockOrchestrator:
        def __init__(self):
            self.sorter = graphlib.TopologicalSorter({})
            self.sorter.prepare()
            self.sorter.done = lambda x: True  # Mock done method

    return MockOrchestrator()


@pytest.fixture
def mock_state():
    """Mock state for testing"""
    state = WorkflowState({"counter": 0})
    return state


@pytest.fixture
def mock_registry():
    """Mock registry for testing"""
    registry = ToolRegistry()  # This will automatically discover and load all tools
    registry.functions["tools.memory.add"] = memory_tools.add_memory
    return registry


@pytest.fixture
def mock_bulkhead_manager():
    """Mock bulkhead manager for testing"""
    return BulkheadManager()


def test_while_handler_execute_cond_true(
    mock_orchestrator, mock_state, mock_registry, mock_bulkhead_manager
):
    """Test while handler executes when condition is true"""
    loop_task = TaskOperatorModel(
        task_id="loop_task",
        operator_type="task",
        function="tools.memory.add",
        args=["counter", 1],
        dependencies=[],
    )

    mock_orchestrator.workflow = MagicMock()
    mock_orchestrator.workflow.tasks = {"loop_task": loop_task}

    task = WhileOperatorModel(
        task_id="while_test",
        operator_type="while",
        condition="{{variables.counter < 3}}",
        loop_body=[loop_task.model_dump()],
    )

    # Execute the while handler
    execute(
        task,
        mock_state,
        mock_orchestrator,
        mock_registry,
        mock_bulkhead_manager,
    )


def test_while_handler_execute_cond_false(
    mock_orchestrator, mock_state, mock_registry, mock_bulkhead_manager
):
    """Test while handler exits when condition is false"""
    # Set counter to 5 so condition should be false
    mock_state.set_variable("counter", 5)

    loop_task = TaskOperatorModel(
        task_id="loop_task",
        operator_type="task",
        function="tools.memory.add",
        args=["counter", 1],
        dependencies=[],
    )

    mock_orchestrator.workflow = MagicMock()
    mock_orchestrator.workflow.tasks = {"loop_task": loop_task}

    task = WhileOperatorModel(
        task_id="while_test",
        operator_type="while",
        condition="{{variables.counter < 3}}",
        loop_body=[loop_task.model_dump()],
    )

    # Execute the while handler
    execute(
        task,
        mock_state,
        mock_orchestrator,
        mock_registry,
        mock_bulkhead_manager,
    )


def test_while_handler_execute_with_templating(
    mock_orchestrator, mock_state, mock_registry, mock_bulkhead_manager
):
    """Test while handler with templating in condition"""
    mock_state.set_variable("max_val", 3)
    mock_state.set_variable("counter", 0)

    loop_task = TaskOperatorModel(
        task_id="loop_task",
        operator_type="task",
        function="tools.memory.add",
        args=["counter", 1],
        dependencies=[],
    )

    mock_orchestrator.workflow = MagicMock()
    mock_orchestrator.workflow.tasks = {"loop_task": loop_task}

    task = WhileOperatorModel(
        task_id="while_test",
        operator_type="while",
        condition="{{variables.counter < variables.max_val}}",
        loop_body=[loop_task.model_dump()],
    )

    # Execute the while handler
    execute(
        task,
        mock_state,
        mock_orchestrator,
        mock_registry,
        mock_bulkhead_manager,
    )
