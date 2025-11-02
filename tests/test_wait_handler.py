import pytest
from unittest.mock import MagicMock, patch
from highway_core.engine.operator_handlers.wait_handler import execute
from highway_core.engine.models import WaitOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
from highway_core.tools.bulkhead import BulkheadManager


def test_execute_wait_with_integer_seconds():
    """Test executing a wait task with integer seconds."""
    # Create a mock task
    task = WaitOperatorModel(
        task_id="wait_task",
        operator_type="wait",
        wait_for=0.1,  # 0.1 seconds to avoid long test times
        dependencies=[],
    )

    # Create a state
    state = WorkflowState({})

    # Create a mock registry
    registry = ToolRegistry()

    # Create a mock bulkhead manager
    bulkhead_manager = BulkheadManager()

    try:
        # Mock time.sleep to avoid actual waiting
        with patch("highway_core.engine.operator_handlers.wait_handler.time.sleep"):
            # Execute the wait handler
            execute(task, state, MagicMock(), registry, bulkhead_manager)
    finally:
        # Ensure bulkhead manager is properly shut down
        bulkhead_manager.shutdown_all()


def test_execute_wait_with_duration_seconds():
    """Test executing a wait task with duration string in seconds."""
    # Create a mock task
    task = WaitOperatorModel(
        task_id="wait_task",
        operator_type="wait",
        wait_for="duration:0.1s",  # 0.1 seconds to avoid long test times
        dependencies=[],
    )

    # Create a state
    state = WorkflowState({})

    # Create a mock registry
    registry = ToolRegistry()

    # Create a mock bulkhead manager
    bulkhead_manager = BulkheadManager()

    try:
        # Mock time.sleep to avoid actual waiting
        with patch("highway_core.engine.operator_handlers.wait_handler.time.sleep"):
            execute(task, state, MagicMock(), registry, bulkhead_manager)
    finally:
        # Ensure bulkhead manager is properly shut down
        bulkhead_manager.shutdown_all()


def test_execute_wait_with_duration_minutes():
    """Test executing a wait task with duration string in minutes."""
    # Create a mock task
    task = WaitOperatorModel(
        task_id="wait_task",
        operator_type="wait",
        wait_for="duration:0.05m",  # 0.05 minutes = 3 seconds, but we'll mock time.sleep
        dependencies=[],
    )

    # Create a state
    state = WorkflowState({})

    # Create a mock registry
    registry = ToolRegistry()

    # Create a mock bulkhead manager
    bulkhead_manager = BulkheadManager()

    try:
        # Mock time.sleep to avoid actual waiting
        with patch("highway_core.engine.operator_handlers.wait_handler.time.sleep"):
            execute(task, state, MagicMock(), registry, bulkhead_manager)
    finally:
        # Ensure bulkhead manager is properly shut down
        bulkhead_manager.shutdown_all()


def test_execute_wait_with_duration_hours():
    """Test executing a wait task with duration string in hours."""
    # Create a mock task
    task = WaitOperatorModel(
        task_id="wait_task",
        operator_type="wait",
        wait_for="duration:0.01h",  # 0.01 hours = 36 seconds, but we'll mock time.sleep
        dependencies=[],
    )

    # Create a state
    state = WorkflowState({})

    # Create a mock registry
    registry = ToolRegistry()

    # Create a mock bulkhead manager
    bulkhead_manager = BulkheadManager()

    try:
        # Mock time.sleep to avoid actual waiting
        with patch("highway_core.engine.operator_handlers.wait_handler.time.sleep"):
            execute(task, state, MagicMock(), registry, bulkhead_manager)
    finally:
        # Ensure bulkhead manager is properly shut down
        bulkhead_manager.shutdown_all()


def test_execute_wait_with_time_format():
    """Test executing a wait task with time format."""
    # Create a mock task
    task = WaitOperatorModel(
        task_id="wait_task",
        operator_type="wait",
        wait_for="time:12:00:00",  # 12:00:00 PM
        dependencies=[],
    )

    # Create a state
    state = WorkflowState({})

    # Create a mock registry
    registry = ToolRegistry()

    # Create a mock bulkhead manager
    bulkhead_manager = BulkheadManager()

    try:
        # Mock time.sleep to avoid actual waiting
        with patch("highway_core.engine.operator_handlers.wait_handler.time.sleep"):
            execute(task, state, MagicMock(), registry, bulkhead_manager)
    finally:
        # Ensure bulkhead manager is properly shut down
        bulkhead_manager.shutdown_all()


def test_execute_wait_with_unrecognized_format():
    """Test executing a wait task with unrecognized format."""
    # Create a mock task
    task = WaitOperatorModel(
        task_id="wait_task",
        operator_type="wait",
        wait_for="invalid_format",
        dependencies=[],
    )

    # Create a state
    state = WorkflowState({})

    # Create a mock registry
    registry = ToolRegistry()

    # Create a mock bulkhead manager
    bulkhead_manager = BulkheadManager()

    try:
        # Execute the wait handler (should proceed immediately)
        execute(task, state, MagicMock(), registry, bulkhead_manager)
    finally:
        # Ensure bulkhead manager is properly shut down
        bulkhead_manager.shutdown_all()


def test_execute_wait_with_non_numeric_string():
    """Test executing a wait task with non-numeric string after duration:."""
    # Create a mock task
    task = WaitOperatorModel(
        task_id="wait_task",
        operator_type="wait",
        wait_for="duration:invalid",
        dependencies=[],
    )

    # Create a state
    state = WorkflowState({})

    # Create a mock registry
    registry = ToolRegistry()

    # Create a mock bulkhead manager
    bulkhead_manager = BulkheadManager()

    try:
        # Execute the wait handler (should try to convert to float and fail gracefully)
        execute(task, state, MagicMock(), registry, bulkhead_manager)
    finally:
        # Ensure bulkhead manager is properly shut down
        bulkhead_manager.shutdown_all()


if __name__ == "__main__":
    pytest.main()
