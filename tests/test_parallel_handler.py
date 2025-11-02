import pytest
from unittest.mock import MagicMock
from highway_core.engine.operator_handlers.parallel_handler import execute
from highway_core.engine.common import ParallelOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
from highway_core.tools.bulkhead import BulkheadManager


def test_execute_parallel_with_empty_branch():
    """Test executing a parallel operator with an empty branch."""
    # Create a mock task with an empty branch
    task = ParallelOperatorModel(
        task_id="parallel_task",
        operator_type="parallel",
        branches={
            "branch_1": ["task_a"],
            "branch_2": [],  # Empty branch
            "branch_3": ["task_b", "task_c"],
        },
        dependencies=[],
    )

    # Create a state
    state = WorkflowState({})

    # Create a mock registry
    registry = ToolRegistry()

    # Create a mock bulkhead manager
    bulkhead_manager = MagicMock()

    # Execute the parallel handler
    execute(task, state, registry, bulkhead_manager)


def test_execute_parallel_without_empty_branches():
    """Test executing a parallel operator without any empty branches."""
    # Create a mock task with no empty branches
    task = ParallelOperatorModel(
        task_id="parallel_task",
        operator_type="parallel",
        branches={
            "branch_1": ["task_a"],
            "branch_2": ["task_b", "task_c"],
            "branch_3": ["task_d"],
        },
        dependencies=[],
    )

    # Create a state
    state = WorkflowState({})

    # Create a mock registry
    registry = ToolRegistry()

    # Create a mock bulkhead manager
    bulkhead_manager = MagicMock()

    # Execute the parallel handler
    execute(task, state, registry, bulkhead_manager)


def test_execute_parallel_with_all_empty_branches():
    """Test executing a parallel operator with all branches being empty."""
    # Create a mock task with all empty branches
    task = ParallelOperatorModel(
        task_id="parallel_task",
        operator_type="parallel",
        branches={"branch_1": [], "branch_2": [], "branch_3": []},
        dependencies=[],
    )

    # Create a state
    state = WorkflowState({})

    # Create a mock registry
    registry = ToolRegistry()

    # Create a mock bulkhead manager
    bulkhead_manager = MagicMock()

    # Execute the parallel handler
    execute(task, state, registry, bulkhead_manager)


def test_execute_parallel_with_none_branches():
    """Test executing a parallel operator with None branches (edge case)."""
    # Create a mock task with no branches
    task = ParallelOperatorModel(
        task_id="parallel_task", operator_type="parallel", branches={}, dependencies=[]
    )

    # Create a state
    state = WorkflowState({})

    # Create a mock registry
    registry = ToolRegistry()

    # Create a mock bulkhead manager
    bulkhead_manager = MagicMock()

    # Execute the parallel handler (should not raise any exception)
    execute(task, state, registry, bulkhead_manager)


if __name__ == "__main__":
    pytest.main()
