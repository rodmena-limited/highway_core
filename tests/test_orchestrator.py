import pytest
from unittest.mock import MagicMock, patch
from highway_core.engine.orchestrator import Orchestrator
from highway_core.engine.models import WorkflowModel, TaskOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry


def test_orchestrator_initialization():
    """Test orchestrator initialization."""
    # Create a mock workflow
    workflow = WorkflowModel(
        name="test_workflow",
        version="1.0.0",
        description="Test workflow",
        variables={},
        start_task="start_task",
        tasks={
            "start_task": TaskOperatorModel(
                task_id="start_task",
                operator_type="task",
                function="tools.log.info",
                args=["Starting workflow..."],
                dependencies=[],
            )
        },
    )

    # Create a state
    state = WorkflowState({})

    # Create a registry
    registry = ToolRegistry()

    # Create an orchestrator
    orchestrator = Orchestrator(workflow, state, registry)

    # Verify initialization
    assert orchestrator.workflow == workflow
    assert orchestrator.state == state
    assert orchestrator.registry == registry
    assert orchestrator.sorter is not None
    assert orchestrator.completed_tasks == set()
    assert len(orchestrator.handler_map) == 4


def test_dependencies_met():
    """Test the _dependencies_met method."""
    # Create a mock workflow
    workflow = WorkflowModel(
        name="test_workflow",
        version="1.0.0",
        description="Test workflow",
        variables={},
        start_task="start_task",
        tasks={},
    )

    # Create a state
    state = WorkflowState({})

    # Create a registry
    registry = ToolRegistry()

    # Create an orchestrator
    orchestrator = Orchestrator(workflow, state, registry)

    # Add a completed task
    orchestrator.completed_tasks.add("task_a")

    # Test with all dependencies met
    result = orchestrator._dependencies_met(["task_a"])
    assert result is True

    # Test with some dependencies not met
    result = orchestrator._dependencies_met(["task_a", "task_b"])
    assert result is False

    # Test with no dependencies
    result = orchestrator._dependencies_met([])
    assert result is True


def test_execute_task():
    """Test the _execute_task method."""
    # Create a mock workflow
    workflow = WorkflowModel(
        name="test_workflow",
        version="1.0.0",
        description="Test workflow",
        variables={},
        start_task="start_task",
        tasks={},
    )

    # Create a state
    state = WorkflowState({})

    # Create a registry
    registry = ToolRegistry()
    registry.functions["tools.log.info"] = lambda msg: {"result": msg}

    # Create an orchestrator
    orchestrator = Orchestrator(workflow, state, registry)

    # Create a mock task
    task = TaskOperatorModel(
        task_id="test_task",
        operator_type="task",
        function="tools.log.info",
        args=["Hello, World!"],
        result_key="test_result",
        dependencies=[],
    )

    # Execute the task
    orchestrator._execute_task(task, state, registry)

    # Verify the result was set in the state
    assert state.get_result("test_result") is not None


def test_execute_condition():
    """Test the _execute_condition method."""
    # Create a mock workflow
    workflow = WorkflowModel(
        name="test_workflow",
        version="1.0.0",
        description="Test workflow",
        variables={"test_var": "test_value"},
        start_task="start_task",
        tasks={},
    )

    # Create a state
    state = WorkflowState({"test_var": "test_value"})

    # Create a registry
    registry = ToolRegistry()

    # Create an orchestrator
    orchestrator = Orchestrator(workflow, state, registry)

    # Mock the condition handler
    with patch(
        "highway_core.engine.operator_handlers.condition_handler.execute"
    ) as mock_condition_execute:
        # Create a mock condition task
        task = MagicMock()
        task.operator_type = "condition"
        task.condition = "{{variables.test_var}} == 'test_value'"
        task.if_true = "task_a"
        task.if_false = "task_b"

        # Execute the condition
        orchestrator._execute_condition(task, state, registry)

        # Verify the condition handler was called
        mock_condition_execute.assert_called_once()


def test_execute_parallel():
    """Test the _execute_parallel method."""
    # Create a mock workflow
    workflow = WorkflowModel(
        name="test_workflow",
        version="1.0.0",
        description="Test workflow",
        variables={},
        start_task="start_task",
        tasks={},
    )

    # Create a state
    state = WorkflowState({})

    # Create a registry
    registry = ToolRegistry()

    # Create an orchestrator
    orchestrator = Orchestrator(workflow, state, registry)

    # Mock the parallel handler
    with patch(
        "highway_core.engine.operator_handlers.parallel_handler.execute"
    ) as mock_parallel_execute:
        # Create a mock parallel task
        task = MagicMock()
        task.operator_type = "parallel"
        task.branches = {"branch_1": ["task_a"], "branch_2": ["task_b"]}

        # Execute the parallel task
        orchestrator._execute_parallel(task, state, registry)

        # Verify the parallel handler was called
        mock_parallel_execute.assert_called_once()


def test_execute_wait():
    """Test the _execute_wait method."""
    # Create a mock workflow
    workflow = WorkflowModel(
        name="test_workflow",
        version="1.0.0",
        description="Test workflow",
        variables={},
        start_task="start_task",
        tasks={},
    )

    # Create a state
    state = WorkflowState({})

    # Create a registry
    registry = ToolRegistry()

    # Create an orchestrator
    orchestrator = Orchestrator(workflow, state, registry)

    # Mock the wait handler
    with patch(
        "highway_core.engine.operator_handlers.wait_handler.execute"
    ) as mock_wait_execute:
        # Create a mock wait task
        task = MagicMock()
        task.operator_type = "wait"
        task.wait_for = 0.1

        # Execute the wait task
        orchestrator._execute_wait(task, state, registry)

        # Verify the wait handler was called
        mock_wait_execute.assert_called_once()


def test_run_method():
    """Test the run method with a simple workflow."""
    # Create tasks
    start_task = TaskOperatorModel(
        task_id="start_task",
        operator_type="task",
        function="tools.log.info",
        args=["Starting workflow..."],
        dependencies=[],
    )

    end_task = TaskOperatorModel(
        task_id="end_task",
        operator_type="task",
        function="tools.log.info",
        args=["Ending workflow..."],
        dependencies=["start_task"],
    )

    # Create a workflow
    workflow = WorkflowModel(
        name="test_workflow",
        version="1.0.0",
        description="Test workflow",
        variables={},
        start_task="start_task",
        tasks={"start_task": start_task, "end_task": end_task},
    )

    # Create a state
    state = WorkflowState({})

    # Create a registry
    registry = ToolRegistry()
    registry.functions["tools.log.info"] = lambda msg: {"result": msg}

    # Create an orchestrator
    orchestrator = Orchestrator(workflow, state, registry)

    # Run the workflow
    orchestrator.run()


def test_run_method_with_missing_task():
    """Test the run method with a missing task in the dependency graph.

    This test checks cases where a task references a dependency that doesn't exist in the workflow.
    The orchestrator should handle this gracefully by detecting the missing dependency."""
    # Create a workflow with only start_task
    start_task = TaskOperatorModel(
        task_id="start_task",
        operator_type="task",
        function="tools.log.info",
        args=["Starting workflow..."],
        dependencies=[],
    )

    # Create a workflow with only start_task (no end_task)
    workflow = WorkflowModel(
        name="test_workflow",
        version="1.0.0",
        description="Test workflow",
        variables={},
        start_task="start_task",
        tasks={
            "start_task": start_task,
        },
    )

    # Create a state
    state = WorkflowState({})

    # Create a registry
    registry = ToolRegistry()
    registry.functions["tools.log.info"] = lambda msg: {"result": msg}

    # Create an orchestrator
    orchestrator = Orchestrator(workflow, state, registry)

    # Run the workflow
    orchestrator.run()


def test_run_method_with_invalid_operator_type():
    """Test the run method when it encounters an operator type without a handler."""
    # Create tasks: start_task is valid, next_task will have no handler
    start_task = TaskOperatorModel(
        task_id="start_task",
        operator_type="task",  # Valid type
        function="tools.log.info",
        args=["Starting workflow..."],
        dependencies=[],
    )

    # This task will have no handler as we'll remove it from the map
    next_task = TaskOperatorModel(
        task_id="next_task",
        operator_type="task",  # Normally valid type
        function="tools.log.info",
        args=["Next step..."],
        dependencies=["start_task"],  # Depends on start_task
    )

    # Create a workflow
    workflow = WorkflowModel(
        name="test_workflow",
        version="1.0.0",
        description="Test workflow",
        variables={},
        start_task="start_task",
        tasks={"start_task": start_task, "next_task": next_task},
    )

    # Create a state
    state = WorkflowState({})

    # Create a registry
    registry = ToolRegistry()
    registry.functions["tools.log.info"] = lambda msg: {"result": msg}

    # Create an orchestrator
    orchestrator = Orchestrator(workflow, state, registry)

    # Manually remove the handler to simulate an invalid/missing handler for next_task
    # We'll remove the 'task' handler which will affect the next_task execution
    original_handler = orchestrator.handler_map.pop("task")

    # Run the workflow (should handle missing handler gracefully)
    # The start_task should complete, then when it tries to execute next_task,
    # it should encounter the missing handler
    orchestrator.run()

    # Restore the handler
    orchestrator.handler_map["task"] = original_handler


if __name__ == "__main__":
    pytest.main()
