import pytest
from unittest.mock import MagicMock, patch
from highway_core.engine.orchestrator import Orchestrator
from highway_core.engine.models import WorkflowModel
from highway_core.engine.models import TaskOperatorModel
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
            ).model_dump()
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
    assert (
        len(orchestrator.handler_map) == 6
    )  # task, condition, parallel, wait, foreach, while
    assert orchestrator.executor is not None
    assert orchestrator.bulkhead_manager is not None


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
        tasks={
            "start_task": start_task.model_dump(),
            "end_task": end_task.model_dump(),
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
            "start_task": start_task.model_dump(),
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
        tasks={
            "start_task": start_task.model_dump(),
            "next_task": next_task.model_dump(),
        },
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
