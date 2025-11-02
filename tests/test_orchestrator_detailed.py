import pytest
from highway_core.engine.orchestrator import Orchestrator
from highway_core.engine.models import WorkflowModel
from highway_core.engine.common import (
    TaskOperatorModel,
    ConditionOperatorModel,
    ParallelOperatorModel,
    WaitOperatorModel,
    AnyOperatorModel,
)
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry


def test_orchestrator_initialization():
    """Test orchestrator initialization"""
    # Create a simple workflow
    task1 = TaskOperatorModel(
        task_id="task1",
        operator_type="task",
        function="tools.memory.set",
        args=["key1", "value1"],
        dependencies=[],
    )

    workflow = WorkflowModel(
        name="test_workflow", start_task="task1", tasks={"task1": task1.model_dump()}
    )

    state = WorkflowState({})
    registry = ToolRegistry()

    orchestrator = Orchestrator(workflow, state, registry)

    # Check that orchestrator was properly initialized
    assert orchestrator.workflow == workflow
    assert orchestrator.state == state
    assert orchestrator.registry == registry

    # Check that handler map contains expected handlers
    assert "task" in orchestrator.handler_map
    assert "condition" in orchestrator.handler_map
    assert "parallel" in orchestrator.handler_map
    assert "wait" in orchestrator.handler_map
    assert "foreach" in orchestrator.handler_map
    assert "while" in orchestrator.handler_map


def test_orchestrator_run_method():
    """Test the orchestrator's run method with a simple workflow"""
    # Create a simple workflow with one task
    task1 = TaskOperatorModel(
        task_id="task1",
        operator_type="task",
        function="tools.memory.set",
        args=["key1", "value1"],
        dependencies=[],
    )

    workflow = WorkflowModel(
        name="test_workflow", start_task="task1", tasks={"task1": task1.model_dump()}
    )

    state = WorkflowState({})
    registry = ToolRegistry()

    orchestrator = Orchestrator(workflow, state, registry)

    # Run the workflow
    orchestrator.run()

    # Check that the task was executed by checking the state
    # We can't easily test this without a more complex setup, but at least run it
    # to make sure it doesn't crash


def test_orchestrator_with_condition_task():
    """Test orchestrator with a condition task"""
    condition_task = ConditionOperatorModel(
        task_id="cond_task",
        operator_type="condition",
        condition="True",
        if_true="task1",
        if_false="task2",
    )

    task1 = TaskOperatorModel(
        task_id="task1",
        operator_type="task",
        function="tools.memory.set",
        args=["result", "true_branch"],
        dependencies=["cond_task"],
    )

    task2 = TaskOperatorModel(
        task_id="task2",
        operator_type="task",
        function="tools.memory.set",
        args=["result", "false_branch"],
        dependencies=["cond_task"],
    )

    workflow = WorkflowModel(
        name="test_cond_workflow",
        start_task="cond_task",
        tasks={"cond_task": condition_task.model_dump(), "task1": task1.model_dump(), "task2": task2.model_dump()},
    )

    state = WorkflowState({})
    registry = ToolRegistry()

    orchestrator = Orchestrator(workflow, state, registry)

    # Run the workflow
    orchestrator.run()


def test_orchestrator_with_wait_task():
    """Test orchestrator with a wait task"""
    wait_task = WaitOperatorModel(
        task_id="wait_task",
        operator_type="wait",
        wait_for=0.1,  # Wait for 0.1 seconds
        dependencies=[],
    )

    task_after_wait = TaskOperatorModel(
        task_id="task_after_wait",
        operator_type="task",
        function="tools.memory.set",
        args=["after_wait", "completed"],
        dependencies=["wait_task"],
    )

    workflow = WorkflowModel(
        name="test_wait_workflow",
        start_task="wait_task",
        tasks={"wait_task": wait_task.model_dump(), "task_after_wait": task_after_wait.model_dump()},
    )

    state = WorkflowState({})
    registry = ToolRegistry()

    orchestrator = Orchestrator(workflow, state, registry)

    # Run the workflow
    orchestrator.run()


def test_orchestrator_with_parallel_task():
    """Test orchestrator with a parallel task"""
    parallel_task = ParallelOperatorModel(
        task_id="parallel_task",
        operator_type="parallel",
        branches={"branch1": ["task1"], "branch2": ["task2"]},
        dependencies=[],
    )

    task1 = TaskOperatorModel(
        task_id="task1",
        operator_type="task",
        function="tools.memory.set",
        args=["branch1_result", "completed"],
        dependencies=["parallel_task"],
    )

    task2 = TaskOperatorModel(
        task_id="task2",
        operator_type="task",
        function="tools.memory.set",
        args=["branch2_result", "completed"],
        dependencies=["parallel_task"],
    )

    workflow = WorkflowModel(
        name="test_parallel_workflow",
        start_task="parallel_task",
        tasks={"parallel_task": parallel_task.model_dump(), "task1": task1.model_dump(), "task2": task2.model_dump()},
    )

    state = WorkflowState({})
    registry = ToolRegistry()

    orchestrator = Orchestrator(workflow, state, registry)

    # Run the workflow
    orchestrator.run()
