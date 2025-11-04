import graphlib
import logging
from typing import TYPE_CHECKING, Dict, Optional, Set

from highway_core.engine.models import AnyOperatorModel, TaskOperatorModel
from highway_core.engine.operator_handlers import task_handler
from highway_core.engine.state import WorkflowState
from highway_core.tools.bulkhead import BulkheadManager
from highway_core.tools.registry import ToolRegistry

if TYPE_CHECKING:
    from highway_core.engine.executors.base import BaseExecutor
    from highway_core.engine.orchestrator import Orchestrator

logger = logging.getLogger(__name__)


def _run_sub_workflow(
    sub_graph_tasks: Dict[str, AnyOperatorModel],
    sub_graph: Dict[str, Set[str]],
    state: WorkflowState,
    registry: Optional[ToolRegistry],
    bulkhead_manager: Optional[BulkheadManager],
    executor: Optional["BaseExecutor"] = None,  # Executor from parent (can be None)
    available_executors: Optional[
        Dict[str, "BaseExecutor"]
    ] = None,  # Available executors for sub-tasks
    orchestrator: Optional["Orchestrator"] = None,  # Pass orchestrator for proper error handling
) -> None:
    """
    Runs a sub-workflow (like a loop body) to completion.
    This is a blocking, sequential, "mini-orchestrator".
    """
    sub_sorter = graphlib.TopologicalSorter(sub_graph)
    sub_sorter.prepare()

    # We only support 'task' for now in sub-workflows.
    # This can be expanded later.
    sub_handler_map = {"task": task_handler.execute}
    # For sub-workflows, we need to select the executor based on each task's runtime
    # We'll need to have access to available executors, so let's pass them as an option parameter

    while sub_sorter.is_active():
        runnable_sub_tasks = sub_sorter.get_ready()
        if not runnable_sub_tasks:
            break

        for task_id in runnable_sub_tasks:
            task_model = sub_graph_tasks[task_id]

            if task_model.operator_type == "task":
                # We must clone the task to resolve templating
                # This is the fix for the `log_user` problem
                task_clone: TaskOperatorModel = task_model.model_copy(deep=True)
                task_clone.args = state.resolve_templating(task_clone.args)

                # For sub-workflows, select executor based on the task's runtime
                selected_sub_executor = executor  # Start with the parent executor
                if available_executors and isinstance(task_clone, TaskOperatorModel):
                    # If a specific executor is available for this task's runtime, use it
                    runtime = getattr(
                        task_clone, "runtime", "python"
                    )  # Default to python if no runtime
                    specific_executor = available_executors.get(runtime)
                    if specific_executor:
                        selected_sub_executor = specific_executor

                handler_func = sub_handler_map.get(task_clone.operator_type)
                if handler_func:
                    # Attempt to start the task in the main workflow's persistence
                    # This might fail if the task is also being tracked by the main workflow, which is fine
                    task_started = False
                    if orchestrator and hasattr(orchestrator, 'persistence'):
                        try:
                            task_started = orchestrator.persistence.start_task(orchestrator.run_id, task_clone)
                        except Exception:
                            # If starting the task fails (e.g., due to duplicate key), 
                            # we'll still execute the task but won't track it again in this context
                            task_started = True  # Treat as started to continue execution

                    handler_func(
                        task=task_clone,
                        state=state,
                        orchestrator=orchestrator,  # Pass the orchestrator for proper error handling
                        registry=registry,  # type: ignore
                        bulkhead_manager=bulkhead_manager,  # type: ignore
                        executor=selected_sub_executor,
                        resource_manager=None,  # Pass None for resource manager in sub-workflows
                        workflow_run_id="",  # Pass empty string for workflow_run_id in sub-workflows
                    )

                    # After successful execution, mark as complete in main workflow persistence if needed
                    if task_started and orchestrator and hasattr(orchestrator, 'persistence'):
                        try:
                            actual_result = None
                            if isinstance(task_clone, TaskOperatorModel) and task_clone.result_key:
                                actual_result = state.get_result(task_clone.result_key)
                            orchestrator.persistence.complete_task(orchestrator.run_id, task_clone.task_id, actual_result)
                        except Exception as e:
                            # If completion fails (e.g., due to duplicate key), that's fine
                            # The task may have already been completed by the main workflow
                            logger.debug("Sub-workflow: Could not complete task '%s' in persistence: %s", task_clone.task_id, str(e))
            else:
                logger.warning(
                    "_run_sub_workflow: Skipping unsupported operator type '%s' for task '%s'",
                    task_model.operator_type,
                    task_id,
                )

            sub_sorter.done(task_id)
