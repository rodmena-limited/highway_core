import logging
from highway_core.engine.models import TaskOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
from highway_core.tools.bulkhead import BulkheadManager
from typing import Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from highway_core.engine.orchestrator import Orchestrator
    from highway_core.engine.executors.base import BaseExecutor  # <-- NEW

logger = logging.getLogger(__name__)


def execute(
    task: TaskOperatorModel,
    state: WorkflowState,
    orchestrator: Optional["Orchestrator"],
    registry: ToolRegistry,  # <-- Keep this required
    bulkhead_manager: Optional[BulkheadManager] = None,
    executor: Optional["BaseExecutor"] = None,  # <-- NEW
    resource_manager: Optional["ContainerResourceManager"] = None,
    workflow_run_id: Optional[str] = None,
) -> List[str]:
    """
    Delegates execution of a TaskOperator to a provided executor.
    """
    logger.info("TaskHandler: Delegating task %s", task.task_id)

    if not executor:
        logger.error(
            "TaskHandler: Error - No executor provided for task %s", task.task_id
        )
        raise ValueError(f"TaskHandler received no executor for task: {task.task_id}")

    try:
        # 1. The Orchestrator has already selected the correct executor.
        #    We just call it and pass all dependencies.
        result = executor.execute(
            task=task,
            state=state,
            registry=registry,
            bulkhead_manager=bulkhead_manager,
            resource_manager=resource_manager,
            workflow_run_id=workflow_run_id,
        )

        # 2. Save the result (this logic stays in the handler)
        if task.result_key:
            state.set_result(task.result_key, result)

        # 3. Mark the task as completed in persistence
        if orchestrator and hasattr(orchestrator.persistence, "complete_task"):
            orchestrator.persistence.complete_task(orchestrator.run_id, task.task_id, result)

        return []  # Return an empty list of new tasks
    except Exception as e:
        logger.error(f"TaskHandler: Error executing task {task.task_id}: {e}")
        # Mark the task as failed in persistence
        if orchestrator and hasattr(orchestrator.persistence, "fail_task"):
            orchestrator.persistence.fail_task(orchestrator.run_id, task.task_id, str(e))
        raise