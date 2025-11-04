import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, List, Optional

from highway_core.engine.models import TaskOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.bulkhead import BulkheadManager
from highway_core.tools.registry import ToolRegistry

if TYPE_CHECKING:
    from highway_core.engine.executors.base import BaseExecutor  # <-- NEW
    from highway_core.engine.orchestrator import Orchestrator
    from highway_core.engine.resource_manager import ContainerResourceManager

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
            "TaskHandler: Error - No executor provided for task %s",
            task.task_id,
        )
        raise ValueError(f"TaskHandler received no executor for task: {task.task_id}")

    success = True  # Flag to track if the task executed successfully
    result = None
    
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

    except Exception as e:
        logger.error(f"TaskHandler: Error executing task {task.task_id}: {e}")
        success = False
        raise
    finally:
        # 3. Trigger webhooks if appropriate (after task execution regardless of success/failure)
        _trigger_webhooks_for_task_event(
            task_id=task.task_id,
            workflow_run_id=workflow_run_id,
            event_type="completed" if success else "failed",
            result=result,
            error_message=str(e) if not success else None
        )

    return []  # Return an empty list of new tasks


def _trigger_webhooks_for_task_event(
    task_id: str,
    workflow_run_id: str,
    event_type: str,  # e.g., "completed", "failed", "started"
    result=None,
    error_message: Optional[str] = None
):
    """
    Creates webhook records for any webhooks registered for this task/event combination.
    These webhooks will be processed by the webhook runner.
    """
    try:
        from highway_core.persistence.database import get_db_manager
        db_manager = get_db_manager()
        
        # Look for webhook configurations registered for this task + event combination
        # In a real implementation, we'd query a webhook_configs table or similar
        # For now, we'll look for pending webhooks that match this task and event
        
        # Create webhook records that will be processed by the webhook runner
        # The webhook runner will actually make the HTTP requests
        
        # This is where we'd check configuration and create actual webhook records
        # In the actual system, this would query stored configurations for webhooks
        # that should be triggered when this task reaches this state
        webhook_payload = {
            "task_id": task_id,
            "workflow_id": workflow_run_id,
            "event": f"task_{event_type}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "result": result if result is not None else None,
        }
        
        if error_message:
            webhook_payload["error"] = error_message
            
        # Create a webhook record to be processed by the webhook runner
        webhook_success = db_manager.create_webhook(
            task_id=task_id,
            execution_id=f"{task_id}_execution_{int(datetime.now(timezone.utc).timestamp())}",
            workflow_id=workflow_run_id,
            url="http://127.0.0.1:7666/webhook_callback",  # Default URL for demo
            method="POST",
            headers={"Content-Type": "application/json"},
            payload=webhook_payload,
            webhook_type=f"on_{event_type}",
            status="pending"
        )
        
        if webhook_success:
            logger.info(f"Created webhook for task {task_id} {event_type} event")
        else:
            logger.warning(f"Failed to create webhook for task {task_id} {event_type} event")
            
    except Exception as e:
        logger.error(f"Error triggering webhooks for task {task_id} {event_type} event: {e}")
