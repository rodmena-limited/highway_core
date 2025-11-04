import logging
import re
import time
from datetime import datetime, timezone, timedelta
from typing import TYPE_CHECKING, List, Optional

from highway_core.engine.models import WaitOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.bulkhead import BulkheadManager
from highway_core.tools.registry import ToolRegistry

if TYPE_CHECKING:
    from highway_core.engine.executors.base import BaseExecutor
    from highway_core.engine.orchestrator import Orchestrator

logger = logging.getLogger(__name__)


def execute(
    task: WaitOperatorModel,
    state: WorkflowState,
    orchestrator: "Orchestrator",
    registry: Optional["ToolRegistry"],
    bulkhead_manager: Optional["BulkheadManager"],
    executor: Optional["BaseExecutor"] = None,
    resource_manager=None,
    workflow_run_id: str = "",
) -> List[str]:
    """
    Executes a WaitOperator.
    - In 'LOCAL' mode, it blocks with time.sleep().
    - In 'DURABLE' mode, it sets the task status to WAITING_FOR_TIMER and returns.
    """
    wait_for = state.resolve_templating(task.wait_for)
    
    # Determine the workflow mode from the orchestrator's context
    workflow_mode = getattr(orchestrator.workflow, 'mode', 'LOCAL')
    
    logger.info(f"WaitHandler: Waiting for {wait_for} in {workflow_mode} mode.")
    
    # --- Calculate wake_up_time ---
    wake_up_time = None
    duration_sec = 0
    
    if isinstance(wait_for, (int, float)):
        duration_sec = wait_for
    elif isinstance(wait_for, str):
        if wait_for.startswith("duration:"):
            duration_str = wait_for[9:]
            match = re.match(r"(\d+\.?\d*)([smh])", duration_str)
            if match:
                value, unit = match.groups()
                value = float(value)
                if unit == "s": duration_sec = value
                elif unit == "m": duration_sec = value * 60
                elif unit == "h": duration_sec = value * 3600
            else:
                try: duration_sec = float(duration_str)
                except ValueError: pass
        elif wait_for.startswith("time:"):
            time_str = wait_for[5:]
            target_time = datetime.strptime(time_str, "%H:%M:%S").time()
            now = datetime.now(timezone.utc)
            # Ensure the target_datetime is timezone-aware
            target_datetime = datetime.combine(now.date(), target_time, tzinfo=timezone.utc)
            if target_datetime < now:
                target_datetime += timedelta(days=1)
            wake_up_time = target_datetime
            duration_sec = (target_datetime - now).total_seconds()

    if wake_up_time is None:
        wake_up_time = datetime.now(timezone.utc) + timedelta(seconds=duration_sec)
    # ------------------------------

    if workflow_mode == "DURABLE":
        # --- DURABLE MODE ---
        # Set task status to WAITING and let the Scheduler wake it up.
        db = orchestrator.persistence.db_manager
        db.update_task_status_and_wakeup(
            workflow_run_id, task.task_id, "WAITING_FOR_TIMER", wake_up_time
        )
        logger.info(f"Task {task.task_id} set to WAITING_FOR_TIMER until {wake_up_time}.")
    
    else:
        # --- LOCAL MODE ---
        # Block the thread. This is the original behavior.
        if duration_sec > 0:
            logger.info(f"Task {task.task_id} sleeping for {duration_sec}s...")
            time.sleep(duration_sec)
            logger.info("WaitHandler: Wait complete.")

    return [] # This handler never adds new tasks