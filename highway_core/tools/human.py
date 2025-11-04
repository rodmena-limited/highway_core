import logging
import uuid
from typing import TYPE_CHECKING, List, Optional

from .decorators import tool
from highway_core.engine.state import WorkflowState

if TYPE_CHECKING:
    from highway_core.engine.orchestrator import Orchestrator

logger = logging.getLogger(__name__)

@tool("human.request_approval")
def request_approval(
    state: WorkflowState,
    orchestrator: "Orchestrator", # Injected by the worker
    task_id: str, # Injected by the worker
    workflow_run_id: str, # Injected by the worker
    approval_prompt: str,
    manager_email: str
) -> None:
    """
    Durable Human Task: Pauses the workflow and waits for an external event.
    """
    
    workflow_mode = getattr(orchestrator.workflow, 'mode', 'LOCAL')
    
    if workflow_mode != "DURABLE":
        logger.error(f"Task {task_id} failed: 'human.request_approval' is only supported in DURABLE mode.")
        raise Exception("Human tasks are only supported in DURABLE mode.")
        
    # --- DURABLE MODE ---
    db = orchestrator.persistence.db_manager
    
    # 1. Generate a unique token for this human task
    token = str(uuid.uuid4())
    
    # 2. Pause the task, setting its status and token
    db.update_task_status_and_token(workflow_run_id, task_id, "WAITING_FOR_EVENT", token)
    
    # 3. Log the approval info (in a real system, you'd email it)
    approval_url = f"http://127.0.0.1:5000/workflow/resume?token={token}&decision=APPROVED"
    denial_url = f"http://127.0.0.1:5000/workflow/resume?token={token}&decision=DENIED"
    
    logger.info(f"--- HUMAN TASK ({task_id}) ---")
    logger.info(f"Prompt: {approval_prompt}")
    logger.info(f"To: {manager_email}")
    logger.info(f"APPROVE: {approval_url}")
    logger.info(f"DENY:    {denial_url}")
    logger.info(f"Workflow {workflow_run_id} is now durably PAUSED.")
    
    # 4. Return. The worker is now free.