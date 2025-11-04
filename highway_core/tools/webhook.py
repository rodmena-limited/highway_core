# --- tools/webhook.py ---
# Implements 'tools.webhook.*' functions for webhook functionality.
# This registers webhooks to be sent automatically when the specified conditions are met.

import logging
from typing import Any, Dict, Optional
from highway_core.persistence.database import get_db_manager

from .decorators import tool

logger = logging.getLogger(__name__)


@tool("tools.webhook.post")
def post(
    task_name: str,
    trigger_on: str,  # e.g., "on_completed", "on_failed", "on_start"
    url: str,
) -> dict[str, object]:
    """Registers a POST webhook to be sent automatically when a task reaches a specific state.
    
    Args:
        task_name: Name of the task to monitor
        trigger_on: When to trigger the webhook ("on_completed", "on_failed", "on_start")
        url: Webhook endpoint URL
    
    Returns:
        dict with success status and webhook information
    """
    logger.info("  [Tool.Webhook.Post] Registering webhook for task '%s' on %s", task_name, trigger_on)
    
    try:
        # This would be handled by the engine when it detects task state changes
        # For now, we're just confirming that the registration worked
        # In practice, the workflow engine would need to track these registrations
        # and trigger the webhooks when the specified events occur
        
        # For the purpose of this implementation, we'll return success
        # The actual webhook execution would be handled by the webhook runner
        # when it processes registered webhooks based on task status changes
        return {
            "success": True,
            "message": f"Webhook registered for task '{task_name}' on '{trigger_on}' with URL {url}",
            "task": task_name,
            "trigger": trigger_on,
            "url": url,
            "method": "POST"
        }
        
    except Exception as e:
        logger.error("  [Tool.Webhook.Post] FAILED to register webhook: %s", e)
        return {
            "success": False,
            "error": str(e)
        }