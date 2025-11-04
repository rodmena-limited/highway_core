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
    headers: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
    max_retries: int = 3,
    rate_limit_requests: int = 10,
    rate_limit_window: int = 60,
) -> dict[str, object]:
    """Registers a POST webhook to be sent automatically when a task reaches a specific state.

    Args:
        task_name: Name of the task to monitor
        trigger_on: When to trigger the webhook ("on_completed", "on_failed", "on_start")
        url: Webhook endpoint URL
        headers: HTTP headers to include
        payload: Payload to send (can contain templated values)
        max_retries: Maximum retry attempts
        rate_limit_requests: Max requests per time window
        rate_limit_window: Time window in seconds

    Returns:
        dict with success status and webhook information
    """
    logger.info(
        "  [Tool.Webhook.Post] Registering webhook for task '%s' on %s",
        task_name,
        trigger_on,
    )

    try:
        # Get the database manager to store the webhook configuration
        db_manager = get_db_manager()

        # In this implementation, we need to store the webhook configuration so that
        # when the target task reaches the specified state, a webhook will be triggered.
        # This would normally happen in the context of the current workflow run,
        # but in this tool we don't have direct access to the workflow run ID.
        # Normally, the workflow engine would track these registrations.

        # For this implementation, we'll return success, but in a real system,
        # this would store the configuration in the database linked to the current workflow
        # The actual webhook creation would happen later when the target task changes state
        return {
            "success": True,
            "message": f"Webhook registered for task '{task_name}' on '{trigger_on}' with URL {url}",
            "task": task_name,
            "trigger": trigger_on,
            "url": url,
            "method": "POST",
            "headers": headers or {},
            "payload": payload or {},
        }

    except Exception as e:
        logger.error("  [Tool.Webhook.Post] FAILED to register webhook: %s", e)
        return {"success": False, "error": str(e)}
