# --- engine/operator_handlers/wait_handler.py ---
# Purpose: Handles the 'WaitOperator'.
# Responsibilities:
# - Pauses execution for a specified duration or until a specific time.

import logging
import time
from datetime import datetime
import re
from typing import List, Optional, TYPE_CHECKING
from highway_core.engine.models import WaitOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
from highway_core.tools.bulkhead import BulkheadManager

if TYPE_CHECKING:
    from highway_core.engine.orchestrator import Orchestrator
    from highway_core.engine.executors.base import BaseExecutor

logger = logging.getLogger(__name__)


def execute(
    task: WaitOperatorModel,
    state: WorkflowState,
    orchestrator: "Orchestrator",  # Added for consistent signature
    registry: Optional["ToolRegistry"], # <-- Make registry optional
    bulkhead_manager: Optional["BulkheadManager"], # <-- Make optional
    executor: Optional["BaseExecutor"] = None, # <-- Add this argument
) -> List[str]:
    """
    Executes a WaitOperator.
    """
    wait_for = state.resolve_templating(task.wait_for)

    logger.info("WaitHandler: Waiting for %s...", wait_for)

    # Handle different wait_for formats
    if isinstance(wait_for, (int, float)):
        # If it's a number, treat as seconds
        time.sleep(wait_for)
        logger.info("WaitHandler: Wait complete.")

    elif isinstance(wait_for, str):
        if wait_for.startswith("duration:"):
            # Parse duration format like "duration:5s", "duration:2m", "duration:1h"
            duration_str = wait_for[9:]  # Remove "duration:" prefix
            # Extract number and unit (s, m, h)
            match = re.match(r"(\d+\.?\d*)([smh])", duration_str)
            if match:
                value, unit = match.groups()
                value = float(value)
                if unit == "s":
                    seconds = value
                elif unit == "m":
                    seconds = value * 60
                elif unit == "h":
                    seconds = value * 3600
                else:
                    seconds = value  # Default to seconds if unit not recognized
                time.sleep(seconds)
                logger.info("WaitHandler: Wait complete.")
            else:
                # If format doesn't match, fallback to seconds (handle potential float conversion errors)
                try:
                    seconds = float(duration_str)
                    time.sleep(seconds)
                    logger.info("WaitHandler: Wait complete.")
                except ValueError:
                    logger.warning(
                        "WaitHandler: Invalid duration format '%s'. Continuing.",
                        duration_str,
                    )

        elif wait_for.startswith("time:"):
            # Parse time format like "time:04:00:00" (4 AM)
            time_str = wait_for[5:]  # Remove "time:" prefix
            target_time = datetime.strptime(time_str, "%H:%M:%S").time()
            now = datetime.now()
            target_datetime = datetime.combine(now.date(), target_time)

            # If the target time is earlier than now, assume it's for tomorrow
            if target_datetime.time() < now.time():
                target_datetime = datetime.combine(
                    now.date().replace(day=now.date().day + 1), target_time
                )

            wait_seconds = (target_datetime - now).total_seconds()
            if wait_seconds > 0:
                logger.info("WaitHandler: Waiting until %s...", target_time)
                time.sleep(wait_seconds)
                logger.info("WaitHandler: Wait complete.")
            else:
                logger.info(
                    "WaitHandler: Target time %s is in the past for today. Continuing.",
                    target_time,
                )

        else:
            # For other string formats (datetime, event-based), we'll implement later
            logger.info(
                "WaitHandler: STUB: Waiting for event/datetime '%s'. Proceeding immediately.",
                wait_for,
            )

    else:
        # For other types (datetime objects), we'll implement later
        logger.info(
            "WaitHandler: STUB: Waiting for '%s'. Proceeding immediately.", wait_for
        )

    return []
