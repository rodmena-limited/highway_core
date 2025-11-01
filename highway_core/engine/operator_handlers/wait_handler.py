# --- engine/operator_handlers/wait_handler.py ---
# Purpose: Handles the 'WaitOperator'.
# Responsibilities:
# - Pauses execution for a 'timedelta' or until a 'datetime' or event.

import time
from datetime import timedelta, datetime
from highway_dsl import WaitOperator
from engine.state import WorkflowState


def execute(task: WaitOperator, state: WorkflowState) -> list[str]:
    """
    Executes a WaitOperator.
    """
    wait_for = task.wait_for  # The model_validator already parsed this

    if isinstance(wait_for, timedelta):
        print(f"  [WaitHandler] Waiting for {wait_for.total_seconds()} seconds...")
        time.sleep(wait_for.total_seconds())
        print("  [WaitHandler] Wait complete.")

    elif isinstance(wait_for, datetime):
        now = datetime.now()
        wait_seconds = (wait_for - now).total_seconds()
        if wait_seconds > 0:
            print(f"  [WaitHandler] Waiting until {wait_for} ({wait_seconds}s)...")
            time.sleep(wait_seconds)
            print("  [WaitHandler] Wait complete.")
        else:
            print(f"  [WaitHandler] Wait time {wait_for} is in the past. Continuing.")

    elif isinstance(wait_for, str):
        # This is a complex event-based wait.
        # A real engine would subscribe to an event system.
        print(
            f"  [WaitHandler] STUB: Waiting for event '{wait_for}'. Proceeding immediately."
        )

    return []
