# This is a special tool that requires the WorkflowState.
# The task_handler will inject the 'state' argument.

from highway_core.engine.state import WorkflowState
from typing import Any, Dict
from .decorators import tool


@tool("tools.memory.set")
def set_memory(state: WorkflowState, key: str, value: Any) -> Dict[str, Any]:
    """
    Saves a value to the workflow's volatile memory.
    This tool MUST return a dict for the 'mem_report' result_key.
    """
    print(f"Tool.Memory: Setting key '{key}'")
    state._data["memory"][key] = value  # Accessing internal state directly

    # Return value as specified by the test workflow
    return {"key": key, "status": "ok"}
