# This is a special tool that requires the WorkflowState.
# The task_handler will inject the 'state' argument.

import threading
from highway_core.engine.state import WorkflowState
from typing import Any, Dict
from .decorators import tool

# A lock to make counter operations atomic
_memory_lock = threading.Lock()


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


@tool("tools.memory.increment")
def increment_memory(state: WorkflowState, key: str) -> Dict[str, Any]:
    """
    Atomically increments a value in memory.
    """
    with _memory_lock:
        current_val = state._data["memory"].get(key, 0)
        if not isinstance(current_val, (int, float)):
            current_val = 0
        new_val = current_val + 1
        state._data["memory"][key] = new_val

    print(f"Tool.Memory: Incremented '{key}' to {new_val}")
    return {"key": key, "new_value": new_val}


@tool("tools.memory.add")
def add_memory(state: WorkflowState, key: str, value: Any) -> Dict[str, Any]:
    """
    Adds a value to a memory key. If the current value is a number, adds to it.
    If the value parameter contains an arithmetic expression, evaluates it first.
    """
    print(f"Tool.Memory: Adding value to key '{key}'")

    computed_value: int = 0

    # If value is a string that looks like an arithmetic expression, try to evaluate it
    if isinstance(value, str) and "+" in value:
        try:
            # This is a simple and limited arithmetic evaluator - only for basic math
            # Split on '+' and evaluate each part
            parts = value.split("+")
            total = 0
            for part in parts:
                part = part.strip()
                try:
                    # If it's a number string or a template like "{{memory.loop_counter}}"
                    if part.startswith("{{") and part.endswith("}}"):
                        # This is a template, resolve it
                        resolved_part = state.resolve_templating(part)
                        total += int(resolved_part)
                    else:
                        total += int(part)
                except ValueError:
                    # If we can't convert to int, just return early
                    print(f"Tool.Memory: Could not convert '{part}' to integer")
                    return {"key": key, "status": "error"}

            computed_value = total
        except Exception:
            # If expression evaluation fails, just use the raw value as an integer if possible
            print(
                f"Tool.Memory: Error evaluating expression '{value}', using raw value"
            )
            try:
                computed_value = int(value)
            except (ValueError, TypeError):
                computed_value = 0  # Default to 0 if not convertible
    else:
        # value is not a string with '+', so try to convert directly to int
        try:
            computed_value = int(value)
        except (ValueError, TypeError):
            computed_value = 0  # Default to 0 if not convertible

    # Get current value and add to it
    current_value = state._data["memory"].get(key, 0)
    try:
        current_value_int = int(current_value)
    except (ValueError, TypeError):
        current_value_int = 0  # Default to 0 if not a number

    final_int_value = current_value_int + computed_value
    state._data["memory"][key] = final_int_value

    # Return value as specified by the test workflow
    return {
        "key": key,
        "old_value": current_value_int,
        "new_value": final_int_value,
        "status": "ok",
    }
