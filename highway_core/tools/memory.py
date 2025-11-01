# --- tools/memory.py ---
# Implements 'tools.memory.set'.
# This is a special tool that needs access to the WorkflowState.
# The TaskHandler will need to handle this specially.

# This module is a bit of a special case.
# The 'set_memory' function will be called by the TaskHandler,
# which will pass in the state object.


def set_memory(state, key: str, value: any) -> dict:
    """
    A special tool function that sets a value in the workflow state.
    """
    print(f"  [Tool.Memory.Set] Saving to memory key: {key}")
    state.set_memory(key, value)

    size = len(value) if isinstance(value, (list, str, dict)) else 1
    return {"key": key, "size": size, "status": "ok"}
