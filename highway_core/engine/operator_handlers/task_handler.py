# --- engine/operator_handlers/task_handler.py ---
# Purpose: Handles the execution of a 'TaskOperator'.
# Responsibilities:
# - Finds the correct tool from the ToolRegistry.
# - Resolves all 'args' and 'kwargs' using the WorkflowState.
# - Executes the tool.
# - Saves the 'result' back to the state if 'result_key' is set.

from highway_dsl import TaskOperator
from engine.state import WorkflowState
from tools.registry import ToolRegistry

# Instantiate the registry. In a real app, this might be injected.
registry = ToolRegistry()


def execute(task: TaskOperator, state: WorkflowState) -> list[str]:
    """
    Executes a TaskOperator.
    """
    # 1. Find the tool
    try:
        func = registry.get_function(task.function)
    except KeyError:
        raise ValueError(f"Tool not found: {task.function}")

    # 2. Resolve arguments
    resolved_args = state.resolve_templating(task.args)
    resolved_kwargs = state.resolve_templating(task.kwargs)

    # 3. Execute the tool
    print(f"  [TaskHandler] Executing: {task.function}")
    result = func(*resolved_args, **resolved_kwargs)

    # 4. Save the result
    if task.result_key:
        state.set_result(task.result_key, result)
        print(f"  [TaskHandler] Saved result to: {task.result_key}")

    # TaskOperator always hands off to the next task in the chain
    # The orchestrator will find this via dependencies.
    # We return an empty list as a simple task doesn't create new branches.
    return []
