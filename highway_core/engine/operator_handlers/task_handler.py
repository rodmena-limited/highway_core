from highway_core.engine.models import TaskOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry


def execute(
    task: TaskOperatorModel, state: WorkflowState, registry: ToolRegistry
) -> None:
    """
    Executes a single TaskOperator.
    """
    print(f"TaskHandler: Executing task: {task.task_id}")

    # 1. Get the tool from the registry
    tool_name = task.function
    try:
        tool_func = registry.get_function(tool_name)
    except KeyError:
        print(f"TaskHandler: Error - {tool_name} not found.")
        raise

    # 2. Resolve arguments
    resolved_args = state.resolve_templating(task.args)
    resolved_kwargs = state.resolve_templating(task.kwargs)

    # 3. Special check for tools that need state
    if tool_name == "tools.memory.set":
        # Inject the state object as the first argument
        resolved_args.insert(0, state)

    # 4. Execute the tool
    try:
        print(f"TaskHandler: Calling {tool_name} with args={resolved_args}")
        result = tool_func(*resolved_args, **resolved_kwargs)
    except Exception as e:
        print(f"TaskHandler: Error executing {tool_name}: {e}")
        raise

    # 5. Save the result
    if task.result_key:
        state.set_result(task.result_key, result)
