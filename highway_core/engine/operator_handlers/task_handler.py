from highway_core.engine.models import TaskOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
from highway_core.tools.bulkhead import BulkheadManager, BulkheadConfig
from typing import Optional


def execute(
    task: TaskOperatorModel,
    state: WorkflowState,
    registry: ToolRegistry,
    bulkhead_manager: Optional[BulkheadManager] = None,
) -> None:
    """
    Executes a single TaskOperator with bulkhead isolation.
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

    # 4. Execute the tool with bulkhead isolation if bulkhead manager is provided
    if bulkhead_manager:
        # Get or create a bulkhead FOR THAT TOOL
        bulkhead = bulkhead_manager.get_bulkhead(tool_name)
        if not bulkhead:
            # Create a default config for the tool
            config = BulkheadConfig(
                name=tool_name,
                max_concurrent_calls=5,  # Reasonable default
                max_queue_size=20,  # Reasonable default queue size
                timeout_seconds=30.0,  # Reasonable timeout
                failure_threshold=3,  # 3 consecutive failures trigger isolation
                success_threshold=2,  # 2 consecutive successes return to healthy
                isolation_duration=60.0,  # 1 minute isolation period
            )
            bulkhead = bulkhead_manager.create_bulkhead(config)

        print(f"TaskHandler: Getting or creating bulkhead for '{tool_name}'")

        # Execute the tool function in the bulkhead
        print(f"TaskHandler: Calling {tool_name} via bulkhead...")
        future = bulkhead.execute(tool_func, *resolved_args, **resolved_kwargs)

        try:
            result = (
                future.result().result
            )  # Get the actual result from ExecutionResult
        except Exception as e:
            print(f"TaskHandler: Error executing {tool_name} in bulkhead: {e}")
            raise
    else:
        # For backward compatibility, execute without bulkhead if not provided
        print(f"TaskHandler: Calling {tool_name} with args={resolved_args}")
        result = tool_func(*resolved_args, **resolved_kwargs)

    # 5. Save the result
    if task.result_key:
        state.set_result(task.result_key, result)
