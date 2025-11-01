from highway_core.engine.models import TaskOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
from highway_core.tools.bulkhead import BulkheadManager, BulkheadConfig


def execute(
    task: TaskOperatorModel, state: WorkflowState, registry: ToolRegistry
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

    # 4. Execute the tool with bulkhead isolation
    # Only apply bulkhead for specific potentially risky operations
    if tool_name.startswith("tools.external.") or tool_name.startswith("tools.http."):
        # Create bulkhead manager specifically for external risky operations
        bulkhead_manager = BulkheadManager()

        # Create a bulkhead for this specific task
        task_bulkhead_config = BulkheadConfig(
            name=f"task-{task.task_id}",
            max_concurrent_calls=1,  # Each task runs in its own isolated space
            max_queue_size=5,  # Small queue for this task
            timeout_seconds=10.0,  # Reasonable timeout for individual tasks
            failure_threshold=2,  # Sensitive to failures for individual tasks
            success_threshold=1,  # Single success returns to healthy
            isolation_duration=5.0,  # Shorter isolation period for tasks
        )

        try:
            task_bulkhead = bulkhead_manager.create_bulkhead(task_bulkhead_config)
        except ValueError as e:
            print(
                f"TaskHandler: Failed to create bulkhead for task {task.task_id}: {e}"
            )
            # Fallback: run directly without bulkhead
            try:
                print(f"TaskHandler: Calling {tool_name} with args={resolved_args}")
                result = tool_func(*resolved_args, **resolved_kwargs)
            except Exception as e:
                print(f"TaskHandler: Error executing {tool_name}: {e}")
                raise
            finally:
                bulkhead_manager.shutdown_all()
        else:
            # Execute the tool function in the bulkhead
            future = task_bulkhead.execute(tool_func, *resolved_args, **resolved_kwargs)

            try:
                result = future.result()
            except Exception as e:
                print(f"TaskHandler: Error executing {tool_name} in bulkhead: {e}")
                raise
            finally:
                bulkhead_manager.shutdown_all()
    else:
        # For internal operations like logging and memory, execute directly
        # to avoid potential state access issues and reduce overhead
        print(f"TaskHandler: Calling {tool_name} with args={resolved_args}")
        result = tool_func(*resolved_args, **resolved_kwargs)

    # 5. Save the result
    if task.result_key:
        state.set_result(task.result_key, result)
