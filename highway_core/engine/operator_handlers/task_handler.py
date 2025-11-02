import logging
from highway_core.engine.models import TaskOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
from highway_core.tools.bulkhead import BulkheadManager, BulkheadConfig
from typing import Optional, List

logger = logging.getLogger(__name__)


def execute(
    task: TaskOperatorModel,
    state: WorkflowState,
    orchestrator,  # Added for consistent signature
    registry: ToolRegistry,
    bulkhead_manager: Optional[BulkheadManager] = None,
) -> List[str]:
    """
    Executes a single TaskOperator with bulkhead isolation.
    """
    logger.info("TaskHandler: Executing task: %s", task.task_id)

    # 1. Get the tool from the registry
    tool_name = task.function
    try:
        tool_func = registry.get_function(tool_name)
    except KeyError:
        logger.error("TaskHandler: Error - %s not found.", tool_name)
        raise

    # 2. Resolve arguments
    resolved_args = state.resolve_templating(task.args)
    resolved_kwargs = state.resolve_templating(task.kwargs)

    # 3. Special check for tools that need state
    if tool_name in ["tools.memory.set", "tools.memory.increment"]:
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

        logger.info("TaskHandler: Getting or creating bulkhead for '%s'", tool_name)

        # Execute the tool function in the bulkhead
        logger.info("TaskHandler: Calling %s via bulkhead...", tool_name)
        future = bulkhead.execute(tool_func, *resolved_args, **resolved_kwargs)

        try:
            result = (
                future.result().result
            )  # Get the actual result from ExecutionResult
        except Exception as e:
            logger.error(
                "TaskHandler: Error executing %s in bulkhead: %s", tool_name, e
            )
            raise
    else:
        # For backward compatibility, execute without bulkhead if not provided
        logger.info("TaskHandler: Calling %s with args=%s", tool_name, resolved_args)
        result = tool_func(*resolved_args, **resolved_kwargs)

    # 5. Save the result
    if task.result_key:
        state.set_result(task.result_key, result)

    return []  # Return an empty list of new tasks
