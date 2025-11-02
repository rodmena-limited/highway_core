# highway_core/engine/executors/local_python.py
import logging
from typing import Any, Optional, TYPE_CHECKING

from highway_core.engine.executors.base import BaseExecutor
from highway_core.tools.bulkhead import BulkheadConfig

if TYPE_CHECKING:
    from highway_core.engine.models import TaskOperatorModel
    from highway_core.engine.state import WorkflowState
    from highway_core.tools.registry import ToolRegistry
    from highway_core.tools.bulkhead import BulkheadManager

logger = logging.getLogger(__name__)


class LocalPythonExecutor(BaseExecutor):
    """
    Executes tasks as Python functions in the local process.
    This encapsulates the original logic from task_handler.
    """

    def execute(
        self,
        task: "TaskOperatorModel",
        state: "WorkflowState",
        registry: "ToolRegistry",
        bulkhead_manager: Optional["BulkheadManager"],
    ) -> Any:
        if not task.function:
            logger.error(
                "LocalPythonExecutor: Error - Task %s has 'python' runtime but no 'function' defined.",
                task.task_id,
            )
            raise ValueError(
                f"Task {task.task_id} is missing 'function' attribute for python runtime."
            )

        logger.info("LocalPythonExecutor: Executing task: %s", task.task_id)

        # 1. Get the tool from the registry
        tool_name = task.function
        try:
            tool_func = registry.get_function(tool_name)
        except KeyError:
            logger.error("LocalPythonExecutor: Error - %s not found.", tool_name)
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
            bulkhead = bulkhead_manager.get_bulkhead(tool_name)
            if not bulkhead:
                # Create a default config for the tool
                config = BulkheadConfig(
                    name=tool_name,
                    max_concurrent_calls=5,
                    max_queue_size=20,
                    timeout_seconds=30.0,
                    failure_threshold=3,
                    success_threshold=2,
                    isolation_duration=60.0,
                )
                bulkhead = bulkhead_manager.create_bulkhead(config)

            logger.info("LocalPythonExecutor: Calling %s via bulkhead...", tool_name)
            future = bulkhead.execute(tool_func, *resolved_args, **resolved_kwargs)

            try:
                result = (
                    future.result().result
                )  # Get the actual result from ExecutionResult
            except Exception as e:
                logger.error(
                    "LocalPythonExecutor: Error executing %s in bulkhead: %s",
                    tool_name,
                    e,
                )
                raise
        else:
            # Execute without bulkhead if not provided
            logger.info(
                "LocalPythonExecutor: Calling %s with args=%s", tool_name, resolved_args
            )
            result = tool_func(*resolved_args, **resolved_kwargs)

        # 5. Return the result (do NOT save to state here)
        return result
