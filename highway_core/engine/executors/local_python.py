# highway_core/engine/executors/local_python.py
import logging
import os
import tempfile
from typing import TYPE_CHECKING, Any, Optional

from highway_core.engine.executors.base import BaseExecutor
from highway_core.tools.bulkhead import BulkheadConfig
from highway_core.utils.docker_detector import is_running_in_docker
from highway_core.utils.naming import generate_safe_container_name

if TYPE_CHECKING:
    from highway_core.engine.models import TaskOperatorModel
    from highway_core.engine.resource_manager import ContainerResourceManager
    from highway_core.engine.state import WorkflowState
    from highway_core.tools.bulkhead import BulkheadManager
    from highway_core.tools.registry import ToolRegistry

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
        resource_manager: Optional["ContainerResourceManager"],
        workflow_run_id: Optional[str],
    ) -> Any:
        # Check if we're running inside Docker
        in_docker = is_running_in_docker()

        if in_docker:
            logger.info(
                "LocalPythonExecutor: System is running in Docker, executing task %s locally to avoid nested containers.",
                task.task_id,
            )
            # If running in Docker, execute locally (following Docker best practices to avoid nested containers)
            return self._execute_locally(task, state, registry, bulkhead_manager)
        else:
            logger.info(
                "LocalPythonExecutor: System is not running in Docker, executing task %s in Docker container with highway_core installed.",
                task.task_id,
            )
            # For requirement #4: if executor is local_python and system is not running inside Docker,
            # we need to run the python code inside a docker container with highway_core installed.
            return self._execute_in_docker_isolated(
                task,
                state,
                registry,
                bulkhead_manager,
                resource_manager,
                workflow_run_id,
            )

    def _execute_locally(
        self,
        task: "TaskOperatorModel",
        state: "WorkflowState",
        registry: "ToolRegistry",
        bulkhead_manager: Optional["BulkheadManager"],
    ) -> Any:
        """
        Execute the Python function locally in the current process.
        """
        if not task.function:
            logger.error(
                "LocalPythonExecutor: Error - Task %s has 'python' runtime but no 'function' defined.",
                task.task_id,
            )
            raise ValueError(
                f"Task {task.task_id} is missing 'function' attribute for python runtime."
            )

        logger.info("LocalPythonExecutor: Executing task: %s", task.task_id)

        # 1. Verify the tool exists in the registry before attempting execution
        if task.function not in registry.functions:
            available_functions = list(registry.functions.keys())
            error_message = (
                f"Task {task.task_id} failed: Function '{task.function}' not found in registry. "
                f"Available functions: {available_functions}"
            )
            logger.error("LocalPythonExecutor: %s", error_message)
            raise KeyError(error_message)

        # 2. Get the tool from the registry
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
                "LocalPythonExecutor: Calling %s with args=%s",
                tool_name,
                resolved_args,
            )
            result = tool_func(*resolved_args, **resolved_kwargs)

        # 5. Return the result (do NOT save to state here)
        return result

    def _execute_in_docker_isolated(
        self,
        task: "TaskOperatorModel",
        state: "WorkflowState",
        registry: "ToolRegistry",
        bulkhead_manager: Optional["BulkheadManager"],
        resource_manager: Optional["ContainerResourceManager"],
        workflow_run_id: Optional[str],
    ) -> Any:
        """
        Execute the Python function inside a Docker container with highway_core available.
        For security and practicality, we'll mount the current highway_core installation
        or create a container that has highway_core pre-installed.
        """
        if not task.function:
            logger.error(
                "LocalPythonExecutor: Error - Task %s has 'python' runtime but no 'function' defined.",
                task.task_id,
            )
            raise ValueError(
                f"Task {task.task_id} is missing 'function' attribute for python runtime."
            )

        # Verify the tool exists in the registry before attempting execution
        if task.function not in registry.functions:
            available_functions = list(registry.functions.keys())
            error_message = (
                f"Task {task.task_id} failed: Function '{task.function}' not found in registry. "
                f"Available functions: {available_functions}"
            )
            logger.error("LocalPythonExecutor: %s", error_message)
            raise KeyError(error_message)

        # First resolve arguments in the current environment
        resolved_args = state.resolve_templating(task.args)
        resolved_kwargs = state.resolve_templating(task.kwargs)

        # We're already confirmed NOT in Docker (checked in execute() method)
        # Proceed directly with Docker execution
        try:
            # Lazy import Docker only when needed
            import docker
            from docker.errors import APIError, ImageNotFound

            # Create a Docker client
            client = docker.from_env()
            client.ping()  # Verify connection

            # Prepare container name
            safe_workflow_id = (
                workflow_run_id if workflow_run_id is not None else "unknown-workflow"
            )
            container_name = generate_safe_container_name(
                task.task_id + "_docker", safe_workflow_id
            )

            # Run the Python script in a Docker container with highway_core mounted
            logger.info(
                "LocalPythonExecutor: Running task %s in Docker container %s with highway_core mounted",
                task.task_id,
                container_name,
            )

            # Check if highway_python_runtime:latest exists locally, if not, build it
            try:
                client.images.get("highway_python_runtime:latest")
                logger.info("Found existing highway_python_runtime:latest image")
                image_name = "highway_python_runtime:latest"
            except:
                # Image doesn't exist, build it
                logger.info("Building highway_python_runtime:latest image...")
                dockerfile_path = os.path.join(
                    os.path.dirname(
                        os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                    ),
                    "containers",
                    "Dockerfile",
                )
                client.images.build(
                    path=os.path.dirname(dockerfile_path),
                    dockerfile="Dockerfile",
                    tag="highway_python_runtime:latest",
                )
                logger.info("Built highway_python_runtime:latest image successfully")
                image_name = "highway_python_runtime:latest"

            logger.info(f"Using Python runtime image: {image_name}")

            # Create a Python script that calls the function
            # Instead of trying to install highway_core in the container at runtime,
            # we'll create a script that assumes highway_core is available (e.g. in a custom image)
            args_repr = repr(resolved_args)
            kwargs_repr = repr(resolved_kwargs)

            # Script that assumes highway_core is already available in the container
            script_content = f"""
import sys
import json
import os

# Import highway_core components
from highway_core.tools.registry import ToolRegistry

# Initialize the registry
registry = ToolRegistry()

# Get the function from the registry
try:
    tool_func = registry.get_function("{task.function}")
    
    # Execute the function with provided arguments
    args = {args_repr}
    kwargs = {kwargs_repr}
    
    # Handle special functions that need state injection
    if "{task.function}" in ["tools.memory.set", "tools.memory.increment"]:
        # For these functions, we need to provide a state object
        # Create a minimal state-like object for the container execution
        class MockState:
            def __init__(self):
                self.variables = {{}}
                self.results = {{}}
                self.memory = {{}}  # Add memory attribute for memory functions
            
            def set_result(self, key, value):
                self.results[key] = value
                
            def set_variable(self, key, value):
                self.variables[key] = value
        
        mock_state = MockState()
        # Insert the state object as the first argument
        args = [mock_state] + args
    
    # Execute and return result
    result = tool_func(*args, **kwargs)
    print(json.dumps({{"success": True, "result": result}}))
except Exception as e:
    print(json.dumps({{"success": False, "error": str(e), "traceback": __import__('traceback').format_exc()}}))
"""

            # For highway_python_runtime:latest, highway_core is already installed in the image
            full_script = script_content

            # Run the script in the runtime image
            result_bytes = client.containers.run(
                image=image_name,
                command=["python", "-c", full_script],
                remove=True,  # Remove container after execution
                stdout=True,
                stderr=True,
                detach=False,
            )

            # Decode the output
            output_str = result_bytes.decode("utf-8").strip()
            logger.info(
                "LocalPythonExecutor: Task %s Docker execution output: %s",
                task.task_id,
                output_str,
            )

            # Extract the JSON result from the output (pip logs may precede the result)
            try:
                # Find the JSON part in the output
                import json

                lines = output_str.strip().split("\n")

                # Look for the last line that is valid JSON (likely the result)
                json_line = None
                for line in reversed(lines):
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            json_obj = json.loads(line)
                            if isinstance(json_obj, dict):  # Valid JSON object
                                json_line = line
                                break
                        except json.JSONDecodeError:
                            continue

                if json_line:
                    result_obj = json.loads(json_line)
                    if result_obj.get("success"):
                        return result_obj["result"]
                    else:
                        error_msg = result_obj.get(
                            "error", "Unknown error in Docker container"
                        )
                        traceback_info = result_obj.get("traceback", "")
                        logger.error(
                            "LocalPythonExecutor: Error from Docker execution: %s. Traceback: %s",
                            error_msg,
                            traceback_info,
                        )
                        raise RuntimeError(f"Docker execution failed: {error_msg}")
                else:
                    logger.error(
                        "LocalPythonExecutor: Could not find JSON result in output: %s",
                        output_str,
                    )
                    raise RuntimeError(
                        f"Could not parse result from Docker execution: {output_str}"
                    )
            except json.JSONDecodeError as e:
                # If it's not JSON, it might be raw output or an error
                logger.warning(
                    "LocalPythonExecutor: Non-JSON output from Docker container: %s. Error: %s",
                    output_str,
                    e,
                )
                return output_str

        except ImageNotFound:
            logger.error("LocalPythonExecutor: Python Docker image not found")
            raise
        except APIError as e:
            logger.error("LocalPythonExecutor: Docker API error - %s", e)
            raise
        except Exception as e:
            logger.error("LocalPythonExecutor: Error running in Docker - %s", e)
            raise
