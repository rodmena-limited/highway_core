# highway_core/engine/executors/docker.py
import logging
from typing import Any, Optional, TYPE_CHECKING
import docker
from docker.errors import ImageNotFound, APIError

from highway_core.engine.executors.base import BaseExecutor

from highway_core.utils.naming import generate_safe_container_name

if TYPE_CHECKING:
    from highway_core.engine.models import TaskOperatorModel
    from highway_core.engine.state import WorkflowState
    from highway_core.tools.registry import ToolRegistry
    from highway_core.tools.bulkhead import BulkheadManager
    from highway_core.engine.resource_manager import ContainerResourceManager

logger = logging.getLogger(__name__)


class DockerExecutor(BaseExecutor):
    """
    Executes tasks as Docker containers.
    """

    def __init__(self) -> None:
        try:
            self.client = docker.from_env()
            self.client.ping()
            logger.info("DockerExecutor: Connected to Docker daemon.")
        except Exception as e:
            logger.error("DockerExecutor: Failed to connect to Docker daemon: %s", e)
            raise ConnectionError(f"Failed to connect to Docker daemon: {e}")

    def execute(
        self,
        task: "TaskOperatorModel",
        state: "WorkflowState",
        registry: "ToolRegistry",  # Ignored
        bulkhead_manager: Optional["BulkheadManager"],  # Ignored
        resource_manager: Optional["ContainerResourceManager"],
        workflow_run_id: Optional[str],
    ) -> Any:
        if not task.image:
            raise ValueError(f"Docker task {task.task_id} is missing 'image'.")

        # 1. Resolve templating in command
        resolved_command = state.resolve_templating(task.command)

        image_name = task.image
        container_name = generate_safe_container_name(task.task_id, workflow_run_id)

        logger.info(
            "DockerExecutor: Running task %s in container %s (%s)",
            task.task_id,
            container_name,
            image_name,
        )

        try:
            # 2. Pull the image
            logger.info("DockerExecutor: Pulling image %s...", image_name)
            self.client.images.pull(image_name)

            # 3. Run the container
            logger.info(
                "DockerExecutor: Running container with command: %s", resolved_command
            )
            container = self.client.containers.run(
                image=image_name,
                command=resolved_command,
                name=container_name,
                detach=True,  # Run in detached mode
                remove=False,  # Don't remove automatically
                stdout=True,
                stderr=True,
            )
            resource_manager.register_container(container.id, container_name)

            # Wait for the container to finish
            result = container.wait()

            # 4. Get output
            output = container.logs().decode("utf-8").rstrip()

            # 5. Remove the container
            container.remove()

            logger.info(
                "DockerExecutor: Task %s output (stripped):\n%s", task.task_id, output
            )

            # Return the cleaned log output as the result
            return output

        except ImageNotFound:
            logger.error("DockerExecutor: Image not found - %s", image_name)
            raise
        except APIError as e:
            logger.error("DockerExecutor: Docker API error - %s", e)
            raise
        except Exception as e:
            logger.error("DockerExecutor: Unknown error running container - %s", e)
            raise
