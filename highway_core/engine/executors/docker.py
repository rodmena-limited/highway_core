# highway_core/engine/executors/docker.py
import logging
from typing import Any, Optional, TYPE_CHECKING
import docker
from docker.errors import ImageNotFound, APIError

from highway_core.engine.executors.base import BaseExecutor

if TYPE_CHECKING:
    from highway_core.engine.models import TaskOperatorModel
    from highway_core.engine.state import WorkflowState
    from highway_core.tools.registry import ToolRegistry
    from highway_core.tools.bulkhead import BulkheadManager

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
    ) -> Any:
        if not task.image:
            raise ValueError(f"Docker task {task.task_id} is missing 'image'.")

        # 1. Resolve templating in command
        resolved_command = state.resolve_templating(task.command)

        image_name = task.image
        logger.info(
            "DockerExecutor: Running task %s in container %s", task.task_id, image_name
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
                detach=False,  # Run and wait
                remove=True,  # Automatically remove when done
                stdout=True,
                stderr=True,
            )

            # 4. Get output
            # container.logs() returns bytes, decode it
            output = container.decode("utf-8")
            # Strip trailing whitespace including newlines as most commands add them
            output = output.rstrip()
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
