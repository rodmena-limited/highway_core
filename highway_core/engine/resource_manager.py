import logging
from typing import Any, Optional, Set

from highway_core.utils.docker_detector import is_running_in_docker
from highway_core.utils.naming import generate_safe_container_name

logger = logging.getLogger(__name__)


class ContainerResourceManager:
    def __init__(self, workflow_id: str):
        self.workflow_id = workflow_id
        self.containers: Set[str] = set()
        self.networks: Set[str] = set()
        self.volumes: Set[str] = set()
        self._docker_client: Optional[Any] = None
        self._is_docker_env = is_running_in_docker()

    @property
    def docker_client(self) -> Any:
        """Lazy initialization of Docker client - only when actually needed"""
        if self._docker_client is None:
            # Check if we're in Docker environment first
            if self._is_docker_env:
                raise RuntimeError(
                    "Cannot access Docker client from inside Docker container"
                )

            # Import Docker only when needed
            import docker

            self._docker_client = docker.from_env()

            # Verify connection
            try:
                self._docker_client.ping()
                logger.info("ContainerResourceManager: Connected to Docker daemon")
            except Exception as e:
                logger.error(
                    "ContainerResourceManager: Failed to connect to Docker daemon: %s",
                    e,
                )
                raise ConnectionError(f"Failed to connect to Docker daemon: {e}")

        return self._docker_client

    def register_container(self, container_id: str, container_name: str) -> None:
        """Track created containers"""
        self.containers.add(container_name)

    def register_network(self, network_id: str, network_name: str) -> None:
        """Track created networks"""
        self.networks.add(network_name)

    def create_isolated_network(self, base_name: str) -> str:
        """Create workflow-specific network"""
        network_name = generate_safe_container_name(base_name, self.workflow_id)

        # Only proceed if we're not in Docker
        if self._is_docker_env:
            logger.warning(
                "Cannot create isolated network inside Docker container, using default network"
            )
            return "default"  # Use default Docker network instead

        try:
            self.docker_client.networks.create(network_name, check_duplicate=True)
            self.register_network(network_name, network_name)
            logger.info(f"Created isolated network: {network_name}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.warning(f"Network {network_name} already exists.")
            else:
                logger.error(f"Error creating network {network_name}: {e}")
                raise
        return network_name

    def cleanup_all(self) -> None:
        """Comprehensive cleanup of all workflow resources"""
        # Only proceed with cleanup if we're not in Docker
        if self._is_docker_env:
            logger.info("Skipping Docker resource cleanup inside Docker container")
            return

        # Stop and remove containers
        for container_name in self.containers:
            try:
                container = self.docker_client.containers.get(container_name)
                container.remove(force=True)
                logger.info(f"Removed container: {container_name}")
            except Exception as e:
                if "not found" not in str(e).lower():
                    logger.error(f"Error removing container {container_name}: {e}")

        # Remove networks
        for network_name in self.networks:
            try:
                network = self.docker_client.networks.get(network_name)
                network.remove()
                logger.info(f"Removed network: {network_name}")
            except Exception as e:
                if "not found" not in str(e).lower():
                    logger.error(f"Error removing network {network_name}: {e}")

    def __enter__(self) -> "ContainerResourceManager":
        return self

    def __exit__(self, exc_type: type, exc_val: BaseException, exc_tb: object) -> None:
        self.cleanup_all()
