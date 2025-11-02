import logging
import docker
from typing import Set

from highway_core.utils.naming import generate_safe_container_name

logger = logging.getLogger(__name__)


class ContainerResourceManager:
    def __init__(self, workflow_id: str):
        self.workflow_id = workflow_id
        self.containers: Set[str] = set()
        self.networks: Set[str] = set()
        self.volumes: Set[str] = set()
        self.docker_client = docker.from_env()

    def register_container(self, container_id: str, container_name: str):
        """Track created containers"""
        self.containers.add(container_name)

    def register_network(self, network_id: str, network_name: str):
        """Track created networks"""
        self.networks.add(network_name)

    def create_isolated_network(self, base_name: str) -> str:
        """Create workflow-specific network"""
        network_name = generate_safe_container_name(base_name, self.workflow_id)
        try:
            self.docker_client.networks.create(network_name, check_duplicate=True)
            self.register_network(network_name, network_name)
            logger.info(f"Created isolated network: {network_name}")
        except docker.errors.APIError as e:
            if "already exists" in str(e):
                logger.warning(f"Network {network_name} already exists.")
            else:
                raise
        return network_name

    def cleanup_all(self):
        """Comprehensive cleanup of all workflow resources"""
        # Stop and remove containers
        for container_name in self.containers:
            try:
                container = self.docker_client.containers.get(container_name)
                container.remove(force=True)
                logger.info(f"Removed container: {container_name}")
            except docker.errors.NotFound:
                pass  # Container already gone
            except Exception as e:
                logger.error(f"Error removing container {container_name}: {e}")

        # Remove networks
        for network_name in self.networks:
            try:
                network = self.docker_client.networks.get(network_name)
                network.remove()
                logger.info(f"Removed network: {network_name}")
            except docker.errors.NotFound:
                pass  # Network already gone
            except Exception as e:
                logger.error(f"Error removing network {network_name}: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup_all()
