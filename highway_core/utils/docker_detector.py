"""
Utility functions for detecting if the code is running inside a Docker container.
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def is_running_in_docker() -> bool:
    """
    Detects if the code is running inside a Docker container using multiple methods.

    Returns:
        True if running inside Docker, False otherwise.
    """
    # Method 1: Check for Docker-specific strings in cgroup
    try:
        cgroup_path = Path("/proc/1/cgroup")
        if cgroup_path.exists():
            content = cgroup_path.read_text()
            if "docker" in content or "lxc" in content or "containerd" in content:
                logger.info("Docker detected via cgroup.")
                return True
    except (FileNotFoundError, PermissionError, OSError):
        # File may not exist on non-Linux systems or in some containerized environments
        pass

    # Method 2: Check for the presence of .dockerenv file
    try:
        if Path("/.dockerenv").exists():
            logger.info("Docker detected via /.dockerenv file.")
            return True
    except (OSError, IOError):
        pass

    # Method 3: Check environment variables that Docker sets
    import os

    if (
        os.environ.get("container") == "docker"
        or "DOCKER" in os.environ.get("container", "").upper()
    ):
        logger.info("Docker detected via environment variable.")
        return True

    # Method 4: Check if the hostname's cgroup path exists and has Docker characteristics
    try:
        self_cgroup_path = Path("/proc/self/cgroup")
        if self_cgroup_path.exists():
            content = self_cgroup_path.read_text()
            if "docker" in content or "lxc" in content or "containerd" in content:
                logger.info("Docker detected via self cgroup.")
                return True
    except (FileNotFoundError, PermissionError, OSError):
        pass

    logger.info("No Docker environment detected.")
    return False
