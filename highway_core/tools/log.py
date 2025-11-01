# --- tools/log.py ---
# Implements 'tools.log.*' functions.

import logging

# Configure a basic logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - [WORKFLOW] - %(message)s")
logger = logging.getLogger("highway_workflow")


def info(message: str):
    """Logs a message at the INFO level."""
    logger.info(message)


def error(message: str):
    """Logs a message at the ERROR level."""
    logger.error(message)
