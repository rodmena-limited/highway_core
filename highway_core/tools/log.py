import logging
from .decorators import tool

logger = logging.getLogger("HighwayEngine")


@tool("tools.log.info")
def info(message: str) -> None:
    """Logs a message at the INFO level."""
    logger.info(message)


@tool("tools.log.error")
def error(message: str) -> None:
    """Logs a message at the ERROR level."""
    logger.error(message)
