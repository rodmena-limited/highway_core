import logging
from .decorators import tool

# Configure a single, shared logger for the engine
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [HighwayEngine] - %(message)s",
)
logger = logging.getLogger("HighwayEngine")


@tool("tools.log.info")
def info(message: str) -> None:
    """Logs a message at the INFO level."""
    logger.info(message)


@tool("tools.log.error")
def error(message: str) -> None:
    """Logs a message at the ERROR level."""
    logger.error(message)
