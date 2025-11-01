import logging

# Configure a single, shared logger for the engine
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [HighwayEngine] - %(message)s",
)
logger = logging.getLogger("HighwayEngine")


def info(message: str) -> None:
    """Logs a message at the INFO level."""
    logger.info(message)


def error(message: str) -> None:
    """Logs a message at the ERROR level."""
    logger.error(message)
