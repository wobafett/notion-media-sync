import logging
import os
import sys
from typing import Optional

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
_LOGGER_NAME = "notion_sync"


def setup_logging(log_file: str = "notion_sync.log") -> logging.Logger:
    """Configure a consistent logger for all sync scripts."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger(_LOGGER_NAME)

    if logger.handlers:
        logger.setLevel(getattr(logging, log_level, logging.INFO))
        return logger

    logger.setLevel(getattr(logging, log_level, logging.INFO))
    formatter = logging.Formatter(LOG_FORMAT)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False

    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Get a child logger from the shared configuration."""
    parent = logging.getLogger(_LOGGER_NAME)
    if not parent.handlers:
        setup_logging()
    return parent.getChild(name) if name else parent


