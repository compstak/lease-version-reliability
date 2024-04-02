import logging
import sys
from typing import Any

import structlog


def get_console_configs() -> list[Any]:
    """Setup console renderer configuration."""
    logging.basicConfig(
        level=logging.DEBUG,
        stream=sys.stdout,
        format="%(message)s",
    )
    return [
        structlog.dev.ConsoleRenderer(),
    ]


def get_json_configs() -> list[Any]:
    """Setup json renderer configuration."""
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format="%(message)s",
    )
    return [
        structlog.processors.format_exc_info,
        structlog.processors.dict_tracebacks,
        structlog.processors.JSONRenderer(),
    ]


def set_dependency_log_level() -> None:
    """Adjust log level for chatty dependencies"""
    logging.getLogger("boto").setLevel(logging.CRITICAL)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    logging.getLogger("s3transfer").setLevel(logging.CRITICAL)
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)
    logging.getLogger("snowflake.connector").setLevel(logging.CRITICAL)
    logging.getLogger("snowflake.connector.connection").propagate = False


def configure_logging(isatty: bool = sys.__stdin__.isatty()) -> None:
    """Initialize logging system."""
    set_dependency_log_level()

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.PositionalArgumentsFormatter(),
    ]

    if not isatty:
        processors = shared_processors + get_json_configs()
    else:
        processors = shared_processors + get_console_configs()

    structlog.configure(
        processors=processors,  # type: ignore
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def initialize_logging(
    is_configured: bool = structlog.is_configured(),
) -> None:
    """Initialize logging system if not configured"""
    if not is_configured:
        configure_logging()
