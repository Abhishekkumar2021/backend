"""Structured logging configuration using structlog."""

import logging
import sys
from pathlib import Path
from typing import Any

import structlog
from structlog.processors import JSONRenderer
from structlog.stdlib import add_log_level


def setup_logging(
    log_level: str = "INFO",
    log_dir: str = "logs",
    json_logs: bool = False,
) -> None:
    """Configure structured logging with structlog.

    Args:
        log_level: Minimum log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir: Directory for log files
        json_logs: If True, output JSON format (for production)
    """
    # Create logs directory
    Path(log_dir).mkdir(exist_ok=True)

    # Configure structlog processors
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if json_logs:
        # Production: JSON logs
        processors = shared_processors + [
            structlog.processors.dict_tracebacks,
            JSONRenderer(),
        ]
    else:
        # Development: Human-readable logs
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(colors=True),
        ]
        

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            log_level.lower()
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    """Get a structured logger instance.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Structured logger instance

    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("user_login", user_id=123, ip="192.168.1.1")
    """
    return structlog.get_logger(name)
