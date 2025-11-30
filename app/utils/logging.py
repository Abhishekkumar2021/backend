"""Centralized logging configuration for the application.

This module provides a reusable logging setup that can be used across all modules.
Supports console and file-based logging with automatic rotation.
"""

import logging
import sys
from pathlib import Path
from typing import Optional
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler


# Global flag to prevent duplicate initialization
_logging_initialized = False


def setup_logging(
    log_dir: str | Path = "logs",
    log_level: int = logging.INFO,
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5,
    enable_console: bool = True,
    enable_file: bool = True,
    enable_daily: bool = True,
) -> None:
    """Configure logging with both console and file handlers.

    Creates up to three log files:
    - logs/app.log: All logs (INFO and above) with size rotation
    - logs/error.log: Only ERROR and CRITICAL logs
    - logs/app_daily.log: Daily rotating logs (keeps 30 days)

    Args:
        log_dir: Directory to store log files (default: "logs")
        log_level: Minimum log level (default: INFO)
        max_bytes: Maximum size per log file in bytes (default: 10MB)
        backup_count: Number of backup files to keep (default: 5)
        enable_console: Enable console output (default: True)
        enable_file: Enable file logging (default: True)
        enable_daily: Enable daily rotation (default: True)

    Example:
        >>> from app.utils.logging import setup_logging, get_logger
        >>> setup_logging()  # Initialize once in main.py
        >>> logger = get_logger(__name__)  # Use in any module
        >>> logger.info("Hello from my module!")
    """
    global _logging_initialized

    if _logging_initialized:
        logging.getLogger("data_agent").debug(
            "Logging already initialized, skipping setup"
        )
        return

    # Create logs directory
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)

    # Define log format
    log_format = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    # Create formatter
    formatter = logging.Formatter(log_format, datefmt=date_format)

    # Root logger configuration
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove existing handlers to avoid duplicates
    root_logger.handlers.clear()

    # 1. Console Handler (stdout)
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    # 2. File Handler - All logs with size rotation
    if enable_file:
        file_handler = RotatingFileHandler(
            log_path / "app.log",
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

        # 3. Error File Handler - Only errors
        error_handler = RotatingFileHandler(
            log_path / "error.log",
            maxBytes=max_bytes // 2,  # Smaller for errors only
            backupCount=max(3, backup_count // 2),
            encoding="utf-8",
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        root_logger.addHandler(error_handler)

    # 4. Daily Rotating Handler
    if enable_daily:
        daily_handler = TimedRotatingFileHandler(
            log_path / "app_daily.log",
            when="midnight",
            interval=1,
            backupCount=30,
            encoding="utf-8",
        )
        daily_handler.setLevel(log_level)
        daily_handler.setFormatter(formatter)
        daily_handler.suffix = "%Y-%m-%d"
        root_logger.addHandler(daily_handler)

    # 5. Reduce noise from external libraries
    _configure_external_loggers()

    # Mark as initialized
    _logging_initialized = True

    # Log the setup completion
    logger = logging.getLogger("data_agent")
    logger.info("📝 Logging initialized - writing to %s/ directory", log_dir)
    if enable_file:
        logger.info(
            "   • app.log: All logs (rotating, %dMB max, %d backups)",
            max_bytes // (1024 * 1024),
            backup_count,
        )
        logger.info(
            "   • error.log: Errors only (rotating, %dMB max, %d backups)",
            (max_bytes // 2) // (1024 * 1024),
            max(3, backup_count // 2),
        )
    if enable_daily:
        logger.info("   • app_daily.log: Daily rotation (30 days retention)")


def _configure_external_loggers() -> None:
    """Reduce noise from external libraries."""
    external_loggers = [
        "uvicorn.access",
        "uvicorn.error",
        "sqlalchemy.engine",
        "sqlalchemy.pool",
        "boto3",
        "botocore",
        "urllib3",
        "s3transfer",
        "asyncio",
        "aiobotocore",
        "werkzeug",
    ]

    for logger_name in external_loggers:
        logging.getLogger(logger_name).setLevel(logging.WARNING)


def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """Get a logger instance for a specific module.

    Args:
        name: Logger name (typically __name__ of the module)
        level: Optional log level override for this logger

    Returns:
        Configured logger instance

    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Hello from %s", __name__)

        >>> # Override level for specific module
        >>> debug_logger = get_logger(__name__, level=logging.DEBUG)
        >>> debug_logger.debug("Detailed debug info")
    """
    logger = logging.getLogger(name)

    if level is not None:
        logger.setLevel(level)

    return logger


def set_log_level(level: int, logger_name: Optional[str] = None) -> None:
    """Change log level dynamically at runtime.

    Args:
        level: New log level (logging.DEBUG, INFO, WARNING, ERROR, CRITICAL)
        logger_name: Specific logger to update (None = root logger)

    Example:
        >>> # Enable debug logging for entire app
        >>> set_log_level(logging.DEBUG)

        >>> # Enable debug only for specific module
        >>> set_log_level(logging.DEBUG, "app.connectors.postgresql")
    """
    if logger_name:
        logger = logging.getLogger(logger_name)
    else:
        logger = logging.getLogger()

    logger.setLevel(level)
    logging.getLogger("data_agent").info(
        "Log level changed to %s for %s",
        logging.getLevelName(level),
        logger_name or "root",
    )


def log_function_call(logger: logging.Logger):
    """Decorator to log function entry/exit.

    Args:
        logger: Logger instance to use

    Example:
        >>> logger = get_logger(__name__)
        >>>
        >>> @log_function_call(logger)
        >>> def my_function(x, y):
        >>>     return x + y
        >>>
        >>> result = my_function(1, 2)
        >>> # Logs: "Entering my_function(1, 2)"
        >>> # Logs: "Exiting my_function -> 3"
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            func_name = func.__name__
            args_repr = [repr(a) for a in args]
            kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
            signature = ", ".join(args_repr + kwargs_repr)

            logger.debug("Entering %s(%s)", func_name, signature)

            try:
                result = func(*args, **kwargs)
                logger.debug("Exiting %s -> %s", func_name, repr(result))
                return result
            except Exception as e:
                logger.exception("Exception in %s: %s", func_name, e)
                raise

        return wrapper

    return decorator


def reset_logging() -> None:
    """Reset logging configuration (useful for testing).

    Warning: This removes all handlers and resets the initialization flag.
    Should only be used in test environments.
    """
    global _logging_initialized

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    _logging_initialized = False

    logging.getLogger("data_agent").debug("Logging configuration reset")


# Convenience function for common log patterns
class LogContext:
    """Context manager for temporary log level changes.

    Example:
        >>> logger = get_logger(__name__)
        >>>
        >>> with LogContext(logging.DEBUG):
        >>>     logger.debug("This will be logged")
        >>>     # ... debug code ...
        >>>
        >>> logger.debug("This won't be logged (back to INFO)")
    """

    def __init__(self, level: int, logger_name: Optional[str] = None):
        """Initialize context with target level.

        Args:
            level: Temporary log level
            logger_name: Specific logger to modify (None = root)
        """
        self.level = level
        self.logger_name = logger_name
        self.original_level: Optional[int] = None

    def __enter__(self):
        """Enter context - change log level."""
        if self.logger_name:
            logger = logging.getLogger(self.logger_name)
        else:
            logger = logging.getLogger()

        self.original_level = logger.level
        logger.setLevel(self.level)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context - restore original level."""
        if self.logger_name:
            logger = logging.getLogger(self.logger_name)
        else:
            logger = logging.getLogger()

        if self.original_level is not None:
            logger.setLevel(self.original_level)


# Export commonly used items
__all__ = [
    "setup_logging",
    "get_logger",
    "set_log_level",
    "log_function_call",
    "reset_logging",
    "LogContext",
]
