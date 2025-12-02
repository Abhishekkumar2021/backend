"""
Structured logging configuration for ETL Engine + FastAPI + Celery.

Features:
- JSON logs to file
- Pretty console logs (dev mode)
- Context-aware structured logging using structlog + contextvars
- Safe trace ID propagation across request/worker/pipeline
"""

import logging
import sys
from pathlib import Path
from typing import Any, Dict

import structlog
from structlog.processors import JSONRenderer
from structlog.stdlib import add_log_level
from structlog.contextvars import (
    merge_contextvars,
    bind_contextvars,
    clear_contextvars,
)
from structlog.dev import ConsoleRenderer
from logging.handlers import RotatingFileHandler

# Allowed trace fields for contextvars (prevents accidental leakage)
TRACE_ID_KEYS = {
    "request_id",
    "pipeline_id",
    "pipeline_run_id",
    "operator_run_id",
    "task_id",
    "job_id",
}


# =============================================================================
# INTERNAL: structlog processor chains
# =============================================================================

def _build_console_processors() -> list:
    """Pretty console logs for development."""
    return [
        merge_contextvars,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        ConsoleRenderer(colors=True),
    ]


def _build_json_processors() -> list:
    """JSON logs for file handlers."""
    return [
        merge_contextvars,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        JSONRenderer(),
    ]


# =============================================================================
# PUBLIC: Logging setup
# =============================================================================

def setup_logging(
    log_level: str = "INFO",
    log_dir: str = "logs",
    json_logs: bool = False,
) -> None:
    """
    Main logging initialization.
    - Console: pretty or JSON (based on json_logs)
    - File: ALWAYS JSON logs
    """

    Path(log_dir).mkdir(parents=True, exist_ok=True)

    # ----------------------------
    # 1. Stdlib logging handlers
    # ----------------------------
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(log_level.upper())

    # Console handler (pretty or JSON)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level.upper())
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    root_logger.addHandler(console_handler)

    # JSON file logs
    file_handler = RotatingFileHandler(
        f"{log_dir}/application.log",
        maxBytes=8 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(log_level.upper())
    file_handler.setFormatter(logging.Formatter("%(message)s"))
    root_logger.addHandler(file_handler)

    # ----------------------------
    # 2. structlog configuration
    # ----------------------------
    processors = (
        _build_json_processors() if json_logs else _build_console_processors()
    )

    structlog.configure(
        processors=processors,
        context_class=dict,
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(log_level.upper())
        ),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


# =============================================================================
# PUBLIC: Trace-ID helpers
# =============================================================================

def bind_trace_ids(**ids: Any) -> None:
    """
    Bind trace IDs into structlog contextvars.
    Only whitelisted keys are stored.
    """
    filtered: Dict[str, Any] = {
        k: v for k, v in ids.items() if k in TRACE_ID_KEYS and v is not None
    }
    if filtered:
        bind_contextvars(**filtered)


def clear_trace_ids() -> None:
    """Clear only allowed IDs from contextvars (keeps other metadata untouched)."""
    for key in TRACE_ID_KEYS:
        bind_contextvars(**{key: None})
    clear_contextvars()


def get_logger(name: str):
    """Retrieve a structured logger with trace IDs attached."""
    return structlog.get_logger(name)
