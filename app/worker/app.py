"""
Celery application configuration and initialization.

Responsibilities:
- Create Celery app instance
- Apply settings from CeleryConfig
- Auto-discover tasks
- Register unified structured signal logging
"""

from celery import Celery
from celery.signals import (
    task_prerun,
    task_postrun,
    task_success,
    task_failure,
    task_retry,
    worker_ready,
    worker_shutdown,
)

from app.worker.config import CeleryConfig
from app.core.logging import get_logger
from app.connectors import factory  # ensures connectors are registered

logger = get_logger(__name__)

# =============================================================================
# Celery Instance
# =============================================================================

celery_app = Celery(
    "data_agent",
    broker=CeleryConfig.BROKER_URL,
    backend=CeleryConfig.RESULT_BACKEND,
    include=[
        "app.worker.tasks.pipeline_tasks",
    ],
)

celery_app.conf.update(CeleryConfig.to_dict())

logger.info(
    "celery_initialized",
    broker=CeleryConfig.BROKER_URL,
    backend=CeleryConfig.RESULT_BACKEND,
    beat_scheduler=CeleryConfig.BEAT_SCHEDULER,
)


# =============================================================================
# Signal Helpers
# =============================================================================

def _safe_preview(data, limit=200):
    """Safely preview long or complex values without logging huge payloads."""
    try:
        s = str(data)
        return s[:limit] + ("..." if len(s) > limit else "")
    except Exception:
        return "<unserializable>"


# =============================================================================
# Task Signals
# =============================================================================

@task_prerun.connect
def task_prerun_handler(task_id, task, args=None, kwargs=None, **extras):
    """Structured log at the start of a task."""
    logger.info(
        "task_started",
        task_id=task_id,
        task_name=task.name,
        args=_safe_preview(args),
        kwargs=_safe_preview(kwargs),
    )


@task_postrun.connect
def task_postrun_handler(task_id, task, retval=None, **extras):
    """Structured log after task finishes (success or failure)."""
    logger.info(
        "task_finished",
        task_id=task_id,
        task_name=task.name,
        result=_safe_preview(retval),
    )


@task_success.connect
def task_success_handler(sender=None, result=None, **extras):
    """Structured log for successful task."""
    logger.info(
        "task_success",
        task_name=sender.name if sender else None,
        result=_safe_preview(result),
    )


@task_failure.connect
def task_failure_handler(task_id, exception, args=None, kwargs=None, traceback=None, **extras):
    """Structured log on task failure."""
    logger.error(
        "task_failed",
        task_id=task_id,
        exception=str(exception),
        exception_type=exception.__class__.__name__,
        args=_safe_preview(args),
        kwargs=_safe_preview(kwargs),
        exc_info=True,
    )


@task_retry.connect
def task_retry_handler(task_id, reason, **extras):
    """Structured log for retry event."""
    logger.warning(
        "task_retry",
        task_id=task_id,
        reason=str(reason),
    )


# =============================================================================
# Worker Lifecycle Signals
# =============================================================================

@worker_ready.connect
def worker_ready_handler(sender=None, **extras):
    """Log when worker is fully booted."""
    logger.info(
        "worker_started",
        hostname=getattr(sender, "hostname", "unknown"),
    )


@worker_shutdown.connect
def worker_shutdown_handler(sender=None, **extras):
    """Log when worker is shutting down."""
    logger.info(
        "worker_stopped",
        hostname=getattr(sender, "hostname", "unknown"),
    )
