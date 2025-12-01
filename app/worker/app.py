"""Celery application configuration and initialization.

Creates and configures the Celery application with:
- Redis broker and backend
- Task discovery
- Signal handlers for monitoring
- Custom configuration
"""

from celery import Celery
from celery.signals import (
    task_prerun,
    task_postrun,
    task_failure,
    task_success,
    task_retry,
    worker_ready,
    worker_shutdown,
)

from app.worker.config import CeleryConfig
from app.core.logging import get_logger

logger = get_logger(__name__)

# Create Celery application
celery_app = Celery(
    "data_agent",
    broker=CeleryConfig.BROKER_URL,
    backend=CeleryConfig.RESULT_BACKEND,
    include=["app.worker.tasks.pipeline_tasks"],
)

# Apply configuration
celery_app.conf.update(CeleryConfig.to_dict())

logger.info(
    "celery_app_configured",
    broker=CeleryConfig.BROKER_URL,
    backend=CeleryConfig.RESULT_BACKEND,
    scheduler=CeleryConfig.BEAT_SCHEDULER,
)


# ===================================================================
# Celery Signal Handlers
# ===================================================================

@task_prerun.connect
def task_prerun_handler(task_id, task, args=None, kwargs=None, **extra):
    """Log when a task starts execution.
    
    Args:
        task_id: Unique task identifier
        task: Task instance
        args: Task positional arguments
        kwargs: Task keyword arguments
        extra: Additional signal data
    """
    logger.info(
        "task_started",
        task_id=task_id,
        task_name=task.name,
        args=args,
        kwargs=kwargs,
    )


@task_postrun.connect
def task_postrun_handler(task_id, task, args=None, kwargs=None, retval=None, **extra):
    """Log when a task completes (success or failure).
    
    Args:
        task_id: Unique task identifier
        task: Task instance
        args: Task positional arguments
        kwargs: Task keyword arguments
        retval: Task return value
        extra: Additional signal data
    """
    logger.info(
        "task_completed",
        task_id=task_id,
        task_name=task.name,
        return_value=str(retval)[:200] if retval else None,
    )


@task_success.connect
def task_success_handler(sender=None, result=None, **extra):
    """Log successful task completion.
    
    Args:
        sender: Task instance
        result: Task result
        extra: Additional signal data
    """
    logger.info(
        "task_success",
        task_name=sender.name if sender else "unknown",
        result=str(result)[:200] if result else None,
    )


@task_failure.connect
def task_failure_handler(task_id, exception, args=None, kwargs=None, traceback=None, **extra):
    """Log task failures with detailed error information.
    
    Args:
        task_id: Unique task identifier
        exception: Exception that caused the failure
        args: Task positional arguments
        kwargs: Task keyword arguments
        traceback: Exception traceback
        extra: Additional signal data
    """
    logger.error(
        "task_failed",
        task_id=task_id,
        exception=str(exception),
        exception_type=exception.__class__.__name__,
        args=args,
        kwargs=kwargs,
        exc_info=True,
    )


@task_retry.connect
def task_retry_handler(task_id, reason, **extra):
    """Log task retry attempts.
    
    Args:
        task_id: Unique task identifier
        reason: Retry reason
        extra: Additional signal data
    """
    logger.warning(
        "task_retrying",
        task_id=task_id,
        reason=str(reason),
    )


@worker_ready.connect
def worker_ready_handler(sender=None, **extra):
    """Log when worker is ready to accept tasks.
    
    Args:
        sender: Worker instance
        extra: Additional signal data
    """
    logger.info(
        "worker_ready",
        hostname=sender.hostname if sender else "unknown",
    )


@worker_shutdown.connect
def worker_shutdown_handler(sender=None, **extra):
    """Log when worker is shutting down.
    
    Args:
        sender: Worker instance
        extra: Additional signal data
    """
    logger.info(
        "worker_shutting_down",
        hostname=sender.hostname if sender else "unknown",
    )