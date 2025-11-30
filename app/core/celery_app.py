"""Celery application configuration.

Configures Celery for distributed task execution with Redis as broker/backend
and custom database-backed scheduler for dynamic pipeline scheduling.
"""

import os

from celery import Celery
from celery.signals import task_prerun, task_postrun, task_failure

from app.core.logging import get_logger

logger = get_logger(__name__)

# Get Redis URL from env or default
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# IMPORTANT FIX:
# Explicitly include the worker module so Celery registers app.worker.run_pipeline
celery_app = Celery(
    "data_agent",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["app.worker"],  # <-- THIS FIXES THE ERROR
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,  # Acknowledge after task completion (safer)
    worker_prefetch_multiplier=1,  # Fetch one task at a time (better distribution)
    result_expires=3600,  # Results expire after 1 hour
    task_time_limit=3600,  # Hard limit: 1 hour per task
    task_soft_time_limit=3300,  # Soft limit: 55 minutes (warn before kill)
    worker_max_tasks_per_child=1000,  # Restart worker after N tasks (prevent memory leaks)

    # Custom scheduler class
    beat_scheduler="app.core.celery_scheduler.DatabaseScheduler",
    beat_max_loop_interval=5,  # Poll for schedule changes every 5 seconds
)


# Celery Signal Handlers
@task_prerun.connect
def task_prerun_handler(task_id, task, *args, **kwargs):
    """Log when a task starts."""
    logger.info("Task started: task_id=%s, task_name=%s", task_id, task.name)


@task_postrun.connect
def task_postrun_handler(task_id, task, *args, retval=None, **kwargs):
    """Log when a task completes successfully."""
    logger.info("Task completed: task_id=%s, task_name=%s", task_id, task.name)


@task_failure.connect
def task_failure_handler(task_id, exception, *args, **kwargs):
    """Log when a task fails."""
    logger.error(
        "Task failed: task_id=%s, exception=%s",
        task_id,
        exception,
        exc_info=True
    )


logger.info("Celery app configured: broker=%s", REDIS_URL)