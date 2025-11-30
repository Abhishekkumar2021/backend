"""Celery application configuration."""

import os
from celery import Celery
from celery.signals import task_prerun, task_postrun, task_failure

from app.core.logging import get_logger

logger = get_logger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery_app = Celery(
    "data_agent",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["app.celery_worker.tasks.pipeline_tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    result_expires=3600,
    task_time_limit=3600,
    task_soft_time_limit=3300,
    worker_max_tasks_per_child=1000,
    beat_scheduler="app.celery_worker.scheduler:DatabaseScheduler",
    beat_max_loop_interval=5,
)


@task_prerun.connect
def task_prerun_handler(task_id, task, *args, **kwargs):
    """Log task start."""
    logger.info("task_started", task_id=task_id, task_name=task.name)


@task_postrun.connect
def task_postrun_handler(task_id, task, *args, retval=None, **kwargs):
    """Log task completion."""
    logger.info("task_completed", task_id=task_id, task_name=task.name)


@task_failure.connect
def task_failure_handler(task_id, exception, *args, **kwargs):
    """Log task failure."""
    logger.error(
        "task_failed",
        task_id=task_id,
        exception=str(exception),
        exc_info=True,
    )