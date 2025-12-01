"""Celery task definitions."""

from app.worker.tasks.pipeline_tasks import execute_pipeline

__all__ = ["execute_pipeline"]
