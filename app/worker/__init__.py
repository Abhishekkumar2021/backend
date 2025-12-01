"""Celery worker module for background task processing.

This module contains:
- Celery application configuration
- Task definitions for pipeline execution
- Database-driven scheduler for dynamic pipeline scheduling
"""

from app.worker.app import celery_app

__all__ = ["celery_app"]