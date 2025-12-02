"""
Celery worker configuration settings.

Centralizes all Celery-specific configuration separately from
the main application config.
"""

from typing import Dict, Any
from app.core.config import settings


class CeleryConfig:
    """
    Clean & production-ready Celery configuration.

    NOTE:
    Keys must be lower-case when passed to Celery.
    Only UPPERCASE class attributes are exported.
    """

    # ----------------------------------------------------------------------
    # Core Broker / Backend
    # ----------------------------------------------------------------------
    BROKER_URL = settings.celery_broker
    RESULT_BACKEND = settings.celery_backend

    # Serialization
    TASK_SERIALIZER = "json"
    RESULT_SERIALIZER = "json"
    ACCEPT_CONTENT = ["json"]

    # Timezone
    TIMEZONE = "UTC"
    ENABLE_UTC = True

    # ----------------------------------------------------------------------
    # Worker Behavior
    # ----------------------------------------------------------------------
    TASK_TRACK_STARTED = True          # Emits event "task-started"
    TASK_ACKS_LATE = True              # Acknowledge only after completion
    TASK_REJECT_ON_WORKER_LOST = True  # Ensures failed task is requeued

    WORKER_PREFETCH_MULTIPLIER = 1     # Consume 1 task at a time
    WORKER_MAX_TASKS_PER_CHILD = 500   # Restart worker to avoid memory leaks
    WORKER_DISABLE_RATE_LIMITS = True

    # ----------------------------------------------------------------------
    # Result Handling
    # ----------------------------------------------------------------------
    RESULT_EXPIRES = 3600              # 1 hour
    RESULT_PERSISTENT = True

    # ----------------------------------------------------------------------
    # Time Limits
    # ----------------------------------------------------------------------
    TASK_TIME_LIMIT = 3600             # Hard timeout: 60 min
    TASK_SOFT_TIME_LIMIT = 3300        # Soft timeout: 55 min

    # ----------------------------------------------------------------------
    # Beat Scheduler (Optional)
    # ----------------------------------------------------------------------
    BEAT_SCHEDULER = (
        "app.worker.scheduler:DatabaseScheduler"
        if settings.ENABLE_SCHEDULER else None
    )
    BEAT_MAX_LOOP_INTERVAL = 5
    BEAT_SCHEDULE_FILENAME = "celerybeat-schedule.db"

    # ----------------------------------------------------------------------
    # Routing
    # ----------------------------------------------------------------------
    TASK_ROUTES = {
        "app.worker.tasks.pipeline_tasks.*": {"queue": "pipelines"},
    }

    TASK_DEFAULT_QUEUE = "default"
    TASK_DEFAULT_EXCHANGE = "default"
    TASK_DEFAULT_EXCHANGE_TYPE = "direct"
    TASK_DEFAULT_ROUTING_KEY = "default"

    # ----------------------------------------------------------------------
    # Logging
    # ----------------------------------------------------------------------
    WORKER_LOG_FORMAT = (
        "[%(asctime)s: %(levelname)s/%(processName)s] %(message)s"
    )
    WORKER_TASK_LOG_FORMAT = (
        "[%(asctime)s: %(levelname)s/%(processName)s]"
        "[%(task_name)s(%(task_id)s)] %(message)s"
    )

    # ----------------------------------------------------------------------
    # Monitoring
    # ----------------------------------------------------------------------
    WORKER_SEND_TASK_EVENTS = True
    TASK_SEND_SENT_EVENT = True

    # ----------------------------------------------------------------------
    # Exporter
    # ----------------------------------------------------------------------
    @classmethod
    def to_dict(cls) -> Dict[str, Any]:
        """
        Convert uppercase config values into Celery-compatible dict.
        Celery expects lowercase keys.
        """
        output = {}
        for key, value in cls.__dict__.items():
            if key.isupper() and not key.startswith("_") and value is not None:
                output[key.lower()] = value
        return output
