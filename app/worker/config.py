"""Celery worker configuration settings.

Centralizes all Celery-specific configuration separate from
the main application config.
"""

from typing import Dict, Any
from app.core.config import settings


class CeleryConfig:
    """Celery configuration class.
    
    Uses main application settings as defaults but allows
    Celery-specific overrides.
    """
    
    # Broker settings - use computed properties from settings
    BROKER_URL: str = settings.celery_broker
    RESULT_BACKEND: str = settings.celery_backend
    
    # Task serialization
    TASK_SERIALIZER: str = "json"
    RESULT_SERIALIZER: str = "json"
    ACCEPT_CONTENT: list[str] = ["json"]
    
    # Timezone settings
    TIMEZONE: str = "UTC"
    ENABLE_UTC: bool = True
    
    # Task execution settings
    TASK_TRACK_STARTED: bool = True
    TASK_ACKS_LATE: bool = True  # Acknowledge after completion (safer)
    TASK_REJECT_ON_WORKER_LOST: bool = True
    
    # Worker settings
    WORKER_PREFETCH_MULTIPLIER: int = 1  # Fetch one task at a time
    WORKER_MAX_TASKS_PER_CHILD: int = 1000  # Restart worker after N tasks
    WORKER_DISABLE_RATE_LIMITS: bool = False
    
    # Result settings
    RESULT_EXPIRES: int = 3600  # Results expire after 1 hour
    RESULT_PERSISTENT: bool = True
    
    # Task time limits
    TASK_TIME_LIMIT: int = 3600  # Hard limit: 1 hour
    TASK_SOFT_TIME_LIMIT: int = 3300  # Soft limit: 55 minutes
    
    # Beat scheduler settings - only if enabled
    BEAT_SCHEDULER: str = "app.worker.scheduler:DatabaseScheduler" if settings.ENABLE_SCHEDULER else None
    BEAT_MAX_LOOP_INTERVAL: int = 5  # Poll database every 5 seconds
    BEAT_SCHEDULE_FILENAME: str = "celerybeat-schedule.db"
    
    # Task routes
    TASK_ROUTES: Dict[str, Dict[str, str]] = {
        "app.worker.tasks.pipeline_tasks.*": {"queue": "pipelines"},
    }
    
    # Task default settings
    TASK_DEFAULT_QUEUE: str = "default"
    TASK_DEFAULT_EXCHANGE: str = "default"
    TASK_DEFAULT_EXCHANGE_TYPE: str = "direct"
    TASK_DEFAULT_ROUTING_KEY: str = "default"
    
    # Logging
    WORKER_LOG_FORMAT: str = "[%(asctime)s: %(levelname)s/%(processName)s] %(message)s"
    WORKER_TASK_LOG_FORMAT: str = "[%(asctime)s: %(levelname)s/%(processName)s][%(task_name)s(%(task_id)s)] %(message)s"
    
    # Monitoring
    WORKER_SEND_TASK_EVENTS: bool = True
    TASK_SEND_SENT_EVENT: bool = True
    
    @classmethod
    def to_dict(cls) -> Dict[str, Any]:
        """Convert config to dictionary for Celery.
        
        Returns:
            Dictionary of configuration values
        """
        config = {}
        for key, value in cls.__dict__.items():
            if key.isupper() and not key.startswith("_"):
                if value is not None:
                    config[key.lower()] = value
        return config