"""
Application configuration using Pydantic settings.
Optimized for Universal ETL + Celery + Monitoring.
"""

from typing import List, Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # =============================================================
    # Application
    # =============================================================
    APP_NAME: str = "Data Agent - Universal ETL"
    ENVIRONMENT: str = Field(
        default="development",
        pattern="^(development|staging|production)$",
    )
    DEBUG: bool = False

    UNIVERSAL_ETL_MODE: str = Field(
        default="standard",  # standard | strict | debug
        pattern="^(standard|strict|debug)$"
    )

    # =============================================================
    # API
    # =============================================================
    API_V1_PREFIX: str = "/api/v1"

    # =============================================================
    # Database
    # =============================================================
    DATABASE_URL: str

    # =============================================================
    # Redis / Celery
    # =============================================================
    REDIS_URL: str = "redis://localhost:6379/0"
    CELERY_BROKER_URL: Optional[str] = None
    CELERY_RESULT_BACKEND: Optional[str] = None

    CELERY_MAX_RETRIES: int = 3
    CELERY_TASK_SOFT_TIMEOUT: int = 300  # seconds
    CELERY_TASK_HARD_TIMEOUT: int = 350
    CELERY_WORKER_CONCURRENCY: int = 4

    ENABLE_SCHEDULER: bool = True

    # =============================================================
    # Universal ETL: pipeline runtime config
    # =============================================================
    PIPELINE_DEFAULT_BATCH_SIZE: int = 5000
    PIPELINE_MAX_PARALLEL_OPERATORS: int = 5
    PIPELINE_RUN_TIMEOUT: int = 3600

    # Logging enrichment
    INCLUDE_TRACE_IDS: bool = True

    # =============================================================
    # Security
    # =============================================================
    MASTER_PASSWORD: Optional[str] = None

    # =============================================================
    # CORS
    # =============================================================
    ALLOWED_ORIGINS: List[str] = ["*"]

    # =============================================================
    # Logging
    # =============================================================
    LOG_LEVEL: str = Field(
        default="INFO",
        pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$",
    )
    JSON_LOGS: bool = False
    LOG_DIR: str = "logs"

    # Structured log metadata
    LOG_INCLUDE_TIMESTAMP: bool = True
    LOG_INCLUDE_PROCESS_ID: bool = True
    LOG_INCLUDE_THREAD_ID: bool = False

    # =============================================================
    # Plugin Registry (Extensible)
    # =============================================================
    SOURCE_REGISTRY_PATH: str = "app.connectors.sources"
    DESTINATION_REGISTRY_PATH: str = "app.connectors.destinations"
    TRANSFORMER_REGISTRY_PATH: str = "app.pipeline.processors"

    # =============================================================
    # Optional: Streaming Systems (Kafka, etc.)
    # =============================================================
    ENABLE_EVENT_STREAMING: bool = False
    KAFKA_BROKERS: Optional[str] = None

    # =============================================================
    # Validators
    # =============================================================
    @field_validator("ALLOWED_ORIGINS", mode="before")
    @classmethod
    def parse_cors_origins(cls, v):
        if isinstance(v, str):
            if v == "*":
                return ["*"]
            return [i.strip() for i in v.split(",")]
        return v

    @field_validator("LOG_LEVEL", mode="before")
    @classmethod
    def uppercase_log_level(cls, v):
        return v.upper() if isinstance(v, str) else v

    # =============================================================
    # Utility
    # =============================================================
    @property
    def celery_broker(self) -> str:
        return self.CELERY_BROKER_URL or self.REDIS_URL

    @property
    def celery_backend(self) -> str:
        return self.CELERY_RESULT_BACKEND or self.REDIS_URL

    def validate_master_password(self) -> None:
        if not self.MASTER_PASSWORD:
            raise ValueError("MASTER_PASSWORD is required for this operation.")


settings = Settings()