"""Application configuration using Pydantic settings.

Configuration is loaded from environment variables with validation.
"""

from typing import List, Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings with validation.
    
    All settings can be configured via environment variables.
    See .env.example for configuration template.
    """
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # Ignore extra fields in .env
    )
    
    # ========== Application ==========
    APP_NAME: str = "Data Agent - Universal ETL"
    ENVIRONMENT: str = Field(
        default="development",
        pattern="^(development|staging|production)$",
        description="Application environment",
    )
    DEBUG: bool = Field(
        default=False,
        description="Enable debug mode",
    )
    
    # ========== API ==========
    API_V1_PREFIX: str = Field(
        default="/api/v1",
        description="API version prefix",
    )
    
    # ========== Database ==========
    DATABASE_URL: str = Field(
        ...,
        description="PostgreSQL connection string",
        examples=["postgresql://user:pass@localhost:5432/dbname"],
    )
    
    # ========== Redis ==========
    REDIS_URL: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection string for caching and Celery",
    )
    
    # ========== Security ==========
    # IMPORTANT: MASTER_PASSWORD is optional at startup
    # It's required only when the system is actually used
    # Celery workers get it from environment at task execution time
    MASTER_PASSWORD: Optional[str] = Field(
        default=None,
        description="Master password for encryption (required for operations)",
    )
    
    # ========== CORS ==========
    ALLOWED_ORIGINS: List[str] = Field(
        default=["*"],
        description="Allowed CORS origins",
    )
    
    # ========== Logging ==========
    LOG_LEVEL: str = Field(
        default="INFO",
        pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$",
        description="Logging level",
    )
    
    JSON_LOGS: bool = Field(
        default=False,
        description="Enable JSON logging format (recommended for production)",
    )
    
    LOG_DIR: str = Field(
        default="logs",
        description="Directory for log files",
    )
    
    # ========== Celery ==========
    CELERY_BROKER_URL: Optional[str] = Field(
        default=None,
        description="Celery broker URL (defaults to REDIS_URL)",
    )
    
    CELERY_RESULT_BACKEND: Optional[str] = Field(
        default=None,
        description="Celery result backend URL (defaults to REDIS_URL)",
    )
    
    # ========== Feature Flags ==========
    ENABLE_CACHE: bool = Field(
        default=True,
        description="Enable Redis caching",
    )
    
    ENABLE_SCHEDULER: bool = Field(
        default=True,
        description="Enable Celery Beat scheduler",
    )
    

    @field_validator("ALLOWED_ORIGINS", mode="before")
    @classmethod
    def parse_cors_origins(cls, v):
        """Parse comma-separated CORS origins.
        
        Args:
            v: CORS origins (string or list)
            
        Returns:
            List of origin strings
        """
        if isinstance(v, str):
            if v == "*":
                return ["*"]
            return [origin.strip() for origin in v.split(",")]
        return v
    
    @field_validator("LOG_LEVEL", mode="before")
    @classmethod
    def uppercase_log_level(cls, v):
        """Ensure log level is uppercase.
        
        Args:
            v: Log level string
            
        Returns:
            Uppercase log level
        """
        if isinstance(v, str):
            return v.upper()
        return v
    
    @property
    def is_production(self) -> bool:
        """Check if running in production.
        
        Returns:
            True if production environment
        """
        return self.ENVIRONMENT == "production"
    
    @property
    def is_development(self) -> bool:
        """Check if running in development.
        
        Returns:
            True if development environment
        """
        return self.ENVIRONMENT == "development"
    
    @property
    def celery_broker(self) -> str:
        """Get Celery broker URL.
        
        Returns:
            Celery broker URL (defaults to REDIS_URL)
        """
        return self.CELERY_BROKER_URL or self.REDIS_URL
    
    @property
    def celery_backend(self) -> str:
        """Get Celery result backend URL.
        
        Returns:
            Celery backend URL (defaults to REDIS_URL)
        """
        return self.CELERY_RESULT_BACKEND or self.REDIS_URL
    
    def validate_master_password(self) -> None:
        """Validate that master password is set.
        
        This should be called before performing operations that require
        the master password (e.g., creating connections, running pipelines).
        
        Raises:
            ValueError: If master password is not set
        """
        if not self.MASTER_PASSWORD:
            raise ValueError(
                "MASTER_PASSWORD is required for this operation. "
                "Please set it in your environment variables."
            )

settings = Settings()