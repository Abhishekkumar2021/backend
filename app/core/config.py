from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Database
    DATABASE_URL: str = "postgresql://user:password@localhost:5432/data_agent_system"
    
    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"
    
    # API
    API_V1_PREFIX: str = "/api/v1"
    APP_NAME: str = "Data Agent - Universal ETL"
    
    # CORS
    ALLOWED_ORIGINS: str = "*"
    ALLOWED_METHODS: str = "*"
    ALLOWED_HEADERS: str = "*"
    
    # Environment
    ENVIRONMENT: str = "development"  # Options: development, staging, production
    
    
    class Config:
        env_file = ".env"

settings = Settings()