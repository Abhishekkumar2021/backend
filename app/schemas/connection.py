from datetime import datetime
from typing import Any

from pydantic import BaseModel, model_validator

from app.models.database import ConnectorType
from app.schemas.connector_configs import (
    PostgresConfig,
    MySQLConfig,
    MSSQLConfig,
    OracleConfig,
    SQLiteConfig,
    MongoDBConfig,
    SnowflakeConfig,
    BigQueryConfig,
    S3Config,
    FileSystemConfig,
    SingerSourceConfig,
    SingerDestinationConfig,
)


class ConnectionBase(BaseModel):
    name: str
    connector_type: ConnectorType
    description: str | None = None
    is_source: bool = True


class ConnectionCreate(ConnectionBase):
    config: dict[str, Any]  # Raw config from user (host, password, etc.)

    @model_validator(mode="after")
    def validate_config(self) -> "ConnectionCreate":
        """Validate config based on connector_type"""
        connector_type = self.connector_type
        config = self.config

        if connector_type == ConnectorType.POSTGRESQL:
            PostgresConfig(**config)
        elif connector_type == ConnectorType.MYSQL:
            MySQLConfig(**config)
        elif connector_type == ConnectorType.MSSQL:
            MSSQLConfig(**config)
        elif connector_type == ConnectorType.ORACLE:
            OracleConfig(**config)
        elif connector_type == ConnectorType.SQLITE:
            SQLiteConfig(**config)
        elif connector_type == ConnectorType.MONGODB:
            MongoDBConfig(**config)
        elif connector_type == ConnectorType.SNOWFLAKE:
            SnowflakeConfig(**config)
        elif connector_type == ConnectorType.BIGQUERY:
            BigQueryConfig(**config)
        elif connector_type == ConnectorType.S3:
            S3Config(**config)
        elif connector_type == ConnectorType.LOCAL_FILE:
            FileSystemConfig(**config)
        elif connector_type == ConnectorType.SINGER:
            if self.is_source:
                SingerSourceConfig(**config)
            else:
                SingerDestinationConfig(**config)
        
        return self


class ConnectionUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    config: dict[str, Any] | None = None
    
    # Note: We cannot strictly validate config here because we don't know the 
    # connector_type of the existing connection without a DB lookup.


class ConnectionInDBBase(ConnectionBase):
    id: int
    last_test_at: datetime | None = None
    last_test_success: bool | None = None
    last_test_error: str | None = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class Connection(ConnectionInDBBase):
    pass


class ConnectionConfigResponse(BaseModel):
    connection_id: int
    connector_type: str
    config: dict[str, Any]


class ConnectionTestResponse(BaseModel):
    connection_id: int
    success: bool
    message: str
    metadata: dict[str, Any]
    tested_at: datetime
    cached: bool


class CacheInvalidationResponse(BaseModel):
    connection_id: int
    cache_invalidated: bool
    message: str