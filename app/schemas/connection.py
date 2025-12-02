"""
Connection schemas (clean, modern, connector-aware)

This module performs:
✓ Strong validation for connection config
✓ Automatic routing to correct Pydantic config class
✓ Separation of create/update/DB/output schemas
✓ Full compatibility with ConnectorFactory + encrypted configs
"""

from datetime import datetime
from typing import Any, Dict, Type

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

# ================================================================
# CONNECTOR CONFIG REGISTRY
# ================================================================
# Maps connector_type → (source_config_cls, destination_config_cls)
CONNECTOR_CONFIG_MAP: Dict[ConnectorType, tuple[Type[BaseModel], Type[BaseModel] | None]] = {
    ConnectorType.POSTGRESQL: (PostgresConfig, None),
    ConnectorType.MYSQL: (MySQLConfig, None),
    ConnectorType.MSSQL: (MSSQLConfig, None),
    ConnectorType.ORACLE: (OracleConfig, None),
    ConnectorType.SQLITE: (SQLiteConfig, None),
    ConnectorType.MONGODB: (MongoDBConfig, None),
    ConnectorType.SNOWFLAKE: (SnowflakeConfig, None),
    ConnectorType.BIGQUERY: (BigQueryConfig, None),
    ConnectorType.S3: (None, S3Config),
    ConnectorType.LOCAL_FILE: (FileSystemConfig, FileSystemConfig),
    ConnectorType.SINGER: (SingerSourceConfig, SingerDestinationConfig),
}


# ================================================================
# BASE MODELS
# ================================================================
class ConnectionBase(BaseModel):
    name: str
    connector_type: ConnectorType
    description: str | None = None
    is_source: bool = True


# ================================================================
# CREATE
# ================================================================
class ConnectionCreate(ConnectionBase):
    """
    User submits:  connector_type + is_source + config
    We must:
    ✓ Select correct Config class
    ✓ Validate config immediately
    """

    config: dict[str, Any]

    @model_validator(mode="after")
    def validate_config(self):
        t = self.connector_type

        if t not in CONNECTOR_CONFIG_MAP:
            raise ValueError(f"Unsupported connector type: {t}")

        source_cls, dest_cls = CONNECTOR_CONFIG_MAP[t]

        if self.is_source:
            cfg_cls = source_cls
        else:
            cfg_cls = dest_cls or source_cls   # some connectors share config

        if cfg_cls is None:
            raise ValueError(f"Connector '{t}' does not support destination mode")

        # Validate using correct Pydantic model
        try:
            cfg_cls(**self.config)
        except Exception as e:
            raise ValueError(f"Invalid config for {t}: {str(e)}") from e

        return self


# ================================================================
# UPDATE (partial)
# ================================================================
class ConnectionUpdate(BaseModel):
    """
    Partial update. We cannot fully validate config here, because
    connector_type may not be provided and must come from DB.
    """
    name: str | None = None
    description: str | None = None
    config: dict[str, Any] | None = None


# ================================================================
# DB FACING
# ================================================================
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


# ================================================================
# RESPONSE MODELS
# ================================================================
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
