"""
Connector configuration schemas (clean, strict, and modern)

These Pydantic models:
✓ Validate all connector configs
✓ Provide default batch_size
✓ Support aliasing (schema → schema_)
✓ Remain extensible for encryption, secrets, validation hooks
✓ Work seamlessly with ConnectorFactory + ConnectionCreate
"""

from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field, SecretStr, model_validator


# ================================================================
# BASE CONFIG
# ================================================================
class BaseConnectorConfig(BaseModel):
    """
    Base class for all connector configs.
    - batch_size is common across ALL connectors.
    - extra fields allowed (pipeline may inject overrides).
    """

    batch_size: int = Field(
        default=1000,
        ge=1,
        description="Number of records to process per batch",
    )

    class Config:
        extra = "allow"
        validate_assignment = True


# ================================================================
# SQL CONNECTORS
# ================================================================
class PostgresConfig(BaseConnectorConfig):
    host: str
    port: int = 5432
    user: str
    password: SecretStr
    database: str
    schema_: str = Field("public", alias="schema")

    class Config:
        populate_by_name = True


class MySQLConfig(BaseConnectorConfig):
    host: str
    port: int = 3306
    user: str
    password: SecretStr
    database: str


class MSSQLConfig(BaseConnectorConfig):
    host: str
    port: int = 1433
    user: str
    password: SecretStr
    database: str


class OracleConfig(BaseConnectorConfig):
    user: str
    password: SecretStr
    dsn: str


class SQLiteConfig(BaseConnectorConfig):
    database_path: str


# ================================================================
# NOSQL / DOCUMENT
# ================================================================
class MongoDBConfig(BaseConnectorConfig):
    host: str
    port: int = 27017
    username: Optional[str] = None
    password: Optional[SecretStr] = None
    database: str
    auth_source: Optional[str] = None

    @model_validator(mode="after")
    def validate_auth(self):
        if (self.username and not self.password) or (self.password and not self.username):
            raise ValueError("Both username and password must be provided for MongoDB authentication")
        return self


# ================================================================
# CLOUD WAREHOUSE
# ================================================================
class SnowflakeConfig(BaseConnectorConfig):
    account: str
    username: str
    password: SecretStr
    role: Optional[str] = None
    warehouse: Optional[str] = None
    database: str
    schema_: str = Field(..., alias="schema")

    class Config:
        populate_by_name = True


class BigQueryConfig(BaseConnectorConfig):
    project_id: str
    dataset_id: str


# ================================================================
# FILE STORAGE CONNECTORS
# ================================================================
class S3Config(BaseConnectorConfig):
    bucket: str
    prefix: Optional[str] = None
    format: Literal["parquet", "json", "csv", "jsonl"] = "parquet"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[SecretStr] = None
    region: str = "us-east-1"

    @model_validator(mode="after")
    def validate_auth(self):
        if self.aws_access_key_id and not self.aws_secret_access_key:
            raise ValueError("aws_secret_access_key is required when aws_access_key_id is provided")
        return self


class FileSystemConfig(BaseConnectorConfig):
    file_path: str
    format: Literal["parquet", "json", "csv", "jsonl"] = "parquet"
    glob_pattern: str = "*"


# ================================================================
# SINGER TAP/TARGET
# ================================================================
class SingerSourceConfig(BaseConnectorConfig):
    tap_executable: str
    tap_config: Dict[str, Any]
    tap_catalog: Optional[Dict[str, Any]] = None
    select_streams: Optional[List[str]] = None

    @model_validator(mode="after")
    def validate_catalog(self):
        if self.tap_catalog is not None and not isinstance(self.tap_catalog, dict):
            raise ValueError("tap_catalog must be a JSON object (dict)")
        return self


class SingerDestinationConfig(BaseConnectorConfig):
    target_executable: str
    target_config: Dict[str, Any]
    target_catalog: Optional[Dict[str, Any]] = None

    @model_validator(mode="after")
    def validate_catalog(self):
        if self.target_catalog is not None and not isinstance(self.target_catalog, dict):
            raise ValueError("target_catalog must be a JSON object (dict)")
        return self
