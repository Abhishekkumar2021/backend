from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, SecretStr


class BaseConnectorConfig(BaseModel):
    """Base configuration for all connectors"""
    batch_size: int = Field(default=1000, description="Number of records to process per batch")


class PostgresConfig(BaseConnectorConfig):
    """Configuration for PostgreSQL connector"""
    host: str = Field(..., description="Database host address")
    port: int = Field(default=5432, description="Database port")
    user: str = Field(..., description="Database username")
    password: SecretStr = Field(..., description="Database password")
    database: str = Field(..., description="Database name")
    schema_: str = Field(default="public", alias="schema", description="Database schema")

    class Config:
        populate_by_name = True


class MySQLConfig(BaseConnectorConfig):
    """Configuration for MySQL connector"""
    host: str = Field(..., description="Database host address")
    port: int = Field(default=3306, description="Database port")
    user: str = Field(..., description="Database username")
    password: SecretStr = Field(..., description="Database password")
    database: str = Field(..., description="Database name")


class MSSQLConfig(BaseConnectorConfig):
    """Configuration for MSSQL connector"""
    host: str = Field(..., description="Database host address")
    port: int = Field(default=1433, description="Database port")
    user: str = Field(..., description="Database username")
    password: SecretStr = Field(..., description="Database password")
    database: str = Field(..., description="Database name")


class OracleConfig(BaseConnectorConfig):
    """Configuration for Oracle connector"""
    user: str = Field(..., description="Database username")
    password: SecretStr = Field(..., description="Database password")
    dsn: str = Field(..., description="Oracle Data Source Name (DSN)")


class SQLiteConfig(BaseConnectorConfig):
    """Configuration for SQLite connector"""
    database_path: str = Field(..., description="Path to SQLite database file")


class MongoDBConfig(BaseConnectorConfig):
    """Configuration for MongoDB connector"""
    host: str = Field(..., description="MongoDB host address")
    port: int = Field(default=27017, description="MongoDB port")
    username: Optional[str] = Field(None, description="MongoDB username")
    password: Optional[SecretStr] = Field(None, description="MongoDB password")
    database: str = Field(..., description="Database name")
    auth_source: Optional[str] = Field(None, description="Authentication database")


class SnowflakeConfig(BaseConnectorConfig):
    """Configuration for Snowflake connector"""
    account: str = Field(..., description="Snowflake account identifier")
    username: str = Field(..., description="Snowflake username")
    password: SecretStr = Field(..., description="Snowflake password")
    role: Optional[str] = Field(None, description="User role")
    warehouse: Optional[str] = Field(None, description="Warehouse to use")
    database: str = Field(..., description="Database name")
    schema_: str = Field(..., alias="schema", description="Database schema")

    class Config:
        populate_by_name = True


class BigQueryConfig(BaseConnectorConfig):
    """Configuration for Google BigQuery connector"""
    project_id: str = Field(..., description="GCP Project ID")
    dataset_id: str = Field(..., description="BigQuery Dataset ID")
    # Note: table_id is typically handled at the pipeline level, but kept flexible if needed.


class S3Config(BaseConnectorConfig):
    """Configuration for AWS S3 connector"""
    bucket: str = Field(..., description="S3 bucket name")
    prefix: Optional[str] = Field(None, description="File path prefix")
    format: Literal["parquet", "json", "csv", "jsonl"] = Field(default="parquet", description="File format")
    aws_access_key_id: Optional[str] = Field(None, description="AWS Access Key ID")
    aws_secret_access_key: Optional[SecretStr] = Field(None, description="AWS Secret Access Key")
    region: str = Field(default="us-east-1", description="AWS Region")


class FileSystemConfig(BaseConnectorConfig):
    """Configuration for Local File System connector"""
    file_path: str = Field(..., description="Base path (directory or file)")
    format: Literal["parquet", "json", "csv", "jsonl"] = Field(default="parquet", description="File format")
    glob_pattern: str = Field(default="*", description="Glob pattern for file matching")


class SingerSourceConfig(BaseConnectorConfig):
    """Configuration for Singer Source (Tap)"""
    tap_executable: str = Field(..., description="Path to tap executable (e.g. 'tap-github')")
    tap_config: Dict[str, Any] = Field(..., description="Singer tap configuration (JSON)")
    tap_catalog: Optional[Dict[str, Any]] = Field(None, description="Singer catalog (JSON)")
    select_streams: Optional[List[str]] = Field(None, description="List of streams to select")


class SingerDestinationConfig(BaseConnectorConfig):
    """Configuration for Singer Destination (Target)"""
    target_executable: str = Field(..., description="Path to target executable (e.g. 'target-jsonl')")
    target_config: Dict[str, Any] = Field(..., description="Singer target configuration (JSON)")
    target_catalog: Optional[Dict[str, Any]] = Field(None, description="Singer catalog (JSON)")
