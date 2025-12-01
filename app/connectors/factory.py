"""Connector Factory with Auto-Registration
Import this module to register all available connectors
"""

from app.connectors.base import ConnectorFactory
from app.connectors.destinations.file.filesystem import FileSystemDestination
from app.connectors.destinations.sql.mssql import MSSQLDestination
from app.connectors.destinations.sql.mysql import MySQLDestination
from app.connectors.destinations.sql.oracle import OracleDestination
from app.connectors.destinations.sql.postgresql import PostgreSQLDestination
from app.connectors.destinations.cloud.s3 import S3Destination
from app.connectors.destinations.sql.sqlite import SQLiteDestination
from app.connectors.destinations.nosql.mongodb import MongoDBDestination
from app.connectors.destinations.cloud.snowflake import SnowflakeDestination
from app.connectors.destinations.cloud.bigquery import BigQueryDestination
from app.connectors.destinations.integration.singer import SingerDestination

from app.connectors.sources.file.filesystem import FileSystemSource
from app.connectors.sources.sql.mssql import MSSQLSource
from app.connectors.sources.sql.mysql import MySQLSource
from app.connectors.sources.sql.oracle import OracleSource
from app.connectors.sources.sql.postgresql import PostgreSQLSource
from app.connectors.sources.cloud.s3 import S3Source
from app.connectors.sources.sql.sqlite import SQLiteSource
from app.connectors.sources.nosql.mongodb import MongoDBSource
from app.connectors.sources.cloud.snowflake import SnowflakeSource
from app.connectors.sources.cloud.bigquery import BigQuerySource
from app.connectors.sources.integration.singer import SingerSource

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


def register_all_connectors():
    """Register all available connectors
    Called on app startup
    """
    # PostgreSQL
    ConnectorFactory.register_source("postgresql", PostgreSQLSource, PostgresConfig)
    ConnectorFactory.register_destination("postgresql", PostgreSQLDestination, PostgresConfig)

    # SQLite
    ConnectorFactory.register_source("sqlite", SQLiteSource, SQLiteConfig)
    ConnectorFactory.register_destination("sqlite", SQLiteDestination, SQLiteConfig)

    # File System
    ConnectorFactory.register_source("local_file", FileSystemSource, FileSystemConfig)
    ConnectorFactory.register_destination("local_file", FileSystemDestination, FileSystemConfig)

    # AWS S3
    ConnectorFactory.register_source("s3", S3Source, S3Config)
    ConnectorFactory.register_destination("s3", S3Destination, S3Config)

    # Oracle
    ConnectorFactory.register_source("oracle", OracleSource, OracleConfig)
    ConnectorFactory.register_destination("oracle", OracleDestination, OracleConfig)

    # MySQL
    ConnectorFactory.register_source("mysql", MySQLSource, MySQLConfig)
    ConnectorFactory.register_destination("mysql", MySQLDestination, MySQLConfig)

    # MSSQL
    ConnectorFactory.register_source("mssql", MSSQLSource, MSSQLConfig)
    ConnectorFactory.register_destination("mssql", MSSQLDestination, MSSQLConfig)

    # MongoDB
    ConnectorFactory.register_source("mongodb", MongoDBSource, MongoDBConfig)
    ConnectorFactory.register_destination("mongodb", MongoDBDestination, MongoDBConfig)

    # Snowflake
    ConnectorFactory.register_source("snowflake", SnowflakeSource, SnowflakeConfig)
    ConnectorFactory.register_destination("snowflake", SnowflakeDestination, SnowflakeConfig)

    # BigQuery
    ConnectorFactory.register_source("bigquery", BigQuerySource, BigQueryConfig)
    ConnectorFactory.register_destination("bigquery", BigQueryDestination, BigQueryConfig)

    # Singer.io
    ConnectorFactory.register_source("singer", SingerSource, SingerSourceConfig)
    ConnectorFactory.register_destination("singer", SingerDestination, SingerDestinationConfig)


# Auto-register on import
register_all_connectors()
