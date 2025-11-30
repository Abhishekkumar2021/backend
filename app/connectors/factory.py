"""Connector Factory with Auto-Registration
Import this module to register all available connectors
"""

from app.connectors.base import ConnectorFactory
from app.connectors.destinations.filesystem import FileSystemDestination
from app.connectors.destinations.mssql import MSSQLDestination
from app.connectors.destinations.mysql import MySQLDestination
from app.connectors.destinations.oracle import OracleDestination
from app.connectors.destinations.postgresql import PostgreSQLDestination
from app.connectors.destinations.s3 import S3Destination
from app.connectors.destinations.sqlite import SQLiteDestination
from app.connectors.destinations.mongodb import MongoDBDestination
from app.connectors.destinations.snowflake import SnowflakeDestination
from app.connectors.destinations.bigquery import BigQueryDestination
from app.connectors.destinations.singer import SingerDestination # Import Singer Destination

from app.connectors.sources.filesystem import FileSystemSource
from app.connectors.sources.mssql import MSSQLSource
from app.connectors.sources.mysql import MySQLSource
from app.connectors.sources.oracle import OracleSource
from app.connectors.sources.postgresql import PostgreSQLSource
from app.connectors.sources.s3 import S3Source
from app.connectors.sources.sqlite import SQLiteSource
from app.connectors.sources.mongodb import MongoDBSource
from app.connectors.sources.snowflake import SnowflakeSource
from app.connectors.sources.bigquery import BigQuerySource
from app.connectors.sources.singer import SingerSource # Import Singer Source


def register_all_connectors():
    """Register all available connectors
    Called on app startup
    """
    # PostgreSQL
    ConnectorFactory.register_source("postgresql", PostgreSQLSource)
    ConnectorFactory.register_destination("postgresql", PostgreSQLDestination)

    # SQLite
    ConnectorFactory.register_source("sqlite", SQLiteSource)
    ConnectorFactory.register_destination("sqlite", SQLiteDestination)

    # File System
    ConnectorFactory.register_source("local_file", FileSystemSource)
    ConnectorFactory.register_destination("local_file", FileSystemDestination)

    # AWS S3
    ConnectorFactory.register_source("s3", S3Source)
    ConnectorFactory.register_destination("s3", S3Destination)

    # Oracle
    ConnectorFactory.register_source("oracle", OracleSource)
    ConnectorFactory.register_destination("oracle", OracleDestination)

    # MySQL
    ConnectorFactory.register_source("mysql", MySQLSource)
    ConnectorFactory.register_destination("mysql", MySQLDestination)

    # MSSQL
    ConnectorFactory.register_source("mssql", MSSQLSource)
    ConnectorFactory.register_destination("mssql", MSSQLDestination)

    # MongoDB
    ConnectorFactory.register_source("mongodb", MongoDBSource)
    ConnectorFactory.register_destination("mongodb", MongoDBDestination)

    # Snowflake
    ConnectorFactory.register_source("snowflake", SnowflakeSource)
    ConnectorFactory.register_destination("snowflake", SnowflakeDestination)

    # BigQuery
    ConnectorFactory.register_source("bigquery", BigQuerySource)
    ConnectorFactory.register_destination("bigquery", BigQueryDestination)

    # Singer.io
    ConnectorFactory.register_source("singer", SingerSource)
    ConnectorFactory.register_destination("singer", SingerDestination)


# Auto-register on import
register_all_connectors()
