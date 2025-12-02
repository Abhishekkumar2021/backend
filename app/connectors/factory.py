"""
Connector Factory (Final Clean Version)
---------------------------------------

Central registry for ALL connectors in the Universal ETL.

Goals:
✓ Cleanest possible structure
✓ DRY registration patterns
✓ Source + Destination symmetry
✓ Proper config model validation
✓ Auto-registration on import
✓ Fully compatible with PipelineEngine + Celery Worker

NOTE:
The actual logic for instantiating connectors lives in:
    app/connectors/base.py (class ConnectorFactory)
This file ONLY registers names → classes → config models.
"""

from __future__ import annotations

from app.core.logging import get_logger
from app.connectors.base import ConnectorFactory

# ---------------------------------------------------------
# IMPORT SOURCE CONNECTOR CLASSES
# ---------------------------------------------------------
from app.connectors.sources.sql.postgresql import PostgreSQLSource
from app.connectors.sources.sql.mysql import MySQLSource
from app.connectors.sources.sql.mssql import MSSQLSource
from app.connectors.sources.sql.oracle import OracleSource
from app.connectors.sources.sql.sqlite import SQLiteSource

from app.connectors.sources.nosql.mongodb import MongoDBSource

from app.connectors.sources.cloud.s3 import S3Source
from app.connectors.sources.cloud.snowflake import SnowflakeSource
from app.connectors.sources.cloud.bigquery import BigQuerySource

from app.connectors.sources.file.filesystem import FileSystemSource

from app.connectors.sources.integration.singer import SingerSource

# ---------------------------------------------------------
# IMPORT DESTINATION CONNECTOR CLASSES
# ---------------------------------------------------------
from app.connectors.destinations.sql.postgresql import PostgreSQLDestination
from app.connectors.destinations.sql.mysql import MySQLDestination
from app.connectors.destinations.sql.mssql import MSSQLDestination
from app.connectors.destinations.sql.oracle import OracleDestination
from app.connectors.destinations.sql.sqlite import SQLiteDestination

from app.connectors.destinations.nosql.mongodb import MongoDBDestination

from app.connectors.destinations.cloud.s3 import S3Destination
from app.connectors.destinations.cloud.snowflake import SnowflakeDestination
from app.connectors.destinations.cloud.bigquery import BigQueryDestination

from app.connectors.destinations.file.filesystem import FileSystemDestination

from app.connectors.destinations.integration.singer import SingerDestination

# ---------------------------------------------------------
# IMPORT CONFIG SCHEMAS
# ---------------------------------------------------------
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

logger = get_logger(__name__)


# ===================================================================
# HELPER — reduces duplicate boilerplate
# ===================================================================
def _register_pair(name: str, source_cls, dest_cls, config_model):
    """
    Register BOTH source & destination with one call.
    Example:
        _register_pair("postgresql", PostgreSQLSource, PostgreSQLDestination, PostgresConfig)
    """
    ConnectorFactory.register_source(name, source_cls, config_model)
    ConnectorFactory.register_destination(name, dest_cls, config_model)


# ===================================================================
# MASTER REGISTRATION FUNCTION
# ===================================================================
def register_all_connectors() -> None:
    """
    Registers all connectors consistently.
    Auto-executed once at module import.
    """
    logger.info("connector_registration_started")

    # Clean & flat list of all connector mappings
    registry: list[tuple] = [
        ("postgresql", PostgreSQLSource, PostgreSQLDestination, PostgresConfig),
        ("mysql", MySQLSource, MySQLDestination, MySQLConfig),
        ("mssql", MSSQLSource, MSSQLDestination, MSSQLConfig),
        ("oracle", OracleSource, OracleDestination, OracleConfig),
        ("sqlite", SQLiteSource, SQLiteDestination, SQLiteConfig),

        ("mongodb", MongoDBSource, MongoDBDestination, MongoDBConfig),

        ("s3", S3Source, S3Destination, S3Config),
        ("snowflake", SnowflakeSource, SnowflakeDestination, SnowflakeConfig),
        ("bigquery", BigQuerySource, BigQueryDestination, BigQueryConfig),

        ("local_file", FileSystemSource, FileSystemDestination, FileSystemConfig),
    ]

    # Register all “normal” connectors
    for name, src_cls, dst_cls, cfg_model in registry:
        _register_pair(name, src_cls, dst_cls, cfg_model)
        logger.debug("connector_registered", name=name)

    # Singer connectors require different config models
    ConnectorFactory.register_source("singer", SingerSource, SingerSourceConfig)
    ConnectorFactory.register_destination("singer", SingerDestination, SingerDestinationConfig)
    logger.debug("connector_registered", name="singer")

    # Summary log
    logger.info(
        "connector_registration_completed",
        sources=len(ConnectorFactory._sources),
        destinations=len(ConnectorFactory._destinations),
    )


# Auto-execute registration on import
register_all_connectors()
