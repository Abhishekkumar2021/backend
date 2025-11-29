"""
Connector Factory with Auto-Registration
Import this module to register all available connectors
"""
from app.connectors.base import ConnectorFactory
from app.connectors.postgresql import PostgreSQLSource, PostgreSQLDestination
from app.connectors.sqlite import SQLiteSource
from app.connectors.filesystem import FileSystemDestination, FileSystemSource
from app.connectors.s3 import S3Source, S3Destination


def register_all_connectors():
    """
    Register all available connectors
    Called on app startup
    """
    # PostgreSQL
    ConnectorFactory.register_source("postgresql", PostgreSQLSource)
    ConnectorFactory.register_destination("postgresql", PostgreSQLDestination)
    
    # SQLite (source only)
    ConnectorFactory.register_source("sqlite", SQLiteSource)
    
    # File System
    ConnectorFactory.register_source("local_file", FileSystemSource)
    ConnectorFactory.register_destination("local_file", FileSystemDestination)
    
    # AWS S3
    ConnectorFactory.register_source("s3", S3Source)
    ConnectorFactory.register_destination("s3", S3Destination)
    
    # TODO: Add more connectors here as they're built
    # ConnectorFactory.register_source("mysql", MySQLSource)
    # ConnectorFactory.register_destination("mysql", MySQLDestination)
    # etc.


# Auto-register on import
register_all_connectors()