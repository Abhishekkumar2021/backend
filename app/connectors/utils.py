from datetime import datetime
from typing import Any

from app.connectors.base import DataType


def map_mongo_type_to_data_type(mongo_type: type) -> DataType:
    """
    Maps a MongoDB (Python) type to a standardized DataType enum.
    """
    if mongo_type is str:
        return DataType.STRING
    elif mongo_type is int:
        return DataType.INTEGER
    elif mongo_type is float:
        return DataType.FLOAT
    elif mongo_type is bool:
        return DataType.BOOLEAN
    elif mongo_type is datetime:
        return DataType.DATETIME
    elif mongo_type is dict or mongo_type is list:
        return DataType.JSON
    elif mongo_type is bytes:
        return DataType.BINARY
    else:
        return DataType.STRING # Default to string for unknown types or complex objects


def map_sql_type_to_data_type(sql_type: str) -> DataType:
    """
    Maps a SQL type string to a standardized DataType enum.
    Covers common types from various SQL databases.
    """
    sql_type = sql_type.lower()
    if "char" in sql_type or "text" in sql_type or "string" in sql_type:
        return DataType.STRING
    elif "int" in sql_type or "number" in sql_type and "(" not in sql_type: # Exclude NUMBER(p,s) which can be float
        return DataType.INTEGER
    elif "float" in sql_type or "double" in sql_type or "decimal" in sql_type or "numeric" in sql_type:
        return DataType.FLOAT
    elif "boolean" in sql_type or "bool" in sql_type:
        return DataType.BOOLEAN
    elif "date" == sql_type: # Exact match for date to avoid datetime
        return DataType.DATE
    elif "timestamp" in sql_type or "datetime" in sql_type:
        return DataType.DATETIME
    elif "json" in sql_type:
        return DataType.JSON
    elif "binary" in sql_type or "blob" in sql_type:
        return DataType.BINARY
    else:
        return DataType.STRING # Default for unhandled or complex types

def map_data_type_to_sql_type(data_type: DataType) -> str:
    """
    Maps a standardized DataType enum to a generic SQL type string.
    This is used for CREATE TABLE statements.
    """
    if data_type == DataType.STRING:
        return "VARCHAR"
    elif data_type == DataType.INTEGER:
        return "INT"
    elif data_type == DataType.FLOAT:
        return "FLOAT"
    elif data_type == DataType.BOOLEAN:
        return "BOOLEAN"
    elif data_type == DataType.DATE:
        return "DATE"
    elif data_type == DataType.DATETIME:
        return "TIMESTAMP_NTZ" # Snowflake specific for datetime without timezone
    elif data_type == DataType.JSON:
        return "VARIANT" # Snowflake specific for JSON
    elif data_type == DataType.BINARY:
        return "BINARY"
    else:
        return "VARCHAR" # Default to VARCHAR for unhandled types

def map_bigquery_type_to_data_type(bq_type: str) -> DataType:
    """
    Maps a BigQuery type string to a standardized DataType enum.
    """
    bq_type = bq_type.upper()
    if bq_type in ("STRING", "BIGNUMERIC", "NUMERIC"): # BIGNUMERIC/NUMERIC can contain decimal points which can be represented as STRING
        return DataType.STRING
    elif bq_type in ("INT64", "INTEGER"):
        return DataType.INTEGER
    elif bq_type in ("FLOAT64", "FLOAT"):
        return DataType.FLOAT
    elif bq_type == "BOOL":
        return DataType.BOOLEAN
    elif bq_type == "DATE":
        return DataType.DATE
    elif bq_type in ("DATETIME", "TIMESTAMP"):
        return DataType.DATETIME
    elif bq_type == "JSON":
        return DataType.JSON
    elif bq_type == "BYTES":
        return DataType.BINARY
    else:
        return DataType.STRING # Default for unhandled or complex types

def map_singer_type_to_data_type(singer_type: str) -> DataType:
    """
    Maps a Singer.io type string (from schema) to a standardized DataType enum.
    """
    if singer_type == "string":
        return DataType.STRING
    elif singer_type == "integer":
        return DataType.INTEGER
    elif singer_type == "number": # Float or Decimal
        return DataType.FLOAT
    elif singer_type == "boolean":
        return DataType.BOOLEAN
    elif singer_type == "date":
        return DataType.DATE
    elif singer_type == "datetime":
        return DataType.DATETIME
    elif singer_type == "object" or singer_type == "array":
        return DataType.JSON
    # Singer doesn't have a direct 'binary' type, map to string
    else:
        return DataType.STRING
