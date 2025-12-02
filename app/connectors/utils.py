"""
Data Type Mapping Utilities
---------------------------

Centralized type-conversion utilities used by:
✓ Source connectors (schema discovery)
✓ Destination connectors (CREATE TABLE)
✓ Processors
✓ Pipeline engine

Goals:
✓ Clean, predictable mappings
✓ Cross-database compatibility
✓ Minimize duplicate logic
"""

from datetime import datetime
from typing import Any, Type

from app.connectors.base import DataType


# =============================================================================
# MONGO TYPE MAPPER
# =============================================================================
def map_mongo_type_to_data_type(py_type: Type[Any]) -> DataType:
    """
    Maps Python types returned by MongoDB to internal DataType enum.
    """
    if py_type is str:
        return DataType.STRING
    if py_type is int:
        return DataType.INTEGER
    if py_type is float:
        return DataType.FLOAT
    if py_type is bool:
        return DataType.BOOLEAN
    if py_type is datetime:
        return DataType.DATETIME
    if py_type in (dict, list):
        return DataType.JSON
    if py_type is bytes:
        return DataType.BINARY

    return DataType.STRING  # Fallback


# =============================================================================
# GENERIC SQL TYPE MAPPER
# =============================================================================
def map_sql_type_to_data_type(sql_type: str) -> DataType:
    """
    Maps SQL column types (MySQL, PostgreSQL, MSSQL, Oracle, SQLite) to DataType.

    Input can be:
        - "VARCHAR"
        - "varchar(255)"
        - "NUMBER(10,2)"
        - "INT"
        - "TIMESTAMP WITH TIME ZONE"
    """
    t = sql_type.lower().strip()

    # Remove length and precision eg: varchar(255) → varchar
    base = t.split("(")[0].strip()

    # --- STRING types ---
    if any(s in base for s in ["char", "text", "string", "uuid"]):
        return DataType.STRING

    # --- INTEGER types ---
    # Bug fixed: previous logic had "( " not in sql_type; NUMBER(10,2) is NOT integer.
    if base in ["int", "integer", "smallint", "tinyint", "mediumint"] or base.endswith("int"):
        return DataType.INTEGER

    # --- FLOAT/DECIMAL types ---
    if base in ["float", "double", "real", "decimal", "numeric"]:
        return DataType.FLOAT

    # --- BOOLEAN ---
    if base in ["bool", "boolean"]:
        return DataType.BOOLEAN

    # --- DATES ---
    if base == "date":
        return DataType.DATE

    # --- DATETIME/TIMESTAMP ---
    if "timestamp" in base or "datetime" in base:
        return DataType.DATETIME

    # --- JSON ---
    if base == "json":
        return DataType.JSON

    # --- BINARY ---
    if any(x in base for x in ["blob", "binary", "varbinary", "bytea"]):
        return DataType.BINARY

    # Fallback
    return DataType.STRING


# =============================================================================
# MAP DataType → Generic SQL Type (used by Snowflake, BigQuery, etc.)
# =============================================================================
def map_data_type_to_sql_type(data_type: DataType) -> str:
    """
    Maps internal DataType to a generic SQL type.
    Snowflake: TIMESTAMP_NTZ, VARIANT
    """
    return {
        DataType.STRING: "VARCHAR",
        DataType.INTEGER: "INT",
        DataType.FLOAT: "FLOAT",
        DataType.BOOLEAN: "BOOLEAN",
        DataType.DATE: "DATE",
        DataType.DATETIME: "TIMESTAMP_NTZ",
        DataType.JSON: "VARIANT",
        DataType.BINARY: "BINARY",
    }.get(data_type, "VARCHAR")


# =============================================================================
# BIGQUERY TYPE MAPPER
# =============================================================================
def map_bigquery_type_to_data_type(bq_type: str) -> DataType:
    """
    Maps BigQuery type names to internal DataType.
    """
    t = bq_type.upper()

    if t in ["STRING"]:
        return DataType.STRING
    if t in ["INT64", "INTEGER"]:
        return DataType.INTEGER
    if t in ["FLOAT64", "FLOAT"]:
        return DataType.FLOAT
    if t == "BOOL":
        return DataType.BOOLEAN
    if t == "DATE":
        return DataType.DATE
    if t in ["DATETIME", "TIMESTAMP"]:
        return DataType.DATETIME
    if t == "JSON":
        return DataType.JSON
    if t == "BYTES":
        return DataType.BINARY

    return DataType.STRING


# =============================================================================
# SINGER TYPE MAPPER
# =============================================================================
def map_singer_type_to_data_type(singer_type: str) -> DataType:
    """
    Maps Singer spec types to internal DataType enum.
    """
    t = singer_type.lower()

    if t == "string":
        return DataType.STRING
    if t == "integer":
        return DataType.INTEGER
    if t == "number":
        return DataType.FLOAT
    if t == "boolean":
        return DataType.BOOLEAN
    if t == "date":
        return DataType.DATE
    if t == "datetime":
        return DataType.DATETIME
    if t in ["object", "array"]:
        return DataType.JSON

    return DataType.STRING
