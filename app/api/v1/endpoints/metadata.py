"""Metadata Scanning API Endpoints.

Provides schema discovery, caching, and ERD data for connections.
"""

import time
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.core.database import get_db_session
from app.api.v1.endpoints.connections import get_decrypted_config
from app.connectors.factory import ConnectorFactory
from app.core.logging import get_logger
from app.models.database import Connection, MetadataCache
from app.services.cache import get_cache

logger = get_logger(__name__)
router = APIRouter()


# ===================================================================
# Helper Functions
# ===================================================================

def serialize_schema(schema) -> Dict[str, Any]:
    """Convert Schema object to JSON-serializable dictionary.

    Args:
        schema: Schema object from connector

    Returns:
        Dictionary representation of schema
    """
    return {
        "tables": [
            {
                "name": table.name,
                "schema": table.schema,
                "row_count": table.row_count,
                "description": table.description,
                "columns": [
                    {
                        "name": col.name,
                        "data_type": col.data_type.value,
                        "nullable": col.nullable,
                        "primary_key": col.primary_key,
                        "foreign_key": col.foreign_key,
                        "default_value": str(col.default_value) if col.default_value else None,
                        "description": col.description,
                    }
                    for col in table.columns
                ],
            }
            for table in schema.tables
        ],
        "version": getattr(schema, "version", None),
        "discovered_at": getattr(schema, "discovered_at", datetime.now(UTC)).isoformat(),
    }


def validate_source_connection(connection: Connection) -> None:
    """Validate that connection is a source.

    Args:
        connection: Connection object

    Raises:
        HTTPException: If connection is not a source
    """
    if not connection.is_source:
        logger.warning(
            "metadata_scan_on_destination",
            connection_id=connection.id,
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Can only scan source connections. Destinations don't have discoverable schemas.",
        )


def update_metadata_cache(
    connection_id: int,
    schema_data: Dict[str, Any],
    table_count: int,
    column_count: int,
    duration: float,
) -> None:
    """Update metadata cache in database (background task).

    This persists schema data beyond Redis cache. This function creates
    and manages its own DB session so it's safe for FastAPI background tasks.

    Args:
        connection_id: Connection ID
        schema_data: Serialized schema data
        table_count: Number of tables
        column_count: Total number of columns
        duration: Scan duration in seconds
    """
    logger.debug("background_metadata_cache_update_task_started", connection_id=connection_id)
    try:
        with get_db_session() as db:  # context-managed DB session
            cache_entry = db.query(MetadataCache).filter(
                MetadataCache.connection_id == connection_id
            ).first()

            now = datetime.now(UTC)

            if cache_entry:
                # Update existing entry
                cache_entry.schema_data = schema_data
                cache_entry.table_count = table_count
                cache_entry.column_count = column_count
                cache_entry.last_scanned_at = now
                cache_entry.scan_duration_seconds = duration

                logger.debug(
                    "metadata_cache_updated",
                    connection_id=connection_id,
                )
            else:
                # Create new entry
                cache_entry = MetadataCache(
                    connection_id=connection_id,
                    schema_data=schema_data,
                    table_count=table_count,
                    column_count=column_count,
                    last_scanned_at=now,
                    scan_duration_seconds=duration,
                )
                db.add(cache_entry)

                logger.debug(
                    "metadata_cache_created",
                    connection_id=connection_id,
                )

            # commit happens in contextmanager
            logger.info(
                "metadata_cache_persisted",
                connection_id=connection_id,
                table_count=table_count,
                column_count=column_count,
            )

    except Exception as e:
        logger.error(
            "background_metadata_cache_update_failed",
            connection_id=connection_id,
            error=str(e),
            exc_info=True,
        )


# ===================================================================
# Lower-level helper used by endpoints (creates its own DB session)
# ===================================================================
def _fetch_connection(connection_id: int) -> Connection:
    with get_db_session() as db:
        conn = db.query(Connection).filter(Connection.id == connection_id).first()
        if not conn:
            logger.warning("connection_not_found", connection_id=connection_id)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Connection with ID {connection_id} not found",
            )
        # detach from session - return ORM object which can be read safely
        return conn


def get_connection_metadata_cached_or_db(connection_id: int) -> Optional[Dict[str, Any]]:
    """Attempt to get metadata from Redis; if not present, return DB cached value (raw dict)."""
    cache = get_cache()

    # Try Redis cache
    cached_schema = cache.get_schema(connection_id)
    if cached_schema:
        logger.debug("metadata_from_redis_cache", connection_id=connection_id)
        return {"source": "redis_cache", **cached_schema}

    # Try DB cache
    with get_db_session() as db:
        db_cache = db.query(MetadataCache).filter(MetadataCache.connection_id == connection_id).first()
        if db_cache:
            # Restore to Redis for faster future access
            try:
                cache.set_schema(connection_id, db_cache.schema_data)
            except Exception:
                logger.debug("failed_to_restore_to_redis", connection_id=connection_id)

            logger.debug("metadata_from_database_cache", connection_id=connection_id)
            return {
                "source": "database_cache",
                **db_cache.schema_data,
                "table_count": db_cache.table_count,
                "column_count": db_cache.column_count,
                "last_scanned_at": db_cache.last_scanned_at.isoformat() if db_cache.last_scanned_at else None,
                "scan_duration_seconds": db_cache.scan_duration_seconds,
            }

    return None


# ===================================================================
# Schema Scanning Endpoints
# ===================================================================

@router.post(
    "/{connection_id}/scan",
    response_model=dict,
    summary="Scan connection schema",
)
def scan_connection_metadata(
    connection_id: int,
    background_tasks: BackgroundTasks,
    force: bool = Query(False, description="Force fresh scan, bypass cache"),
) -> Dict[str, Any]:
    """Scan connection and discover schema metadata.

    Discovers tables, columns, data types, primary keys, and foreign keys.
    Results are cached for 1 hour unless force=true.

    Args:
        connection_id: Connection ID to scan
        force: If true, bypass cache and re-scan
        background_tasks: FastAPI BackgroundTasks (optional)

    Returns:
        Schema metadata with scan statistics

    Raises:
        HTTPException: If connection not found or scan fails
    """
    logger.info(
        "metadata_scan_requested",
        connection_id=connection_id,
        force=force,
    )

    # Fetch connection (raises 404 if not found)
    connection = _fetch_connection(connection_id)

    # Validate is source connection
    validate_source_connection(connection)

    cache = get_cache()

    # Check cache unless force refresh
    if not force:
        cached_schema = cache.get_schema(connection_id)
        if cached_schema:
            logger.debug("metadata_cache_hit", connection_id=connection_id)
            return {
                "connection_id": connection_id,
                "cached": True,
                **cached_schema,
            }

    # Get decrypted config (this function handles caching/decryption)
    config = get_decrypted_config(connection)

    # Create connector instance
    try:
        connector = ConnectorFactory.create_source(connection.connector_type.value, config)
    except ValueError as e:
        logger.error(
            "unsupported_connector_type",
            connection_id=connection_id,
            connector_type=connection.connector_type,
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported connector type: {connection.connector_type}",
        )

    # Discover schema (this can be slow)
    start_time = time.time()

    logger.info("schema_discovery_started", connection_id=connection_id)
    try:
        schema = connector.discover_schema()
    except Exception as e:
        logger.error(
            "schema_discovery_failed",
            connection_id=connection_id,
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Schema discovery failed: {str(e)}",
        )

    scan_duration = time.time() - start_time

    # Serialize schema
    schema_dict = serialize_schema(schema)

    # Calculate statistics
    total_tables = len(schema.tables)
    total_columns = sum(len(table.columns) for table in schema.tables)

    # Cache schema for 1 hour in Redis
    try:
        cache.set_schema(connection_id, schema_dict)
    except Exception as e:
        logger.warning("failed_to_cache_schema_in_redis", connection_id=connection_id, error=str(e))

    logger.info(
        "schema_discovered",
        connection_id=connection_id,
        table_count=total_tables,
        column_count=total_columns,
        scan_duration_seconds=round(scan_duration, 2),
    )

    # Update metadata cache in database (async if background tasks provided)
    if background_tasks:
        # Background task will manage its own DB session
        background_tasks.add_task(
            update_metadata_cache,
            connection_id,
            schema_dict,
            total_tables,
            total_columns,
            scan_duration,
        )
    else:
        # Synchronous update
        update_metadata_cache(connection_id, schema_dict, total_tables, total_columns, scan_duration)

    return {
        "connection_id": connection_id,
        "cached": False,
        "scan_duration_seconds": round(scan_duration, 2),
        "total_tables": total_tables,
        "total_columns": total_columns,
        **schema_dict,
    }


@router.get(
    "/{connection_id}/metadata",
    response_model=dict,
    summary="Get cached metadata",
)
def get_connection_metadata(connection_id: int) -> Dict[str, Any]:
    """Get cached metadata for a connection.

    Returns data from Redis cache or database cache.
    Does NOT trigger a new scan - use /scan endpoint for that.

    Args:
        connection_id: Connection ID

    Returns:
        Cached metadata

    Raises:
        HTTPException: If connection not found or no cache exists
    """
    logger.debug("metadata_retrieval_requested", connection_id=connection_id)

    # Ensure connection exists
    _ = _fetch_connection(connection_id)

    # Try Redis or DB cache
    cached_or_db = get_connection_metadata_cached_or_db(connection_id)
    if cached_or_db:
        response = {"connection_id": connection_id, **cached_or_db}
        return response

    logger.warning("no_metadata_cache_found", connection_id=connection_id)
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="No cached metadata found. Use /scan endpoint to discover schema.",
    )


# ===================================================================
# Table Information Endpoints
# ===================================================================

@router.get(
    "/{connection_id}/tables",
    response_model=dict,
    summary="List tables",
)
def list_tables(connection_id: int) -> Dict[str, Any]:
    """Get list of tables without full column details.

    Lightweight endpoint useful for UI dropdowns and table selection.

    Args:
        connection_id: Connection ID

    Returns:
        List of tables with basic information

    Raises:
        HTTPException: If no metadata found
    """
    logger.debug("table_list_requested", connection_id=connection_id)

    metadata = get_connection_metadata(connection_id)

    # Extract table summaries
    tables = [
        {
            "name": table["name"],
            "schema": table["schema"],
            "row_count": table["row_count"],
            "column_count": len(table["columns"]),
            "description": table.get("description"),
        }
        for table in metadata.get("tables", [])
    ]

    logger.debug(
        "table_list_retrieved",
        connection_id=connection_id,
        table_count=len(tables),
    )

    return {
        "connection_id": connection_id,
        "total_tables": len(tables),
        "tables": tables,
    }


@router.get(
    "/{connection_id}/tables/{table_name}",
    response_model=dict,
    summary="Get table details",
)
def get_table_details(connection_id: int, table_name: str) -> Dict[str, Any]:
    """Get detailed information about a specific table.

    Returns columns, data types, and constraints.

    Args:
        connection_id: Connection ID
        table_name: Table name to retrieve

    Returns:
        Table details with all columns

    Raises:
        HTTPException: If table not found
    """
    logger.debug("table_details_requested", connection_id=connection_id, table_name=table_name)

    metadata = get_connection_metadata(connection_id)

    # Find table
    table = next((t for t in metadata.get("tables", []) if t["name"] == table_name), None)

    if not table:
        logger.warning("table_not_found", connection_id=connection_id, table_name=table_name)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{table_name}' not found in connection metadata",
        )

    logger.debug("table_details_retrieved", connection_id=connection_id, table_name=table_name)

    return {"connection_id": connection_id, **table}


# ===================================================================
# ERD (Entity-Relationship Diagram) Endpoint
# ===================================================================

@router.get(
    "/{connection_id}/erd",
    response_model=dict,
    summary="Get ERD data",
)
def get_erd_data(connection_id: int) -> Dict[str, Any]:
    """Get Entity-Relationship Diagram data.

    Returns nodes (tables) and edges (foreign key relationships).
    Formatted for React Flow or similar visualization libraries.

    Args:
        connection_id: Connection ID

    Returns:
        ERD data with nodes and edges

    Raises:
        HTTPException: If no metadata found
    """
    logger.debug("erd_data_requested", connection_id=connection_id)

    metadata = get_connection_metadata(connection_id)

    # Build nodes (tables)
    nodes = []
    for table in metadata.get("tables", []):
        nodes.append(
            {
                "id": table["name"],
                "label": table["name"],
                "row_count": table["row_count"],
                "columns": len(table["columns"]),
                "primary_keys": [col["name"] for col in table["columns"] if col["primary_key"]],
            }
        )

    # Build edges (foreign key relationships)
    edges = []
    for table in metadata.get("tables", []):
        for col in table["columns"]:
            if col["foreign_key"]:
                try:
                    target_table, target_column = col["foreign_key"].split(".", 1)
                    edges.append(
                        {
                            "id": f"{table['name']}.{col['name']}-{target_table}.{target_column}",
                            "source": table["name"],
                            "target": target_table,
                            "source_column": col["name"],
                            "target_column": target_column,
                        }
                    )
                except ValueError:
                    logger.warning(
                        "invalid_foreign_key_format",
                        connection_id=connection_id,
                        table=table["name"],
                        column=col["name"],
                        foreign_key=col["foreign_key"],
                    )

    logger.debug(
        "erd_data_retrieved",
        connection_id=connection_id,
        node_count=len(nodes),
        edge_count=len(edges),
    )

    return {"connection_id": connection_id, "nodes": nodes, "edges": edges}


# ===================================================================
# Cache Management Endpoints
# ===================================================================

@router.delete(
    "/{connection_id}/cache",
    response_model=dict,
    summary="Clear metadata cache",
)
def clear_metadata_cache(connection_id: int) -> Dict[str, Any]:
    """Clear metadata cache (both Redis and database).

    Forces next scan to be fresh.

    Args:
        connection_id: Connection ID

    Returns:
        Clear result

    Raises:
        HTTPException: If connection not found
    """
    logger.info("metadata_cache_clear_requested", connection_id=connection_id)

    # Ensure connection exists
    connection = _fetch_connection(connection_id)

    # Clear Redis cache
    cache = get_cache()
    cache.invalidate_schema(connection_id)

    # Clear database cache
    with get_db_session() as db:
        db_cache = db.query(MetadataCache).filter(MetadataCache.connection_id == connection_id).first()
        cleared_from_db = False
        if db_cache:
            db.delete(db_cache)
            cleared_from_db = True

    logger.info(
        "metadata_cache_cleared",
        connection_id=connection_id,
        cleared_from_database=cleared_from_db,
    )

    return {"connection_id": connection_id, "cache_cleared": True, "message": "Metadata cache cleared successfully"}
