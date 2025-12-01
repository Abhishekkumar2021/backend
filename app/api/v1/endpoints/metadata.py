"""Metadata Scanning API Endpoints.

Provides schema discovery, caching, and ERD data for connections.
"""

import time
from datetime import UTC, datetime
from typing import Any, Dict, List

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api.deps import get_db
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
        "version": schema.version,
        "discovered_at": schema.discovered_at.isoformat(),
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
    db: Session,
    connection_id: int,
    schema_data: Dict[str, Any],
    table_count: int,
    column_count: int,
    duration: float,
) -> None:
    """Update metadata cache in database (background task).
    
    This persists schema data beyond Redis cache.
    
    Args:
        db: Database session
        connection_id: Connection ID
        schema_data: Serialized schema data
        table_count: Number of tables
        column_count: Total number of columns
        duration: Scan duration in seconds
    """
    try:
        cache_entry = db.query(MetadataCache).filter(
            MetadataCache.connection_id == connection_id
        ).first()

        if cache_entry:
            # Update existing entry
            cache_entry.schema_data = schema_data
            cache_entry.table_count = table_count
            cache_entry.column_count = column_count
            cache_entry.last_scanned_at = datetime.now(UTC)
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
                scan_duration_seconds=duration,
            )
            db.add(cache_entry)
            
            logger.debug(
                "metadata_cache_created",
                connection_id=connection_id,
            )

        db.commit()
        
        logger.info(
            "metadata_cache_persisted",
            connection_id=connection_id,
            table_count=table_count,
            column_count=column_count,
        )
        
    except Exception as e:
        db.rollback()
        logger.error(
            "metadata_cache_update_failed",
            connection_id=connection_id,
            error=str(e),
            exc_info=True,
        )


# ===================================================================
# Schema Scanning Endpoints
# ===================================================================

@router.post(
    "/{connection_id}/scan",
    response_model=Dict[str, Any],
    summary="Scan connection schema",
)
def scan_connection_metadata(
    connection_id: int,
    force: bool = Query(False, description="Force fresh scan, bypass cache"),
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Scan connection and discover schema metadata.
    
    Discovers tables, columns, data types, primary keys, and foreign keys.
    Results are cached for 1 hour unless force=true.
    
    Args:
        connection_id: Connection ID to scan
        force: If true, bypass cache and re-scan
        background_tasks: FastAPI background tasks
        db: Database session
        
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
    
    connection = db.query(Connection).filter(
        Connection.id == connection_id
    ).first()

    if not connection:
        logger.warning(
            "connection_not_found",
            connection_id=connection_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection with ID {connection_id} not found",
        )

    # Validate is source connection
    validate_source_connection(connection)

    cache = get_cache()

    # Check cache unless force refresh
    if not force:
        cached_schema = cache.get_schema(connection_id)
        if cached_schema:
            logger.debug(
                "metadata_cache_hit",
                connection_id=connection_id,
            )
            return {
                "connection_id": connection_id,
                "cached": True,
                **cached_schema,
            }

    # Get decrypted config
    config = get_decrypted_config(connection)

    # Create connector instance
    try:
        connector = ConnectorFactory.create_source(
            connection.connector_type.value,
            config,
        )
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
    
    logger.info(
        "schema_discovery_started",
        connection_id=connection_id,
    )

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

    # Cache schema for 1 hour
    cache.set_schema(connection_id, schema_dict)
    
    logger.info(
        "schema_discovered",
        connection_id=connection_id,
        table_count=total_tables,
        column_count=total_columns,
        scan_duration_seconds=round(scan_duration, 2),
    )

    # Update metadata cache in database (async)
    if background_tasks:
        background_tasks.add_task(
            update_metadata_cache,
            db,
            connection_id,
            schema_dict,
            total_tables,
            total_columns,
            scan_duration,
        )

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
    response_model=Dict[str, Any],
    summary="Get cached metadata",
)
def get_connection_metadata(
    connection_id: int,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Get cached metadata for a connection.
    
    Returns data from Redis cache or database cache.
    Does NOT trigger a new scan - use /scan endpoint for that.
    
    Args:
        connection_id: Connection ID
        db: Database session
        
    Returns:
        Cached metadata
        
    Raises:
        HTTPException: If connection not found or no cache exists
    """
    logger.debug(
        "metadata_retrieval_requested",
        connection_id=connection_id,
    )
    
    connection = db.query(Connection).filter(
        Connection.id == connection_id
    ).first()

    if not connection:
        logger.warning(
            "connection_not_found",
            connection_id=connection_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection with ID {connection_id} not found",
        )

    cache = get_cache()

    # Try Redis cache first
    cached_schema = cache.get_schema(connection_id)
    if cached_schema:
        logger.debug(
            "metadata_from_redis_cache",
            connection_id=connection_id,
        )
        return {
            "connection_id": connection_id,
            "source": "redis_cache",
            **cached_schema,
        }

    # Try database cache
    db_cache = db.query(MetadataCache).filter(
        MetadataCache.connection_id == connection_id
    ).first()

    if db_cache:
        # Restore to Redis cache
        cache.set_schema(connection_id, db_cache.schema_data)
        
        logger.debug(
            "metadata_from_database_cache",
            connection_id=connection_id,
        )

        return {
            "connection_id": connection_id,
            "source": "database_cache",
            "table_count": db_cache.table_count,
            "column_count": db_cache.column_count,
            "last_scanned_at": db_cache.last_scanned_at.isoformat(),
            "scan_duration_seconds": db_cache.scan_duration_seconds,
            **db_cache.schema_data,
        }

    logger.warning(
        "no_metadata_cache_found",
        connection_id=connection_id,
    )
    
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="No cached metadata found. Use /scan endpoint to discover schema.",
    )


# ===================================================================
# Table Information Endpoints
# ===================================================================

@router.get(
    "/{connection_id}/tables",
    response_model=Dict[str, Any],
    summary="List tables",
)
def list_tables(
    connection_id: int,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Get list of tables without full column details.
    
    Lightweight endpoint useful for UI dropdowns and table selection.
    
    Args:
        connection_id: Connection ID
        db: Database session
        
    Returns:
        List of tables with basic information
        
    Raises:
        HTTPException: If no metadata found
    """
    logger.debug(
        "table_list_requested",
        connection_id=connection_id,
    )
    
    # Get cached metadata
    try:
        metadata = get_connection_metadata(connection_id, db)
    except HTTPException as e:
        if e.status_code == status.HTTP_404_NOT_FOUND:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No metadata found. Please scan the connection first.",
            )
        raise

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
    response_model=Dict[str, Any],
    summary="Get table details",
)
def get_table_details(
    connection_id: int,
    table_name: str,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Get detailed information about a specific table.
    
    Returns columns, data types, and constraints.
    
    Args:
        connection_id: Connection ID
        table_name: Table name to retrieve
        db: Database session
        
    Returns:
        Table details with all columns
        
    Raises:
        HTTPException: If table not found
    """
    logger.debug(
        "table_details_requested",
        connection_id=connection_id,
        table_name=table_name,
    )
    
    # Get cached metadata
    try:
        metadata = get_connection_metadata(connection_id, db)
    except HTTPException as e:
        if e.status_code == status.HTTP_404_NOT_FOUND:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No metadata found. Please scan the connection first.",
            )
        raise

    # Find table
    table = next(
        (t for t in metadata.get("tables", []) if t["name"] == table_name),
        None,
    )

    if not table:
        logger.warning(
            "table_not_found",
            connection_id=connection_id,
            table_name=table_name,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{table_name}' not found in connection metadata",
        )
    
    logger.debug(
        "table_details_retrieved",
        connection_id=connection_id,
        table_name=table_name,
    )

    return {
        "connection_id": connection_id,
        **table,
    }


# ===================================================================
# ERD (Entity-Relationship Diagram) Endpoint
# ===================================================================

@router.get(
    "/{connection_id}/erd",
    response_model=Dict[str, Any],
    summary="Get ERD data",
)
def get_erd_data(
    connection_id: int,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Get Entity-Relationship Diagram data.
    
    Returns nodes (tables) and edges (foreign key relationships).
    Formatted for React Flow or similar visualization libraries.
    
    Args:
        connection_id: Connection ID
        db: Database session
        
    Returns:
        ERD data with nodes and edges
        
    Raises:
        HTTPException: If no metadata found
    """
    logger.debug(
        "erd_data_requested",
        connection_id=connection_id,
    )
    
    # Get cached metadata
    try:
        metadata = get_connection_metadata(connection_id, db)
    except HTTPException as e:
        if e.status_code == status.HTTP_404_NOT_FOUND:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No metadata found. Please scan the connection first.",
            )
        raise

    # Build nodes (tables)
    nodes = []
    for table in metadata.get("tables", []):
        nodes.append({
            "id": table["name"],
            "label": table["name"],
            "row_count": table["row_count"],
            "columns": len(table["columns"]),
            "primary_keys": [
                col["name"] for col in table["columns"] if col["primary_key"]
            ],
        })

    # Build edges (foreign key relationships)
    edges = []
    for table in metadata.get("tables", []):
        for col in table["columns"]:
            if col["foreign_key"]:
                try:
                    # foreign_key format: "table_name.column_name"
                    target_table, target_column = col["foreign_key"].split(".", 1)
                    edges.append({
                        "id": f"{table['name']}.{col['name']}-{target_table}.{target_column}",
                        "source": table["name"],
                        "target": target_table,
                        "source_column": col["name"],
                        "target_column": target_column,
                    })
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

    return {
        "connection_id": connection_id,
        "nodes": nodes,
        "edges": edges,
    }


# ===================================================================
# Cache Management Endpoints
# ===================================================================

@router.delete(
    "/{connection_id}/cache",
    response_model=Dict[str, Any],
    summary="Clear metadata cache",
)
def clear_metadata_cache(
    connection_id: int,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Clear metadata cache (both Redis and database).
    
    Forces next scan to be fresh.
    
    Args:
        connection_id: Connection ID
        db: Database session
        
    Returns:
        Clear result
        
    Raises:
        HTTPException: If connection not found
    """
    logger.info(
        "metadata_cache_clear_requested",
        connection_id=connection_id,
    )
    
    connection = db.query(Connection).filter(
        Connection.id == connection_id
    ).first()

    if not connection:
        logger.warning(
            "connection_not_found",
            connection_id=connection_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection with ID {connection_id} not found",
        )

    # Clear Redis cache
    cache = get_cache()
    cache.invalidate_schema(connection_id)

    # Clear database cache
    db_cache = db.query(MetadataCache).filter(
        MetadataCache.connection_id == connection_id
    ).first()

    if db_cache:
        db.delete(db_cache)
        db.commit()
        
        logger.info(
            "metadata_cache_cleared",
            connection_id=connection_id,
            cleared_from_database=True,
        )
    else:
        logger.info(
            "metadata_cache_cleared",
            connection_id=connection_id,
            cleared_from_database=False,
        )

    return {
        "connection_id": connection_id,
        "cache_cleared": True,
        "message": "Metadata cache cleared successfully",
    }