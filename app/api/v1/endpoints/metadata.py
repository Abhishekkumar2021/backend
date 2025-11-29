"""
Metadata Scanning API Endpoints
Schema discovery and caching
"""
import time
from typing import Dict, Any
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from sqlalchemy.orm import Session

from app.api import deps
from app.models import db_models
from app.services.cache import get_cache
from app.connectors.factory import ConnectorFactory
from app.api.v1.endpoints.connections import get_decrypted_config

router = APIRouter()


def serialize_schema(schema) -> Dict[str, Any]:
    """
    Convert Schema object to JSON-serializable dict
    
    Args:
        schema: Schema object from connector
        
    Returns:
        Dictionary representation
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
                        "description": col.description
                    }
                    for col in table.columns
                ]
            }
            for table in schema.tables
        ],
        "version": schema.version,
        "discovered_at": schema.discovered_at.isoformat()
    }


@router.post("/{connection_id}/scan", response_model=dict)
def scan_connection_metadata(
    connection_id: int,
    force: bool = Query(False, description="Force fresh scan, bypass cache"),
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(deps.get_db)
):
    """
    Scan connection and discover schema metadata
    
    Returns tables, columns, data types, primary keys, foreign keys
    Results are cached for 1 hour unless force=true
    
    Query params:
    - force: If true, bypass cache and re-scan
    """
    connection = db.query(db_models.Connection).filter(
        db_models.Connection.id == connection_id
    ).first()
    
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection with ID {connection_id} not found"
        )
    
    if not connection.is_source:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Can only scan source connections. Destinations don't have discoverable schemas."
        )
    
    cache = get_cache()
    
    # Check cache unless force refresh
    if not force:
        cached_schema = cache.get_schema(connection_id)
        if cached_schema:
            return {
                "connection_id": connection_id,
                "cached": True,
                **cached_schema
            }
    
    # Get decrypted config
    config = get_decrypted_config(connection)
    
    # Create connector instance
    try:
        connector = ConnectorFactory.create_source(
            connection.connector_type.value,
            config
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported connector type: {connection.connector_type}"
        )
    
    # Discover schema (this can be slow)
    start_time = time.time()
    
    try:
        schema = connector.discover_schema()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Schema discovery failed: {str(e)}"
        )
    
    scan_duration = time.time() - start_time
    
    # Serialize schema
    schema_dict = serialize_schema(schema)
    
    # Calculate stats
    total_tables = len(schema.tables)
    total_columns = sum(len(table.columns) for table in schema.tables)
    
    # Cache schema for 1 hour
    cache.set_schema(connection_id, schema_dict)
    
    # Update metadata cache in database (async)
    if background_tasks:
        background_tasks.add_task(
            _update_metadata_cache,
            db,
            connection_id,
            schema_dict,
            total_tables,
            total_columns,
            scan_duration
        )
    
    return {
        "connection_id": connection_id,
        "cached": False,
        "scan_duration_seconds": round(scan_duration, 2),
        "total_tables": total_tables,
        "total_columns": total_columns,
        **schema_dict
    }


def _update_metadata_cache(
    db: Session,
    connection_id: int,
    schema_data: Dict[str, Any],
    table_count: int,
    column_count: int,
    duration: float
):
    """
    Background task to update metadata cache in database
    This persists schema data beyond Redis cache
    """
    try:
        # Check if cache entry exists
        cache_entry = db.query(db_models.MetadataCache).filter(
            db_models.MetadataCache.connection_id == connection_id
        ).first()
        
        if cache_entry:
            # Update existing
            cache_entry.schema_data = schema_data
            cache_entry.table_count = table_count
            cache_entry.column_count = column_count
            cache_entry.last_scanned_at = datetime.now(timezone.utc)
            cache_entry.scan_duration_seconds = duration
        else:
            # Create new
            cache_entry = db_models.MetadataCache(
                connection_id=connection_id,
                schema_data=schema_data,
                table_count=table_count,
                column_count=column_count,
                scan_duration_seconds=duration
            )
            db.add(cache_entry)
        
        db.commit()
    except Exception as e:
        db.rollback()
        # Log error but don't fail the request
        import logging
        logging.error(f"Failed to update metadata cache: {str(e)}")


@router.get("/{connection_id}/metadata", response_model=dict)
def get_connection_metadata(
    connection_id: int,
    db: Session = Depends(deps.get_db)
):
    """
    Get cached metadata for a connection
    
    Returns data from Redis cache or database cache
    Does NOT trigger a new scan - use /scan endpoint for that
    """
    connection = db.query(db_models.Connection).filter(
        db_models.Connection.id == connection_id
    ).first()
    
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection with ID {connection_id} not found"
        )
    
    cache = get_cache()
    
    # Try Redis cache first
    cached_schema = cache.get_schema(connection_id)
    if cached_schema:
        return {
            "connection_id": connection_id,
            "source": "redis_cache",
            **cached_schema
        }
    
    # Try database cache
    db_cache = db.query(db_models.MetadataCache).filter(
        db_models.MetadataCache.connection_id == connection_id
    ).first()
    
    if db_cache:
        # Restore to Redis cache
        cache.set_schema(connection_id, db_cache.schema_data)
        
        return {
            "connection_id": connection_id,
            "source": "database_cache",
            "table_count": db_cache.table_count,
            "column_count": db_cache.column_count,
            "last_scanned_at": db_cache.last_scanned_at.isoformat(),
            "scan_duration_seconds": db_cache.scan_duration_seconds,
            **db_cache.schema_data
        }
    
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="No cached metadata found. Use /scan endpoint to discover schema."
    )


@router.get("/{connection_id}/tables", response_model=dict)
def list_tables(
    connection_id: int,
    db: Session = Depends(deps.get_db)
):
    """
    Get list of tables (lightweight, without full column details)
    
    Useful for UI dropdowns and table selection
    """
    # Get cached metadata
    try:
        metadata = get_connection_metadata(connection_id, db)
    except HTTPException as e:
        if e.status_code == 404:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No metadata found. Please scan the connection first."
            )
        raise
    
    # Extract just table names and row counts
    tables = [
        {
            "name": table["name"],
            "schema": table["schema"],
            "row_count": table["row_count"],
            "column_count": len(table["columns"])
        }
        for table in metadata.get("tables", [])
    ]
    
    return {
        "connection_id": connection_id,
        "total_tables": len(tables),
        "tables": tables
    }


@router.get("/{connection_id}/tables/{table_name}", response_model=dict)
def get_table_details(
    connection_id: int,
    table_name: str,
    db: Session = Depends(deps.get_db)
):
    """
    Get detailed information about a specific table
    
    Returns columns, data types, constraints
    """
    # Get cached metadata
    try:
        metadata = get_connection_metadata(connection_id, db)
    except HTTPException as e:
        if e.status_code == 404:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No metadata found. Please scan the connection first."
            )
        raise
    
    # Find table
    table = next(
        (t for t in metadata.get("tables", []) if t["name"] == table_name),
        None
    )
    
    if not table:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{table_name}' not found in connection metadata"
        )
    
    return {
        "connection_id": connection_id,
        **table
    }


@router.get("/{connection_id}/erd", response_model=dict)
def get_erd_data(
    connection_id: int,
    db: Session = Depends(deps.get_db)
):
    """
    Get Entity-Relationship Diagram data
    
    Returns nodes (tables) and edges (foreign key relationships)
    Formatted for React Flow or similar visualization libraries
    """
    # Get cached metadata
    try:
        metadata = get_connection_metadata(connection_id, db)
    except HTTPException as e:
        if e.status_code == 404:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No metadata found. Please scan the connection first."
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
            "primary_keys": [col["name"] for col in table["columns"] if col["primary_key"]]
        })
    
    # Build edges (foreign keys)
    edges = []
    for table in metadata.get("tables", []):
        for col in table["columns"]:
            if col["foreign_key"]:
                # foreign_key format: "table_name.column_name"
                target_table, target_column = col["foreign_key"].split(".")
                edges.append({
                    "id": f"{table['name']}.{col['name']}-{target_table}.{target_column}",
                    "source": table["name"],
                    "target": target_table,
                    "source_column": col["name"],
                    "target_column": target_column
                })
    
    return {
        "connection_id": connection_id,
        "nodes": nodes,
        "edges": edges
    }


@router.delete("/{connection_id}/cache", response_model=dict)
def clear_metadata_cache(
    connection_id: int,
    db: Session = Depends(deps.get_db)
):
    """
    Clear metadata cache (both Redis and database)
    
    Forces next scan to be fresh
    """
    connection = db.query(db_models.Connection).filter(
        db_models.Connection.id == connection_id
    ).first()
    
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection with ID {connection_id} not found"
        )
    
    # Clear Redis cache
    cache = get_cache()
    cache.invalidate_schema(connection_id)
    
    # Clear database cache
    db_cache = db.query(db_models.MetadataCache).filter(
        db_models.MetadataCache.connection_id == connection_id
    ).first()
    
    if db_cache:
        db.delete(db_cache)
        db.commit()
    
    return {
        "connection_id": connection_id,
        "cache_cleared": True,
        "message": "Metadata cache cleared successfully"
    }