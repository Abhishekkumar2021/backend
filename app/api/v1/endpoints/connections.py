"""Connection Management API Endpoints.

Provides CRUD operations for data connections with:
- Encryption/decryption of credentials
- Connection testing with caching
- Schema discovery
- Cache invalidation
"""

from datetime import UTC, datetime
from typing import Dict, Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.connectors.base import ConnectionTestResult, Schema
from app.connectors.factory import ConnectorFactory
from app.core.exceptions import EncryptionError, ConnectorError, ValidationError
from app.core.logging import get_logger
from app.models.database import Connection, MetadataCache, Pipeline
from app.models.enums import PipelineStatus
from app.schemas.connection import (
    Connection as ConnectionSchema,
    ConnectionCreate,
    ConnectionUpdate,
    ConnectionConfigResponse,
    ConnectionTestResponse,
    CacheInvalidationResponse,
)
from app.services.cache import get_cache
from app.services.encryption import get_encryption_service

logger = get_logger(__name__)
router = APIRouter()


# ===================================================================
# Helper Functions
# ===================================================================

def get_decrypted_config(connection: Connection) -> Dict[str, Any]:
    """Get decrypted connection configuration with caching.
    
    Args:
        connection: Connection database object
        
    Returns:
        Decrypted configuration dictionary
        
    Raises:
        HTTPException: If decryption fails or system is locked
    """
    cache = get_cache()

    # Try cache first
    cached_config = cache.get_config(connection.id)
    if cached_config:
        logger.debug(
            "config_cache_hit",
            connection_id=connection.id,
        )
        return cached_config

    # Cache miss - decrypt and cache
    try:
        encryption_service = get_encryption_service()
        config = encryption_service.decrypt_config(connection.config_encrypted)

        # Cache for 30 minutes
        cache.set_config(connection.id, config)
        
        logger.debug(
            "config_decrypted_and_cached",
            connection_id=connection.id,
        )

        return config
        
    except RuntimeError as e:
        logger.error(
            "encryption_service_not_unlocked",
            connection_id=connection.id,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="System is locked. Please unlock with master password first.",
        )
    except Exception as e:
        logger.error(
            "config_decryption_failed",
            connection_id=connection.id,
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to decrypt config: {str(e)}",
        )


def validate_connection_not_in_use(
    db: Session,
    connection_id: int,
) -> None:
    """Validate that connection is not used in active pipelines.
    
    Args:
        db: Database session
        connection_id: Connection ID to check
        
    Raises:
        HTTPException: If connection is in use
    """
    active_pipelines_count = (
        db.query(Pipeline)
        .filter(
            (Pipeline.source_connection_id == connection_id)
            | (Pipeline.destination_connection_id == connection_id),
            Pipeline.status == PipelineStatus.ACTIVE,
        )
        .count()
    )

    if active_pipelines_count > 0:
        logger.warning(
            "connection_deletion_blocked",
            connection_id=connection_id,
            active_pipelines=active_pipelines_count,
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot delete connection used in {active_pipelines_count} active pipeline(s)",
        )


def mask_sensitive_fields(config: Dict[str, Any]) -> Dict[str, Any]:
    """Mask sensitive fields in configuration.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Configuration with masked sensitive fields
    """
    masked_config = config.copy()
    
    sensitive_fields = ["password", "secret", "token", "key", "credential"]
    
    for field in sensitive_fields:
        if field in masked_config:
            masked_config[field] = "********"
    
    return masked_config


# ===================================================================
# CRUD Endpoints
# ===================================================================

@router.get(
    "/",
    response_model=List[ConnectionSchema],
    summary="List all connections",
)
def list_connections(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records to return"),
    is_source: Optional[bool] = Query(None, description="Filter by source/destination"),
    db: Session = Depends(get_db),
) -> List[Connection]:
    """List all connections with pagination and filtering.
    
    Args:
        skip: Pagination offset
        limit: Maximum records to return
        is_source: Filter by source (True) or destination (False)
        db: Database session
        
    Returns:
        List of connections
    """
    logger.info(
        "list_connections_requested",
        skip=skip,
        limit=limit,
        is_source=is_source,
    )
    
    query = db.query(Connection)

    if is_source is not None:
        query = query.filter(Connection.is_source == is_source)

    connections = query.offset(skip).limit(limit).all()
    
    logger.info(
        "connections_listed",
        count=len(connections),
    )
    
    return connections


@router.post(
    "/",
    response_model=ConnectionSchema,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new connection",
)
def create_connection(
    connection_in: ConnectionCreate,
    db: Session = Depends(get_db),
) -> Connection:
    """Create a new data connection.
    
    Configuration is encrypted before storage using the master password.
    
    Args:
        connection_in: Connection creation data
        db: Database session
        
    Returns:
        Created connection
        
    Raises:
        HTTPException: If connection name exists or encryption fails
    """
    logger.info(
        "connection_creation_requested",
        name=connection_in.name,
        connector_type=connection_in.connector_type,
        is_source=connection_in.is_source,
    )
    
    # Check for duplicate name
    existing = db.query(Connection).filter(
        Connection.name == connection_in.name
    ).first()

    if existing:
        logger.warning(
            "connection_name_conflict",
            name=connection_in.name,
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Connection with name '{connection_in.name}' already exists",
        )

    # Encrypt configuration
    try:
        encryption_service = get_encryption_service()
        config_encrypted = encryption_service.encrypt_config(connection_in.config)
        
    except RuntimeError:
        logger.error("encryption_service_not_unlocked")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="System is locked. Please unlock with master password first.",
        )
    except Exception as e:
        logger.error(
            "encryption_failed",
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Encryption failed: {str(e)}",
        )

    # Create database object
    db_obj = Connection(
        name=connection_in.name,
        connector_type=connection_in.connector_type,
        description=connection_in.description,
        is_source=connection_in.is_source,
        config_encrypted=config_encrypted,
    )

    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)
    
    logger.info(
        "connection_created",
        connection_id=db_obj.id,
        name=db_obj.name,
    )

    return db_obj


@router.get(
    "/{connection_id}",
    response_model=ConnectionSchema,
    summary="Get connection by ID",
)
def get_connection(
    connection_id: int,
    db: Session = Depends(get_db),
) -> Connection:
    """Retrieve a specific connection by ID.
    
    Args:
        connection_id: Connection ID
        db: Database session
        
    Returns:
        Connection object
        
    Raises:
        HTTPException: If connection not found
    """
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
    
    logger.debug(
        "connection_retrieved",
        connection_id=connection_id,
    )

    return connection


@router.patch(
    "/{connection_id}",
    response_model=ConnectionSchema,
    summary="Update connection",
)
def update_connection(
    connection_id: int,
    connection_update: ConnectionUpdate,
    db: Session = Depends(get_db),
) -> Connection:
    """Update an existing connection.
    
    Configuration will be re-encrypted if provided.
    Cache will be invalidated.
    
    Args:
        connection_id: Connection ID
        connection_update: Update data
        db: Database session
        
    Returns:
        Updated connection
        
    Raises:
        HTTPException: If connection not found or name conflict
    """
    logger.info(
        "connection_update_requested",
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

    # Update name if provided
    if connection_update.name is not None:
        # Check for name conflicts
        existing = (
            db.query(Connection)
            .filter(
                Connection.name == connection_update.name,
                Connection.id != connection_id,
            )
            .first()
        )

        if existing:
            logger.warning(
                "connection_name_conflict",
                name=connection_update.name,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Connection with name '{connection_update.name}' already exists",
            )

        connection.name = connection_update.name

    # Update description if provided
    if connection_update.description is not None:
        connection.description = connection_update.description

    # Update and re-encrypt config if provided
    if connection_update.config is not None:
        try:
            encryption_service = get_encryption_service()
            connection.config_encrypted = encryption_service.encrypt_config(
                connection_update.config
            )
        except RuntimeError:
            logger.error("encryption_service_not_unlocked")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="System is locked. Please unlock with master password first.",
            )

    db.commit()
    db.refresh(connection)

    # Invalidate cache
    cache = get_cache()
    cache.invalidate_connection(connection_id)
    
    logger.info(
        "connection_updated",
        connection_id=connection_id,
    )

    return connection


@router.delete(
    "/{connection_id}",
    response_model=ConnectionSchema,
    summary="Delete connection",
)
def delete_connection(
    connection_id: int,
    db: Session = Depends(get_db),
) -> Connection:
    """Delete a connection.
    
    Will fail if connection is used in active pipelines.
    Cache will be invalidated.
    
    Args:
        connection_id: Connection ID
        db: Database session
        
    Returns:
        Deleted connection
        
    Raises:
        HTTPException: If connection not found or in use
    """
    logger.info(
        "connection_deletion_requested",
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

    # Validate not in use
    validate_connection_not_in_use(db, connection_id)

    # Invalidate cache
    cache = get_cache()
    cache.invalidate_connection(connection_id)

    db.delete(connection)
    db.commit()
    
    logger.info(
        "connection_deleted",
        connection_id=connection_id,
    )

    return connection


# ===================================================================
# Connection Testing
# ===================================================================

@router.post(
    "/{connection_id}/test",
    response_model=ConnectionTestResponse,
    summary="Test connection",
)
def test_connection(
    connection_id: int,
    force: bool = Query(False, description="Force fresh test, bypass cache"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Test connection health.
    
    Attempts to connect and validates credentials.
    Results are cached for 5 minutes unless force=true.
    
    Args:
        connection_id: Connection ID
        force: Force fresh test, bypass cache
        db: Database session
        
    Returns:
        Test result with success status and metadata
        
    Raises:
        HTTPException: If connection not found or unsupported type
    """
    logger.info(
        "connection_test_requested",
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

    cache = get_cache()

    # Check cache unless force refresh
    if not force:
        cached_result = cache.get_test_result(connection_id)
        if cached_result:
            logger.debug(
                "connection_test_cache_hit",
                connection_id=connection_id,
            )
            return {**cached_result, "cached": True}

    # Get decrypted config
    config = get_decrypted_config(connection)

    # Test connection
    try:
        if connection.is_source:
            connector = ConnectorFactory.create_source(
                connection.connector_type.value,
                config,
            )
        else:
            connector = ConnectorFactory.create_destination(
                connection.connector_type.value,
                config,
            )
            
        test_result = connector.test_connection()
        
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
    except Exception as e:
        logger.error(
            "connection_test_failed",
            connection_id=connection_id,
            error=str(e),
            exc_info=True,
        )
        test_result = ConnectionTestResult(
            success=False,
            message=f"Unexpected error: {str(e)}",
        )

    # Update connection record
    connection.last_test_at = datetime.now(UTC)
    connection.last_test_success = test_result.success
    connection.last_test_error = None if test_result.success else test_result.message

    db.commit()
    db.refresh(connection)

    # Build response
    result = {
        "connection_id": connection_id,
        "success": test_result.success,
        "message": test_result.message,
        "metadata": test_result.metadata,
        "tested_at": test_result.tested_at,
        "cached": False,
    }

    # Cache result for 5 minutes
    # Note: We cache the dictionary representation, including ISO format date for JSON serialization in cache
    cache_data = result.copy()
    cache_data["tested_at"] = result["tested_at"].isoformat()
    cache.set_test_result(connection_id, cache_data)
    
    logger.info(
        "connection_test_completed",
        connection_id=connection_id,
        success=test_result.success,
    )

    return result


# ===================================================================
# Schema Discovery
# ===================================================================

@router.post(
    "/{connection_id}/discover_schema",
    response_model=Schema,
    summary="Discover connection schema",
)
def discover_connection_schema(
    connection_id: int,
    force: bool = Query(False, description="Force fresh discovery, bypass cache"),
    db: Session = Depends(get_db),
) -> Schema:
    """Discover schema for a source connection.
    
    Retrieves tables, columns, and relationships from the data source.
    Results are cached for 1 hour.
    
    Args:
        connection_id: Connection ID
        force: Force fresh discovery, bypass cache
        db: Database session
        
    Returns:
        Schema object with tables and columns
        
    Raises:
        HTTPException: If connection not found or not a source
    """
    logger.info(
        "schema_discovery_requested",
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

    if not connection.is_source:
        logger.warning(
            "schema_discovery_on_destination",
            connection_id=connection_id,
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Schema discovery is only supported for source connections",
        )

    cache = get_cache()

    # Check cache unless force refresh
    if not force:
        cached_schema_data = cache.get_schema(connection_id)
        if cached_schema_data:
            logger.debug(
                "schema_cache_hit",
                connection_id=connection_id,
            )
            return Schema(**cached_schema_data)

    # Get decrypted config
    config = get_decrypted_config(connection)

    try:
        source_connector = ConnectorFactory.create_source(
            connection.connector_type.value,
            config,
        )
        discovered_schema = source_connector.discover_schema()

        # Update or create MetadataCache entry
        metadata_cache_entry = db.query(MetadataCache).filter(
            MetadataCache.connection_id == connection_id
        ).first()
        
        if metadata_cache_entry:
            metadata_cache_entry.schema_data = discovered_schema.model_dump()
            metadata_cache_entry.table_count = len(discovered_schema.tables)
            metadata_cache_entry.column_count = sum(
                len(t.columns) for t in discovered_schema.tables
            )
            metadata_cache_entry.last_scanned_at = datetime.now(UTC)
        else:
            metadata_cache_entry = MetadataCache(
                connection_id=connection_id,
                schema_data=discovered_schema.model_dump(),
                table_count=len(discovered_schema.tables),
                column_count=sum(len(t.columns) for t in discovered_schema.tables),
                last_scanned_at=datetime.now(UTC),
            )
            db.add(metadata_cache_entry)

        db.commit()
        db.refresh(metadata_cache_entry)

        # Cache schema for 1 hour
        cache.set_schema(connection_id, discovered_schema.model_dump())
        
        logger.info(
            "schema_discovered",
            connection_id=connection_id,
            table_count=len(discovered_schema.tables),
            column_count=sum(len(t.columns) for t in discovered_schema.tables),
        )

        return discovered_schema

    except ValueError as e:
        logger.error(
            "unsupported_connector_for_schema_discovery",
            connection_id=connection_id,
            error=str(e),
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported connector type: {str(e)}",
        )
    except Exception as e:
        logger.error(
            "schema_discovery_failed",
            connection_id=connection_id,
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to discover schema: {str(e)}",
        )


# ===================================================================
# Utility Endpoints
# ===================================================================

@router.get(
    "/{connection_id}/config",
    response_model=ConnectionConfigResponse,
    summary="Get connection config (masked)",
)
def get_connection_config(
    connection_id: int,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Get connection configuration with sensitive fields masked.
    
    ⚠️  WARNING: Use only for debugging/admin purposes.
    Sensitive fields are masked for security.
    
    Args:
        connection_id: Connection ID
        db: Database session
        
    Returns:
        Configuration with masked sensitive fields
        
    Raises:
        HTTPException: If connection not found
    """
    logger.info(
        "config_retrieval_requested",
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

    # Get decrypted config
    config = get_decrypted_config(connection)

    # Mask sensitive fields
    config_masked = mask_sensitive_fields(config)

    return {
        "connection_id": connection_id,
        "connector_type": connection.connector_type.value,
        "config": config_masked,
    }


@router.post(
    "/{connection_id}/cache/invalidate",
    response_model=CacheInvalidationResponse,
    summary="Invalidate connection cache",
)
def invalidate_connection_cache(
    connection_id: int,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Invalidate all cached data for a connection.
    
    Useful after external changes (password rotation, schema changes, etc.).
    
    Args:
        connection_id: Connection ID
        db: Database session
        
    Returns:
        Invalidation result
        
    Raises:
        HTTPException: If connection not found
    """
    logger.info(
        "cache_invalidation_requested",
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
    success = cache.invalidate_connection(connection_id)
    
    logger.info(
        "cache_invalidated",
        connection_id=connection_id,
        success=success,
    )

    return {
        "connection_id": connection_id,
        "cache_invalidated": success,
        "message": "Cache cleared successfully" if success else "Cache not available",
    }