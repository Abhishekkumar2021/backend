"""Connection Management API Endpoints
CRUD operations + connection testing + Redis caching + Schema Discovery
"""

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api import deps
from app.connectors.base import ConnectionTestResult, Schema # Import Schema for response model
from app.connectors.factory import ConnectorFactory
from app.models import database
from app.models.database import MetadataCache # Import MetadataCache for schema caching
from app.schemas import connection as schemas
from app.services.cache import get_cache
from app.services.encryption import get_encryption_service

router = APIRouter()


def get_decrypted_config(connection: database.Connection) -> dict:
    """Helper: Get decrypted config with Redis caching
    Avoids repeated decryption operations
    """
    cache = get_cache()

    # Try cache first
    cached_config = cache.get_config(connection.id)
    if cached_config:
        return cached_config

    # Cache miss - decrypt and cache
    try:
        encryption_service = get_encryption_service()
        config = encryption_service.decrypt_config(connection.config_encrypted)

        # Cache for 30 minutes
        cache.set_config(connection.id, config)

        return config
    except RuntimeError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="System is locked. Please unlock with master password first.",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to decrypt config: {e!s}"
        )


@router.get("/", response_model=list[schemas.Connection])
def list_connections(skip: int = 0, limit: int = 100, is_source: bool = None, db: Session = Depends(deps.get_db)):
    """List all connections with optional filtering

    Query params:
    - skip: Pagination offset
    - limit: Max records to return
    - is_source: Filter by source (True) or destination (False)
    """
    query = db.query(database.Connection)

    if is_source is not None:
        query = query.filter(database.Connection.is_source == is_source)

    connections = query.offset(skip).limit(limit).all()
    return connections


@router.post("/", response_model=schemas.Connection, status_code=status.HTTP_201_CREATED)
def create_connection(connection_in: schemas.ConnectionCreate, db: Session = Depends(deps.get_db)):
    """Create a new connection

    Config will be encrypted before storage
    """
    # Check if connection with same name exists
    existing = db.query(database.Connection).filter(database.Connection.name == connection_in.name).first()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Connection with name '{connection_in.name}' already exists",
        )

    # Encrypt configuration
    try:
        encryption_service = get_encryption_service()
        config_encrypted = encryption_service.encrypt_config(connection_in.config)
    except RuntimeError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="System is locked. Please unlock with master password first.",
        )
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Encryption failed: {e!s}")

    # Create DB object
    db_obj = database.Connection(
        name=connection_in.name,
        connector_type=connection_in.connector_type,
        description=connection_in.description,
        is_source=connection_in.is_source,
        config_encrypted=config_encrypted,
    )

    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)

    return db_obj


@router.get("/{connection_id}", response_model=schemas.Connection)
def get_connection(connection_id: int, db: Session = Depends(deps.get_db)):
    """Get connection by ID"""
    connection = db.query(database.Connection).filter(database.Connection.id == connection_id).first()

    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Connection with ID {connection_id} not found"
        )

    return connection


@router.patch("/{connection_id}", response_model=schemas.Connection)
def update_connection(
    connection_id: int, connection_update: schemas.ConnectionUpdate, db: Session = Depends(deps.get_db)
):
    """Update connection details

    Can update name, description, and config
    Config will be re-encrypted if provided
    Cache will be invalidated
    """
    connection = db.query(database.Connection).filter(database.Connection.id == connection_id).first()

    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Connection with ID {connection_id} not found"
        )

    # Update fields
    if connection_update.name is not None:
        # Check for name conflicts
        existing = (
            db.query(database.Connection)
            .filter(database.Connection.name == connection_update.name, database.Connection.id != connection_id)
            .first()
        )

        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Connection with name '{connection_update.name}' already exists",
            )

        connection.name = connection_update.name

    if connection_update.description is not None:
        connection.description = connection_update.description

    if connection_update.config is not None:
        try:
            encryption_service = get_encryption_service()
            connection.config_encrypted = encryption_service.encrypt_config(connection_update.config)
        except RuntimeError:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="System is locked. Please unlock with master password first.",
            )

    db.commit()
    db.refresh(connection)

    # Invalidate all cache for this connection
    cache = get_cache()
    cache.invalidate_connection(connection_id)

    return connection


@router.delete("/{connection_id}", response_model=schemas.Connection)
def delete_connection(connection_id: int, db: Session = Depends(deps.get_db)):
    """Delete connection

    Will fail if connection is used in active pipelines
    Cache will be invalidated
    """
    connection = db.query(database.Connection).filter(database.Connection.id == connection_id).first()

    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Connection with ID {connection_id} not found"
        )

    # Check if used in pipelines
    active_pipelines = (
        db.query(database.Pipeline)
        .filter(
            (database.Pipeline.source_connection_id == connection_id)
            | (database.Pipeline.destination_connection_id == connection_id),
            database.Pipeline.status == database.PipelineStatus.ACTIVE,
        )
        .count()
    )

    if active_pipelines > 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot delete connection used in {active_pipelines} active pipeline(s)",
        )

    # Invalidate cache
    cache = get_cache()
    cache.invalidate_connection(connection_id)

    db.delete(connection)
    db.commit()

    return connection


@router.post("/{connection_id}/test", response_model=dict)
def test_connection(
    connection_id: int,
    force: bool = Query(False, description="Force fresh test, bypass cache"),
    db: Session = Depends(deps.get_db),
):
    """Test connection health

    Attempts to connect and validates credentials
    Results are cached for 5 minutes unless force=true
    Updates last_test_at, last_test_success, and last_test_error fields
    """
    connection = db.query(database.Connection).filter(database.Connection.id == connection_id).first()

    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Connection with ID {connection_id} not found"
        )

    cache = get_cache()

    # Check cache unless force refresh
    if not force:
        cached_result = cache.get_test_result(connection_id)
        if cached_result:
            return {**cached_result, "cached": True}

    # Get decrypted config (with caching)
    config = get_decrypted_config(connection)

    # Test connection
    try:
        if connection.is_source:
            connector = ConnectorFactory.create_source(connection.connector_type.value, config)
        else:
            connector = ConnectorFactory.create_destination(connection.connector_type.value, config)
        test_result = connector.test_connection()
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported connector type: {connection.connector_type}"
        )
    except Exception as e:
        # Catch any unexpected errors during the test and return a failed result
        from app.connectors.base import ConnectionTestResult
        test_result = ConnectionTestResult(success=False, message=f"Unexpected error during connection test: {e!s}")

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
        "tested_at": test_result.tested_at.isoformat(),
        "cached": False,
    }

    # Cache result for 5 minutes
    cache.set_test_result(connection_id, result)

    return result


@router.get("/{connection_id}/config", response_model=dict)
def get_connection_config(connection_id: int, db: Session = Depends(deps.get_db)):
    """Get decrypted connection configuration

    ⚠️ WARNING: This exposes sensitive credentials
    Use only for debugging/admin purposes
    Config is cached for 30 minutes
    """
    connection = db.query(database.Connection).filter(database.Connection.id == connection_id).first()

    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Connection with ID {connection_id} not found"
        )

    # Get decrypted config (with caching)
    config = get_decrypted_config(connection)

    # Mask password in response
    config_masked = config.copy()
    if "password" in config_masked:
        config_masked["password"] = "********"

    return {"connection_id": connection_id, "connector_type": connection.connector_type.value, "config": config_masked}


@router.post("/{connection_id}/cache/invalidate", response_model=dict)
def invalidate_connection_cache(connection_id: int, db: Session = Depends(deps.get_db)):
    """Manually invalidate all cached data for a connection

    Useful after external changes (password rotation, schema changes, etc.)
    """
    connection = db.query(database.Connection).filter(database.Connection.id == connection_id).first()

    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Connection with ID {connection_id} not found"
        )

    cache = get_cache()
    success = cache.invalidate_connection(connection_id)

    return {
        "connection_id": connection_id,
        "cache_invalidated": success,
        "message": "Cache cleared successfully" if success else "Cache not available",
    }


@router.post("/{connection_id}/discover_schema", response_model=Schema)
def discover_connection_schema(
    connection_id: int,
    force: bool = Query(False, description="Force fresh schema discovery, bypass cache"),
    db: Session = Depends(deps.get_db),
) -> Schema:
    """Discover the schema for a given source connection.

    Attempts to connect to the source and retrieve its schema (tables, columns).
    Results are cached to improve performance.
    """
    connection = db.query(database.Connection).filter(database.Connection.id == connection_id).first()

    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Connection with ID {connection_id} not found"
        )

    if not connection.is_source:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Schema discovery is only supported for source connections."
        )

    cache = get_cache()

    # Check cache unless force refresh
    if not force:
        cached_schema_data = cache.get_schema(connection_id)
        if cached_schema_data:
            # Reconstruct Schema object from cached data
            return Schema(**cached_schema_data)

    # Get decrypted config (with caching)
    config = get_decrypted_config(connection)

    try:
        source_connector = ConnectorFactory.create_source(connection.connector_type.value, config)
        discovered_schema = source_connector.discover_schema()

        # Update or create MetadataCache entry
        metadata_cache_entry = db.query(MetadataCache).filter(MetadataCache.connection_id == connection_id).first()
        if metadata_cache_entry:
            metadata_cache_entry.schema_data = discovered_schema.model_dump() # Use model_dump for Pydantic v2
            metadata_cache_entry.table_count = len(discovered_schema.tables)
            metadata_cache_entry.column_count = sum(len(t.columns) for t in discovered_schema.tables)
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

        return discovered_schema

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported connector type for schema discovery: {e!s}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to discover schema: {e!s}"
        )
