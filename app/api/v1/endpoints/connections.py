"""
Connection Management API Endpoints (Updated)
Uses new DB session manager from app.core.database.get_db_session
"""

from datetime import datetime, UTC
from typing import Dict, Any, List, Optional

from fastapi import APIRouter, HTTPException, Query, status

from app.core.database import get_db_session
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
    """Fetch decrypted config with caching."""
    cache = get_cache()

    cached = cache.get_config(connection.id)
    if cached:
        return cached

    try:
        enc = get_encryption_service()
        cfg = enc.decrypt_config(connection.config_encrypted)
        cache.set_config(connection.id, cfg)
        return cfg

    except RuntimeError:
        raise HTTPException(
            status_code=503,
            detail="System is locked. Unlock using master password.",
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to decrypt config: {e}",
        )


def validate_connection_not_in_use(db, connection_id: int):
    """Prevent deletion if used by active pipelines."""
    count = (
        db.query(Pipeline)
        .filter(
            (Pipeline.source_connection_id == connection_id)
            | (Pipeline.destination_connection_id == connection_id),
            Pipeline.status == PipelineStatus.ACTIVE,
        )
        .count()
    )
    
    if count > 0:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot delete connection: used in {count} active pipeline(s).",
        )


def mask_sensitive_fields(config: Dict[str, Any]) -> Dict[str, Any]:
    masked = config.copy()
    for key in ["password", "secret", "token", "key", "credential"]:
        if key in masked:
            masked[key] = "********"
    return masked


# ===================================================================
# LIST CONNECTIONS
# ===================================================================

@router.get("/", response_model=List[ConnectionSchema], summary="List all connections")
def list_connections(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    is_source: Optional[bool] = Query(None),
):
    with get_db_session() as db:
        query = db.query(Connection)
        if is_source is not None:
            query = query.filter(Connection.is_source == is_source)
        results = query.offset(skip).limit(limit).all()
        return [ConnectionSchema.model_validate(c) for c in results]


# ===================================================================
# CREATE CONNECTION
# ===================================================================

@router.post("/", response_model=ConnectionSchema, status_code=201)
def create_connection(connection_in: ConnectionCreate):
    with get_db_session() as db:

        exists = db.query(Connection).filter(
            Connection.name == connection_in.name
        ).first()

        if exists:
            raise HTTPException(
                status_code=400,
                detail=f"Connection '{connection_in.name}' already exists.",
            )

        # Encrypt config
        try:
            enc = get_encryption_service()
            enc_cfg = enc.encrypt_config(connection_in.config)
        except RuntimeError:
            raise HTTPException(
                503, "System locked. Unlock with master password."
            )
        except Exception as e:
            raise HTTPException(500, f"Encryption failed: {e}")

        obj = Connection(
            name=connection_in.name,
            connector_type=connection_in.connector_type,
            is_source=connection_in.is_source,
            description=connection_in.description,
            config_encrypted=enc_cfg,
        )

        db.add(obj)
        db.flush()  # ensures obj.id exists
        db.refresh(obj)
        return ConnectionSchema.model_validate(obj)


# ===================================================================
# GET CONNECTION
# ===================================================================

@router.get("/{connection_id}", response_model=ConnectionSchema)
def get_connection_by_id(connection_id: int):
    with get_db_session() as db:
        conn = db.query(Connection).filter(Connection.id == connection_id).first()
        if not conn:
            raise HTTPException(404, "Connection not found")
        return ConnectionSchema.model_validate(conn)


# ===================================================================
# UPDATE CONNECTION
# ===================================================================

@router.patch("/{connection_id}", response_model=ConnectionSchema)
def update_connection(connection_id: int, upd: ConnectionUpdate):
    with get_db_session() as db:
        conn = db.query(Connection).filter(Connection.id == connection_id).first()
        if not conn:
            raise HTTPException(404, "Connection not found")

        # Name update
        if upd.name:
            conflict = (
                db.query(Connection)
                .filter(Connection.name == upd.name, Connection.id != connection_id)
                .first()
            )
            if conflict:
                raise HTTPException(400, "Connection name already exists")
            conn.name = upd.name

        if upd.description is not None:
            conn.description = upd.description

        if upd.config is not None:
            try:
                enc = get_encryption_service()
                conn.config_encrypted = enc.encrypt_config(upd.config)
            except RuntimeError:
                raise HTTPException(503, "System locked")
            except Exception as e:
                raise HTTPException(500, f"Encryption failed: {e}")

        # Cache invalidation
        get_cache().invalidate_connection(connection_id)

        db.flush()
        db.refresh(conn)
        return ConnectionSchema.model_validate(conn)


# ===================================================================
# DELETE CONNECTION
# ===================================================================

@router.delete("/{connection_id}", response_model=ConnectionSchema)
def delete_connection(connection_id: int):
    with get_db_session() as db:
        conn = db.query(Connection).filter(Connection.id == connection_id).first()
        if not conn:
            raise HTTPException(404, "Connection not found")

        validate_connection_not_in_use(db, connection_id)

        get_cache().invalidate_connection(connection_id)
        
        # Convert before deletion to ensure we have the data
        result = ConnectionSchema.model_validate(conn)
        db.delete(conn)

        return result


# ===================================================================
# TEST CONNECTION
# ===================================================================

@router.post("/{connection_id}/test", response_model=ConnectionTestResponse)
def test_connection(connection_id: int, force: bool = False):
    with get_db_session() as db:

        conn = db.query(Connection).filter(Connection.id == connection_id).first()
        if not conn:
            raise HTTPException(404, "Connection not found")

        cache = get_cache()

        if not force:
            cached = cache.get_test_result(connection_id)
            if cached:
                return {**cached, "cached": True}

        config = get_decrypted_config(conn)

        # Perform test
        try:
            if conn.is_source:
                connector = ConnectorFactory.create_source(
                    conn.connector_type.value, config
                )
            else:
                connector = ConnectorFactory.create_destination(
                    conn.connector_type.value, config
                )

            res = connector.test_connection()

        except Exception as e:
            res = ConnectionTestResult(success=False, message=str(e))

        # Update DB fields
        conn.last_test_at = datetime.now(UTC)
        conn.last_test_success = res.success
        conn.last_test_error = None if res.success else res.message

        db.flush()

        response = {
            "connection_id": connection_id,
            "success": res.success,
            "message": res.message,
            "metadata": res.metadata,
            "tested_at": res.tested_at,
            "cached": False,
        }

        # Cache it
        cache_data = response.copy()
        cache_data["tested_at"] = res.tested_at.isoformat()
        cache.set_test_result(connection_id, cache_data)

        return response


# ===================================================================
# SCHEMA DISCOVERY
# ===================================================================

@router.post("/{connection_id}/discover_schema", response_model=Schema)
def discover_schema(connection_id: int, force: bool = False):
    with get_db_session() as db:

        conn = db.query(Connection).filter(Connection.id == connection_id).first()
        if not conn:
            raise HTTPException(404, "Connection not found")

        if not conn.is_source:
            raise HTTPException(400, "Schema discovery only for sources")

        cache = get_cache()

        if not force:
            cached = cache.get_schema(connection_id)
            if cached:
                return Schema(**cached)

        config = get_decrypted_config(conn)

        try:
            src = ConnectorFactory.create_source(conn.connector_type.value, config)
            schema = src.discover_schema()
        except Exception as e:
            raise HTTPException(500, f"Schema discovery failed: {e}")

        # Update database metadata cache
        meta = (
            db.query(MetadataCache)
            .filter(MetadataCache.connection_id == connection_id)
            .first()
        )
        if meta:
            meta.schema_data = schema.model_dump()
            meta.table_count = len(schema.tables)
            meta.column_count = sum(len(t.columns) for t in schema.tables)
            meta.last_scanned_at = datetime.now(UTC)
        else:
            meta = MetadataCache(
                connection_id=connection_id,
                schema_data=schema.model_dump(),
                table_count=len(schema.tables),
                column_count=sum(len(t.columns) for t in schema.tables),
                last_scanned_at=datetime.now(UTC),
            )
            db.add(meta)

        cache.set_schema(connection_id, schema.model_dump())
        return schema


# ===================================================================
# GET MASKED CONFIG
# ===================================================================

@router.get("/{connection_id}/config", response_model=ConnectionConfigResponse)
def get_config_masked(connection_id: int):
    with get_db_session() as db:
        conn = db.query(Connection).filter(Connection.id == connection_id).first()
        if not conn:
            raise HTTPException(404, "Connection not found")

        cfg = get_decrypted_config(conn)
        return {
            "connection_id": connection_id,
            "connector_type": conn.connector_type.value,
            "config": mask_sensitive_fields(cfg),
        }


# ===================================================================
# INVALIDATE CACHE
# ===================================================================

@router.post("/{connection_id}/cache/invalidate", response_model=CacheInvalidationResponse)
def invalidate_cache(connection_id: int):
    conn = None
    with get_db_session() as db:
        conn = db.query(Connection).filter(Connection.id == connection_id).first()
        if not conn:
            raise HTTPException(404, "Connection not found")

    cache = get_cache()
    success = cache.invalidate_connection(connection_id)

    return {
        "connection_id": connection_id,
        "cache_invalidated": success,
        "message": "Cache invalidated" if success else "Cache not available",
    }
