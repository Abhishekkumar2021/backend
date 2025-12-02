"""Transformer Management API Endpoints."""

from typing import List, Dict, Any, Optional

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from app.core.database import get_db_session
from app.models.database import Transformer
from app.pipeline.processors.registry import list_processor_types
from app.core.logging import get_logger


logger = get_logger(__name__)
router = APIRouter()


# ===================================================================
# Schemas
# ===================================================================

class TransformerCreate(BaseModel):
    name: str = Field(..., min_length=1)
    type: str = Field(..., min_length=1)
    config: Dict[str, Any] = Field(default_factory=dict)


class TransformerUpdate(BaseModel):
    name: Optional[str] = None
    config: Optional[Dict[str, Any]] = None


class TransformerSchema(BaseModel):
    id: int
    name: str
    type: str
    config: Dict[str, Any]
    created_at: Any

    class Config:
        from_attributes = True


# ===================================================================
# Transformer Endpoints
# ===================================================================

@router.get(
    "/types",
    response_model=Dict[str, str],
    summary="List available transformer types",
)
def get_transformer_types():
    """Return all available processors that can be used as transformers."""
    logger.debug("transformer_types_requested")
    return list_processor_types()


@router.post(
    "/",
    response_model=TransformerSchema,
    status_code=status.HTTP_201_CREATED,
    summary="Create transformer configuration",
)
def create_transformer(transformer_in: TransformerCreate):
    """Register a reusable transformer configuration."""
    logger.info(
        "transformer_creation_requested",
        name=transformer_in.name,
        type=transformer_in.type,
    )

    valid_types = list_processor_types().keys()
    if transformer_in.type not in valid_types:
        logger.warning(
            "invalid_transformer_type",
            provided=transformer_in.type,
        )
        raise HTTPException(
            status_code=400,
            detail=f"Invalid transformer type. Must be one of: {', '.join(valid_types)}",
        )

    with get_db_session() as db:
        # Check for name uniqueness
        exists = db.query(Transformer).filter(Transformer.name == transformer_in.name).first()
        if exists:
            logger.warning("transformer_name_conflict", name=transformer_in.name)
            raise HTTPException(
                status_code=400,
                detail=f"Transformer with name '{transformer_in.name}' already exists",
            )

        transformer = Transformer(
            name=transformer_in.name,
            type=transformer_in.type,
            config=transformer_in.config,
        )

        db.add(transformer)
        db.commit()
        db.refresh(transformer)

        logger.info(
            "transformer_created",
            transformer_id=transformer.id,
            name=transformer.name,
        )

        return transformer


@router.get(
    "/",
    response_model=List[TransformerSchema],
    summary="List all transformers",
)
def list_transformers():
    """List all transformer definitions."""
    logger.debug("transformer_list_requested")

    with get_db_session() as db:
        transformers = db.query(Transformer).order_by(Transformer.created_at.desc()).all()

        logger.info(
            "transformer_list_retrieved",
            count=len(transformers),
        )

        return transformers


@router.get(
    "/{transformer_id}",
    response_model=TransformerSchema,
    summary="Get transformer by ID",
)
def get_transformer(transformer_id: int):
    """Retrieve a transformer configuration by ID."""
    logger.debug("transformer_retrieval_requested", transformer_id=transformer_id)

    with get_db_session() as db:
        t = db.query(Transformer).filter(Transformer.id == transformer_id).first()

        if not t:
            logger.warning(
                "transformer_not_found",
                transformer_id=transformer_id,
            )
            raise HTTPException(
                status_code=404,
                detail="Transformer not found",
            )

        logger.debug("transformer_retrieved", transformer_id=transformer_id)
        return t


@router.patch(
    "/{transformer_id}",
    response_model=TransformerSchema,
    summary="Update transformer",
)
def update_transformer(transformer_id: int, update: TransformerUpdate):
    """Update an existing transformer definition."""
    logger.info(
        "transformer_update_requested",
        transformer_id=transformer_id,
    )

    with get_db_session() as db:
        t = db.query(Transformer).filter(Transformer.id == transformer_id).first()

        if not t:
            logger.warning(
                "transformer_not_found",
                transformer_id=transformer_id,
            )
            raise HTTPException(
                status_code=404,
                detail="Transformer not found",
            )

        # Name update
        if update.name:
            exists = (
                db.query(Transformer)
                .filter(Transformer.name == update.name, Transformer.id != transformer_id)
                .first()
            )
            if exists:
                logger.warning(
                    "transformer_name_conflict",
                    name=update.name,
                )
                raise HTTPException(
                    status_code=400,
                    detail=f"Transformer name '{update.name}' already exists",
                )
            t.name = update.name

        # Config update
        if update.config is not None:
            t.config = update.config

        db.commit()
        db.refresh(t)

        logger.info(
            "transformer_updated",
            transformer_id=transformer_id,
        )

        return t


@router.delete(
    "/{transformer_id}",
    status_code=200,
    summary="Delete transformer",
)
def delete_transformer(transformer_id: int):
    """Delete a transformer configuration."""
    logger.info(
        "transformer_delete_requested",
        transformer_id=transformer_id,
    )

    with get_db_session() as db:
        t = db.query(Transformer).filter(Transformer.id == transformer_id).first()

        if not t:
            logger.warning(
                "transformer_not_found",
                transformer_id=transformer_id,
            )
            raise HTTPException(
                status_code=404,
                detail="Transformer not found",
            )

        db.delete(t)
        db.commit()

        logger.info(
            "transformer_deleted",
            transformer_id=transformer_id,
        )

        return {"status": "success", "message": "Transformer deleted"}
