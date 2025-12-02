"""Transformer Management API Endpoints."""

from typing import List, Dict, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.api.deps import get_db
from app.models.database import Transformer
from app.pipeline.processors.registry import list_processor_types
from app.core.logging import get_logger

logger = get_logger(__name__)
router = APIRouter()

# Schemas
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

# Endpoints

@router.get("/types", response_model=Dict[str, str])
def get_transformer_types():
    """List available transformer types (classes)."""
    return list_processor_types()

@router.post("/", response_model=TransformerSchema, status_code=status.HTTP_201_CREATED)
def create_transformer(
    transformer_in: TransformerCreate,
    db: Session = Depends(get_db),
):
    """Register a new transformer configuration."""
    # Check if type exists
    valid_types = list_processor_types().keys()
    if transformer_in.type not in valid_types:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail=f"Invalid transformer type. Must be one of: {', '.join(valid_types)}"
        )

    # Check name uniqueness
    existing = db.query(Transformer).filter(Transformer.name == transformer_in.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Transformer name already exists")

    transformer = Transformer(
        name=transformer_in.name,
        type=transformer_in.type,
        config=transformer_in.config
    )
    db.add(transformer)
    db.commit()
    db.refresh(transformer)
    return transformer

@router.get("/", response_model=List[TransformerSchema])
def list_transformers(db: Session = Depends(get_db)):
    """List all registered transformers."""
    return db.query(Transformer).all()

@router.delete("/{transformer_id}")
def delete_transformer(transformer_id: int, db: Session = Depends(get_db)):
    """Delete a transformer."""
    t = db.query(Transformer).filter(Transformer.id == transformer_id).first()
    if not t:
        raise HTTPException(status_code=404, detail="Transformer not found")
    
    db.delete(t)
    db.commit()
    return {"status": "success"}
