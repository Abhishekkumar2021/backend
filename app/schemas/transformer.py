from typing import Dict, Any, Optional
from pydantic import BaseModel, Field

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
