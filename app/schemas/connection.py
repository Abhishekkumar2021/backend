from typing import Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, Json
from app.models.db_models import ConnectorType

class ConnectionBase(BaseModel):
    name: str
    connector_type: ConnectorType
    description: Optional[str] = None
    is_source: bool = True

class ConnectionCreate(ConnectionBase):
    config: Dict[str, Any]  # Raw config from user (host, password, etc.)

class ConnectionUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    config: Optional[Dict[str, Any]] = None

class ConnectionInDBBase(ConnectionBase):
    id: int
    last_test_at: Optional[datetime] = None
    last_test_success: Optional[bool] = None
    last_test_error: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class Connection(ConnectionInDBBase):
    pass
