from datetime import datetime
from typing import Any

from pydantic import BaseModel

from app.models.db_models import ConnectorType


class ConnectionBase(BaseModel):
    name: str
    connector_type: ConnectorType
    description: str | None = None
    is_source: bool = True


class ConnectionCreate(ConnectionBase):
    config: dict[str, Any]  # Raw config from user (host, password, etc.)


class ConnectionUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    config: dict[str, Any] | None = None


class ConnectionInDBBase(ConnectionBase):
    id: int
    last_test_at: datetime | None = None
    last_test_success: bool | None = None
    last_test_error: str | None = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class Connection(ConnectionInDBBase):
    pass
