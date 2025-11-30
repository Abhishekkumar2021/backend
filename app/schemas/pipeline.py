from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel

from app.models.db_models import PipelineStatus


class PipelineBase(BaseModel):
    name: str
    description: str | None = None
    source_connection_id: int
    destination_connection_id: int
    source_config: dict[str, Any]
    destination_config: dict[str, Any]
    transform_config: dict[str, Any] | None = None
    schedule_cron: str | None = None
    schedule_enabled: bool = False
    sync_mode: str = "full_refresh"


class PipelineCreate(PipelineBase):
    pass


class PipelineUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    source_config: dict[str, Any] | None = None
    destination_config: dict[str, Any] | None = None
    schedule_cron: str | None = None
    schedule_enabled: bool | None = None
    status: PipelineStatus | None = None


class PipelineInDBBase(PipelineBase):
    id: int
    status: PipelineStatus
    created_at: datetime
    updated_at: datetime
    last_run_at: datetime | None = None
    next_run_at: datetime | None = None

    class Config:
        from_attributes = True


class Pipeline(PipelineInDBBase):
    pass


# Schemas for PipelineVersion
class PipelineVersionBase(BaseModel):
    pipeline_id: int
    version_number: int
    created_by: Optional[str] = None
    description: Optional[str] = None
    config_snapshot: dict[str, Any]


class PipelineVersionCreate(BaseModel):
    description: Optional[str] = None


class PipelineVersionInDBBase(PipelineVersionBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True


class PipelineVersion(PipelineVersionInDBBase):
    pass
