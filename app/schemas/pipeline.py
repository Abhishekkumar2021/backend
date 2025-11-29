from typing import Optional, Dict, Any, List
from datetime import datetime
from pydantic import BaseModel, Field
from app.models.db_models import PipelineStatus

class PipelineBase(BaseModel):
    name: str
    description: Optional[str] = None
    source_connection_id: int
    destination_connection_id: int
    source_config: Dict[str, Any]
    destination_config: Dict[str, Any]
    transform_config: Optional[Dict[str, Any]] = None
    schedule_cron: Optional[str] = None
    schedule_enabled: bool = False
    sync_mode: str = "full_refresh"

class PipelineCreate(PipelineBase):
    pass

class PipelineUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    source_config: Optional[Dict[str, Any]] = None
    destination_config: Optional[Dict[str, Any]] = None
    schedule_cron: Optional[str] = None
    schedule_enabled: Optional[bool] = None
    status: Optional[PipelineStatus] = None

class PipelineInDBBase(PipelineBase):
    id: int
    status: PipelineStatus
    created_at: datetime
    updated_at: datetime
    last_run_at: Optional[datetime] = None
    next_run_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class Pipeline(PipelineInDBBase):
    pass
