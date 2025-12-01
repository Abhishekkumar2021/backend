from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field

from app.models.database import PipelineStatus


class PipelineSourceConfig(BaseModel):
    """Configuration for the source side of a pipeline."""
    table_name: str = Field(..., description="Name of the table or stream to read from")
    query: Optional[str] = Field(None, description="Custom SQL query to execute")
    
    class Config:
        extra = "allow"  # Allow overriding connector settings like batch_size


class PipelineDestinationConfig(BaseModel):
    """Configuration for the destination side of a pipeline."""
    table_name: Optional[str] = Field(None, description="Target table name (if different)")
    
    class Config:
        extra = "allow"  # Allow overriding connector settings


class PipelineTransformConfig(BaseModel):
    """Configuration for data transformations."""
    processor_type: str = Field("noop", description="Type of processor to use")
    config: dict[str, Any] = Field(default_factory=dict, description="Processor-specific configuration")


class PipelineBase(BaseModel):
    name: str
    description: str | None = None
    source_connection_id: int
    destination_connection_id: int
    source_config: PipelineSourceConfig
    destination_config: PipelineDestinationConfig
    transform_config: PipelineTransformConfig | None = None
    schedule_cron: str | None = None
    schedule_enabled: bool = False
    sync_mode: str = "full_refresh"


class PipelineCreate(PipelineBase):
    pass


class PipelineUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    source_config: PipelineSourceConfig | None = None
    destination_config: PipelineDestinationConfig | None = None
    transform_config: PipelineTransformConfig | None = None
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