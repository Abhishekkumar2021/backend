"""
Pipeline Schemas
----------------

Clean, modern, and fully compatible with:
✓ Universal Pipeline Engine
✓ Transformer registry
✓ Operator-aware execution
✓ Incremental sync
✓ DB-driven Celery Beat scheduler
"""

from datetime import datetime
from typing import Any, Optional, List

from pydantic import BaseModel, Field, field_validator, model_validator

from app.models.database import PipelineStatus


# =============================================================================
# SOURCE CONFIG
# =============================================================================

class PipelineSourceConfig(BaseModel):
    """
    Per-pipeline override of source connector behavior.
    This overrides connector-level config with:
    ✓ table_name OR stream identifier
    ✓ optional custom query
    ✓ optional overrides for batch_size, replication key, etc.
    """
    table_name: Optional[str] = Field(
        None,
        description="Source table/stream name. If omitted, connector decides default."
    )
    query: Optional[str] = Field(
        None,
        description="Custom query. Overrides table-based extraction entirely."
    )

    class Config:
        extra = "allow"   # Allow overriding connector config (e.g., batch_size, cursor fields)


# =============================================================================
# DESTINATION CONFIG
# =============================================================================

class PipelineDestinationConfig(BaseModel):
    """
    Per-pipeline destination behavior overrides.
    Includes:
    ✓ table_name overrides
    ✓ write_mode (append/overwrite/truncate)
    ✓ ability to override connector-specific settings
    """
    table_name: Optional[str] = Field(
        None,
        description="Destination table/collection override."
    )
    write_mode: Optional[str] = Field(
        None,
        description="append | overwrite | truncate | upsert (if supported)"
    )

    class Config:
        extra = "allow"  # Allow API overrides like batch_size, compression, etc.


# =============================================================================
# TRANSFORM CONFIG (Supports both new format + legacy)
# =============================================================================

class PipelineTransformerItem(BaseModel):
    """
    For new transform system:
    {
        "transformer_id": 12,
        "config_override": {...}
    }
    or inline:
    {
        "type": "sql_processor",
        "config": {...}
    }
    """
    transformer_id: Optional[int] = None
    type: Optional[str] = None
    config: Optional[dict[str, Any]] = None
    config_override: Optional[dict[str, Any]] = None


class PipelineTransformConfig(BaseModel):
    """
    Full transformation block.
    Supports:
    ✓ New transformer chain:    { "transformers": [...] }
    ✓ Legacy single processor:  { "processor_type": "sql", "config": {...} }
    """
    transformers: Optional[List[PipelineTransformerItem]] = None
    processor_type: Optional[str] = Field(
        None,
        description="Legacy processor type – used if transformers[] missing."
    )
    config: Optional[dict[str, Any]] = None

    @model_validator(mode="after")
    def validate_structure(self):
        if not self.transformers and not self.processor_type:
            # allow noop by default
            self.processor_type = "noop"
        return self


# =============================================================================
# BASE PIPELINE MODEL
# =============================================================================

class PipelineBase(BaseModel):
    """Shared pipeline fields used across create/read/update."""
    
    name: str = Field(..., description="Human-friendly pipeline name")
    description: Optional[str] = None

    source_connection_id: int = Field(..., description="FK to connection table")
    destination_connection_id: int = Field(..., description="FK to connection table")

    source_config: PipelineSourceConfig = Field(
        ..., description="Config overrides for source connector"
    )
    destination_config: PipelineDestinationConfig = Field(
        ..., description="Config overrides for destination connector"
    )

    transform_config: Optional[PipelineTransformConfig] = Field(
        None,
        description="Transformation chain for the pipeline"
    )

    schedule_cron: Optional[str] = Field(
        None,
        description="Cron string for Celery Beat (e.g. '0 2 * * *')"
    )
    schedule_enabled: bool = Field(
        default=False,
        description="Enable/disable scheduler for this pipeline"
    )

    sync_mode: str = Field(
        default="full_refresh",
        description="full_refresh | incremental | append | upsert"
    )

    @field_validator("schedule_cron")
    def validate_cron_format(cls, v):
        if v is not None and len(v.split()) != 5:
            raise ValueError("Invalid cron format. Expected 'm h dom mon dow'")
        return v


class PipelineCreate(PipelineBase):
    """Used for creating new pipelines."""
    pass


class PipelineUpdate(BaseModel):
    """
    Partial update model. Everything optional.
    """
    name: Optional[str] = None
    description: Optional[str] = None

    source_config: Optional[PipelineSourceConfig] = None
    destination_config: Optional[PipelineDestinationConfig] = None
    transform_config: Optional[PipelineTransformConfig] = None

    schedule_cron: Optional[str] = None
    schedule_enabled: Optional[bool] = None
    status: Optional[PipelineStatus] = None

    @field_validator("schedule_cron")
    def validate_update_cron(cls, v):
        if v is not None and len(v.split()) != 5:
            raise ValueError("Invalid cron format")
        return v


# =============================================================================
# DB REPRESENTATION
# =============================================================================

class PipelineInDBBase(PipelineBase):
    """Fields stored in DB."""

    id: int
    status: PipelineStatus

    created_at: datetime
    updated_at: datetime

    last_run_at: Optional[datetime] = None
    next_run_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class Pipeline(PipelineInDBBase):
    """Full pipeline object returned from API."""
    pass


# =============================================================================
# PIPELINE VERSIONS
# =============================================================================

class PipelineVersionBase(BaseModel):
    """
    Snapshot stored for versioning pipelines.
    """
    pipeline_id: int
    version_number: int
    created_by: Optional[str] = None
    description: Optional[str] = None
    config_snapshot: dict[str, Any] = Field(
        ..., description="Full JSON snapshot of pipeline at time of save"
    )


class PipelineVersionCreate(BaseModel):
    """
    Create only takes a description; snapshot is built server-side.
    """
    description: Optional[str] = None


class PipelineVersionInDBBase(PipelineVersionBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True


class PipelineVersion(PipelineVersionInDBBase):
    pass
