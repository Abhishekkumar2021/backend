"""
Job & JobLog Schemas
--------------------

These schemas define:
✓ Job creation/update models
✓ Job DB representation
✓ Structured logging for job execution
✓ Safe defaults for numeric counters
"""

from datetime import datetime
from typing import Any, Optional, Dict

from pydantic import BaseModel, Field

from app.models.database import JobStatus


# =============================================================================
# JOB LOG MODELS
# =============================================================================

class JobLogBase(BaseModel):
    """
    Base fields for logs emitted during job execution.
    """

    level: str = Field(..., description="Log level (INFO/WARN/ERROR)")
    message: str = Field(..., description="Log message")
    timestamp: datetime = Field(..., description="Timestamp of log event")
    metadata: Optional[Dict[str, Any]] = Field(
        default=None, description="Structured log metadata"
    )

    class Config:
        from_attributes = True


class JobLog(JobLogBase):
    """
    Full JobLog object loaded from database.
    """

    id: int
    job_id: int


# =============================================================================
# JOB MODELS
# =============================================================================

class JobBase(BaseModel):
    """
    Minimal Job definition.
    """

    pipeline_id: int = Field(..., description="Pipeline associated with this job")
    status: JobStatus = Field(..., description="Current job execution status")


class JobCreate(JobBase):
    """
    Used when creating a new job record.
    """
    pass


class Job(JobBase):
    """
    Full Job model returned from DB.
    Designed to be read-only from API layer.
    """

    id: int
    celery_task_id: Optional[str] = Field(
        default=None, description="Celery task ID associated with this job"
    )
    started_at: Optional[datetime] = Field(
        default=None, description="Time when job execution started"
    )
    completed_at: Optional[datetime] = Field(
        default=None, description="Time when job execution finished"
    )
    duration_seconds: Optional[float] = Field(
        default=None, description="Execution duration in seconds"
    )

    # IMPORTANT — set safe defaults (Fixes silent validation failures)
    records_extracted: int = Field(
        default=0, description="Total number of extracted records"
    )
    records_loaded: int = Field(
        default=0, description="Total number of successfully loaded records"
    )
    records_failed: int = Field(
        default=0, description="Total number of failed records"
    )

    error_message: Optional[str] = Field(
        default=None, description="Error message if job failed"
    )

    created_at: datetime

    class Config:
        from_attributes = True
