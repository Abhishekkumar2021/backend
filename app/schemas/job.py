from datetime import datetime
from typing import Any

from pydantic import BaseModel

from app.models.database import JobStatus


class JobLogBase(BaseModel):
    level: str
    message: str
    timestamp: datetime
    log_metadata: dict[str, Any] | None = None

    class Config:
        from_attributes = True


class JobLog(JobLogBase):
    id: int
    job_id: int


class JobBase(BaseModel):
    pipeline_id: int
    status: JobStatus


class JobCreate(JobBase):
    pass


class Job(JobBase):
    id: int
    celery_task_id: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_seconds: float | None = None
    records_extracted: int
    records_loaded: int
    records_failed: int
    error_message: str | None = None
    created_at: datetime

    class Config:
        from_attributes = True
