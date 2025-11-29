from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel
from app.models.db_models import JobStatus

class JobLogBase(BaseModel):
    level: str
    message: str
    timestamp: datetime
    log_metadata: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True

class JobBase(BaseModel):
    pipeline_id: int
    status: JobStatus

class JobCreate(JobBase):
    pass

class Job(JobBase):
    id: int
    celery_task_id: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    records_extracted: int
    records_loaded: int
    records_failed: int
    error_message: Optional[str] = None
    created_at: datetime
    
    # Include logs optionally? For now, separate endpoint usually better for logs
    
    class Config:
        from_attributes = True
