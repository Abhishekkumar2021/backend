"""
Jobs API Endpoints — WITH CORRELATION ID + NEW DB SESSION
"""

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Response
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.core.database import get_db_session   # ✅ NEW
from app.core.tracing import ensure_correlation_id
from app.schemas.job import Job as JobSchema, JobCreate, JobLog
from app.services.job import JobService
from app.worker.app import celery_app
from app.models.database import Pipeline, JobStatus, Job

router = APIRouter()


# ------------------------------------------------------------
# Dependency wrapper for FastAPI (required because get_db_session
# is a context manager, not a FastAPI dependency by default)
# ------------------------------------------------------------
def get_db():
    """Wrap get_db_session() so FastAPI can use it as a dependency."""
    with get_db_session() as session:
        yield session


# ===================================================================
# Trigger Job
# ===================================================================
@router.post(
    "/{pipeline_id}",
    response_model=JobSchema,
    status_code=status.HTTP_201_CREATED
)
def trigger_pipeline_job(
    *,
    db: Session = Depends(get_db),
    pipeline_id: int,
    response: Response,
) -> Any:
    """
    Trigger a new job for a specific pipeline with correlation_id.
    """
    # Generate correlation ID (or reuse if already present)
    correlation_id = ensure_correlation_id()

    # Validate pipeline
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    # Create job
    job_in = JobCreate(
        pipeline_id=pipeline_id,
        status=JobStatus.PENDING
    )
    job = JobService.create_job(db, job_in)

    # Store correlation ID
    job.correlation_id = correlation_id
    db.commit()
    db.refresh(job)

    # Send Celery task with correlation ID
    task = celery_app.send_task(
        "pipeline.execute",
        args=[job.id],
        kwargs={"correlation_id": correlation_id}
    )

    # Store task ID
    job.celery_task_id = task.id
    db.commit()
    db.refresh(job)

    # Send correlation ID to client
    response.headers["X-Correlation-ID"] = correlation_id

    return job


# ===================================================================
# Summary
# ===================================================================
@router.get("/summary", response_model=dict)
def get_jobs_summary(db: Session = Depends(get_db)) -> Any:
    """Return aggregated job metrics."""
    total_jobs = db.query(func.count(Job.id)).scalar() or 0
    successful = db.query(func.count(Job.id)).filter(Job.status == JobStatus.SUCCESS).scalar() or 0
    failed = db.query(func.count(Job.id)).filter(Job.status == JobStatus.FAILED).scalar() or 0
    running = db.query(func.count(Job.id)).filter(Job.status == JobStatus.RUNNING).scalar() or 0
    pending = db.query(func.count(Job.id)).filter(Job.status == JobStatus.PENDING).scalar() or 0
    cancelled = db.query(func.count(Job.id)).filter(Job.status == JobStatus.CANCELLED).scalar() or 0

    extracted = db.query(func.sum(Job.records_extracted)).scalar() or 0
    loaded = db.query(func.sum(Job.records_loaded)).scalar() or 0

    return {
        "total_jobs": total_jobs,
        "successful_jobs": successful,
        "failed_jobs": failed,
        "running_jobs": running,
        "pending_jobs": pending,
        "cancelled_jobs": cancelled,
        "total_records_extracted": extracted,
        "total_records_loaded": loaded,
    }


# ===================================================================
# List Jobs
# ===================================================================
@router.get("/", response_model=list[JobSchema])
def list_jobs(
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100,
    pipeline_id: Optional[int] = None,
    correlation_id: Optional[str] = None,
) -> Any:
    """Retrieve jobs with optional filtering."""
    query = db.query(Job)

    if pipeline_id:
        query = query.filter(Job.pipeline_id == pipeline_id)

    if correlation_id:
        query = query.filter(Job.correlation_id == correlation_id)

    jobs = query.order_by(Job.created_at.desc()).offset(skip).limit(limit).all()
    return jobs


# ===================================================================
# Get Job Details
# ===================================================================
@router.get("/{job_id}", response_model=JobSchema)
def read_job(job_id: int, db: Session = Depends(get_db)) -> Any:
    job = JobService.get_job(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


# ===================================================================
# Get Job Logs
# ===================================================================
@router.get("/{job_id}/logs", response_model=list[JobLog])
def read_job_logs(job_id: int, db: Session = Depends(get_db)) -> Any:
    return JobService.get_job_logs(db, job_id)


# ===================================================================
# Trace Full Pipeline by Correlation ID
# ===================================================================
@router.get("/trace/{correlation_id}", response_model=dict)
def trace_by_correlation_id(correlation_id: str, db: Session = Depends(get_db)) -> Any:
    """
    Return all system activity linked to a correlation ID:
    - Jobs
    - PipelineRuns
    - OperatorRuns
    - Logs
    """
    from app.models.database import PipelineRun, OperatorRun, JobLog, OperatorRunLog

    jobs = db.query(Job).filter(Job.correlation_id == correlation_id).all()
    pipeline_runs = db.query(PipelineRun).filter(
        PipelineRun.correlation_id == correlation_id
    ).all()
    operator_runs = db.query(OperatorRun).filter(
        OperatorRun.correlation_id == correlation_id
    ).all()
    job_logs = db.query(JobLog).filter(
        JobLog.correlation_id == correlation_id
    ).order_by(JobLog.timestamp).all()
    operator_logs = db.query(OperatorRunLog).filter(
        OperatorRunLog.correlation_id == correlation_id
    ).order_by(OperatorRunLog.timestamp).all()

    return {
        "correlation_id": correlation_id,
        "jobs": [{"id": j.id, "pipeline_id": j.pipeline_id, "status": j.status.value} for j in jobs],
        "pipeline_runs": [{"id": pr.id, "status": pr.status.value} for pr in pipeline_runs],
        "operator_runs": [{"id": o.id, "operator_name": o.operator_name} for o in operator_runs],
        "job_logs_count": len(job_logs),
        "operator_logs_count": len(operator_logs),
    }
