from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.api.deps import get_db
from app.schemas.job import Job as JobSchema, JobCreate, JobLog
from app.services.job_service import JobService
from app.core.celery_app import celery_app
from app.models.database import Pipeline, JobStatus
from app.models.database import Job


router = APIRouter()


@router.post("/{pipeline_id}", response_model=JobSchema, status_code=status.HTTP_201_CREATED)
def trigger_pipeline_job(
    *,
    db: Session = Depends(get_db),
    pipeline_id: int,
) -> Any:
    """
    Trigger a new job for a specific pipeline.
    """
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")

    job_in = JobCreate(pipeline_id=pipeline_id, status=JobStatus.PENDING)
    job = JobService.create_job(db, job_in)

    # Enqueue the Celery task
    # The run_pipeline task no longer takes master_password as an argument.
    # It expects it to be available in the worker's environment.
    task = celery_app.send_task("app.worker.run_pipeline", args=[job.id])

    # Update job with Celery task ID
    job.celery_task_id = task.id
    db.add(job)
    db.commit()
    db.refresh(job)

    return job


@router.get("/summary", response_model=dict)
def get_jobs_summary(db: Session = Depends(get_db)) -> Any:
    """
    Get a summary of job statistics (total, success, failed, running, etc.).
    """
    total_jobs = db.query(func.count(Job.id)).scalar()
    successful_jobs = db.query(func.count(Job.id)).filter(Job.status == JobStatus.SUCCESS).scalar()
    failed_jobs = db.query(func.count(Job.id)).filter(Job.status == JobStatus.FAILED).scalar()
    running_jobs = db.query(func.count(Job.id)).filter(Job.status == JobStatus.RUNNING).scalar()
    pending_jobs = db.query(func.count(Job.id)).filter(Job.status == JobStatus.PENDING).scalar()
    cancelled_jobs = db.query(func.count(Job.id)).filter(Job.status == JobStatus.CANCELLED).scalar()

    total_records_extracted = db.query(func.sum(Job.records_extracted)).scalar()
    total_records_loaded = db.query(func.sum(Job.records_loaded)).scalar()

    return {
        "total_jobs": total_jobs,
        "successful_jobs": successful_jobs,
        "failed_jobs": failed_jobs,
        "running_jobs": running_jobs,
        "pending_jobs": pending_jobs,
        "cancelled_jobs": cancelled_jobs,
        "total_records_extracted": total_records_extracted if total_records_extracted else 0,
        "total_records_loaded": total_records_loaded if total_records_loaded else 0,
    }


@router.get("/", response_model=list[JobSchema])
def list_jobs(
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100,
    pipeline_id: int | None = None,
) -> Any:
    """
    Retrieve a list of jobs.
    """
    jobs = JobService.get_jobs(db, skip=skip, limit=limit, pipeline_id=pipeline_id)
    return jobs


@router.get("/{job_id}", response_model=JobSchema)
def read_job(
    job_id: int,
    db: Session = Depends(get_db),
) -> Any:
    """
    Get a specific job by ID.
    """
    job = JobService.get_job(db, job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return job


@router.get("/{job_id}/logs", response_model=list[JobLog])
def read_job_logs(
    *,
    db: Session = Depends(get_db),
    job_id: int,
) -> Any:
    """Get logs for a specific job.
    """
    logs = JobService.get_job_logs(db, job_id)
    return logs
