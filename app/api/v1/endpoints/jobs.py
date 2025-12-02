"""
Job Management API Endpoints (Updated for new DB session pattern)

Rewritten to use:
    from app.core.database import get_db_session

Removes old dependency injection and ensures:
✓ Safe session handling (commit/rollback)
✓ No leaking connections
✓ Clean, isolated DB scopes
✓ Consistent error handling
"""

from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError

from app.core.database import get_db_session
from app.schemas.job import Job as JobSchema, JobCreate, JobLog
from app.schemas.common import DataResponse
from app.services.job import JobService
from app.worker.app import celery_app
from app.models.database import Pipeline, JobStatus, PipelineStatus, Job
from app.core.logging import get_logger

logger = get_logger(__name__)
router = APIRouter()


# ===================================================================
# Helper Functions
# ===================================================================

def validate_pipeline(db, pipeline_id: int) -> Pipeline:
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(404, f"Pipeline {pipeline_id} not found")
    return pipeline


def validate_pipeline_executable(pipeline: Pipeline):
    if pipeline.status not in [PipelineStatus.ACTIVE, PipelineStatus.DRAFT]:
        raise HTTPException(
            400,
            f"Pipeline must be ACTIVE or DRAFT. Current: {pipeline.status.value}"
        )


def check_existing_job(db, pipeline_id: int):
    existing = (
        db.query(Job)
        .filter(
            Job.pipeline_id == pipeline_id,
            Job.status.in_([JobStatus.PENDING, JobStatus.RUNNING])
        )
        .first()
    )

    if existing:
        raise HTTPException(
            409,
            f"Pipeline already has a {existing.status.value} job (ID: {existing.id})"
        )


def validate_job_exists(db, job_id: int) -> Job:
    job = JobService.get_job(db, job_id)
    if not job:
        raise HTTPException(404, f"Job {job_id} not found")
    return job


# ===================================================================
# Trigger Job
# ===================================================================

@router.post(
    "/{pipeline_id}",
    response_model=JobSchema,
    status_code=201,
    summary="Trigger pipeline job",
)
def trigger_pipeline_job(pipeline_id: int):
    with get_db_session() as db:
        pipeline = validate_pipeline(db, pipeline_id)
        validate_pipeline_executable(pipeline)
        check_existing_job(db, pipeline_id)

        job = JobService.create_job(
            db,
            JobCreate(pipeline_id=pipeline_id, status=JobStatus.PENDING),
        )

        try:
            task = celery_app.send_task(
                "app.worker.tasks.pipeline_tasks.execute_pipeline",
                args=[job.id],
            )
            job.celery_task_id = task.id
            db.flush()

        except Exception as e:
            JobService.update_job_status(
                db,
                job.id,
                JobStatus.FAILED,
                error_message=f"Failed to enqueue Celery task: {e}",
            )
            raise HTTPException(500, f"Failed to enqueue job: {e}")

        return job


# ===================================================================
# Job Summary
# ===================================================================

@router.get(
    "/summary",
    response_model=DataResponse[Dict],
    summary="Job summary statistics"
)
def get_jobs_summary():
    with get_db_session() as db:
        try:
            total = db.query(func.count(Job.id)).scalar() or 0
            success = db.query(func.count(Job.id)).filter(Job.status == JobStatus.SUCCESS).scalar() or 0
            failed = db.query(func.count(Job.id)).filter(Job.status == JobStatus.FAILED).scalar() or 0
            running = db.query(func.count(Job.id)).filter(Job.status == JobStatus.RUNNING).scalar() or 0
            pending = db.query(func.count(Job.id)).filter(Job.status == JobStatus.PENDING).scalar() or 0
            cancelled = db.query(func.count(Job.id)).filter(Job.status == JobStatus.CANCELLED).scalar() or 0

            total_extracted = db.query(func.sum(Job.records_extracted)).scalar() or 0
            total_loaded = db.query(func.sum(Job.records_loaded)).scalar() or 0
            total_failed = db.query(func.sum(Job.records_failed)).scalar() or 0

            avg_duration = (
                db.query(func.avg(Job.duration_seconds))
                .filter(Job.status == JobStatus.SUCCESS)
                .scalar()
            )

            summary = {
                "total_jobs": total,
                "successful_jobs": success,
                "failed_jobs": failed,
                "running_jobs": running,
                "pending_jobs": pending,
                "cancelled_jobs": cancelled,
                "total_records_extracted": total_extracted,
                "total_records_loaded": total_loaded,
                "total_records_failed": total_failed,
                "average_duration_seconds": round(avg_duration, 2) if avg_duration else None,
                "success_rate_percent": round((success / total * 100), 2) if total else 0,
            }

            return DataResponse(
                success=True,
                message="Job summary retrieved",
                data=summary,
            )

        except Exception as e:
            raise HTTPException(500, f"Error fetching summary: {e}")


# ===================================================================
# List Jobs
# ===================================================================

@router.get("/", response_model=list[JobSchema])
def list_jobs(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    pipeline_id: Optional[int] = None,
    status: Optional[JobStatus] = None,
):
    with get_db_session() as db:
        if pipeline_id:
            validate_pipeline(db, pipeline_id)

        jobs = JobService.get_jobs(
            db,
            skip=skip,
            limit=limit,
            pipeline_id=pipeline_id,
            status=status,
        )
        return jobs


# ===================================================================
# Job Details
# ===================================================================

@router.get("/{job_id}", response_model=JobSchema)
def read_job(job_id: int):
    with get_db_session() as db:
        return validate_job_exists(db, job_id)


# ===================================================================
# Job Logs
# ===================================================================

@router.get("/{job_id}/logs", response_model=list[JobLog])
def read_job_logs(
    job_id: int,
    level: Optional[str] = None,
    limit: Optional[int] = Query(None, ge=1, le=10_000),
):
    with get_db_session() as db:
        validate_job_exists(db, job_id)

        valid_levels = ["INFO", "WARNING", "ERROR", "DEBUG", "CRITICAL"]
        if level and level.upper() not in valid_levels:
            raise HTTPException(400, f"Invalid log level: {level}")

        logs = JobService.get_job_logs(
            db,
            job_id,
            level.upper() if level else None,
            limit
        )
        return logs


# ===================================================================
# Cancel Job
# ===================================================================

@router.post("/{job_id}/cancel", response_model=JobSchema)
def cancel_job(job_id: int):
    with get_db_session() as db:
        job = validate_job_exists(db, job_id)

        if job.status not in [JobStatus.PENDING, JobStatus.RUNNING]:
            raise HTTPException(
                400,
                f"Cannot cancel job in status: {job.status.value}",
            )

        if job.celery_task_id:
            try:
                celery_app.control.revoke(job.celery_task_id, terminate=True)
            except Exception as e:
                logger.warning("celery_revoke_failed", task_id=job.celery_task_id, error=str(e))

        return JobService.cancel_job(db, job_id)


# ===================================================================
# Delete Job
# ===================================================================

@router.delete("/{job_id}", response_model=JobSchema)
def delete_job(job_id: int):
    with get_db_session() as db:
        job = validate_job_exists(db, job_id)

        if job.status in [JobStatus.PENDING, JobStatus.RUNNING]:
            raise HTTPException(
                400,
                f"Cannot delete job in status {job.status.value}. Cancel it first."
            )

        db.delete(job)
        return job
