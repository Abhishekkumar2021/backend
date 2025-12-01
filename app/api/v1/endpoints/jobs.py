"""Job Management API Endpoints.

Provides job triggering, monitoring, cancellation, and log retrieval
with comprehensive error handling and structured logging.
"""

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError

from app.api.deps import get_db
from app.schemas.job import Job as JobSchema, JobCreate, JobLog
from app.schemas.common import DataResponse
from app.services.job import JobService
from app.worker.app import celery_app
from app.models.database import Pipeline, JobStatus, PipelineStatus
from app.models.database import Job
from app.core.logging import get_logger

logger = get_logger(__name__)
router = APIRouter()


# ===================================================================
# Helper Functions
# ===================================================================

def validate_pipeline(db: Session, pipeline_id: int) -> Pipeline:
    """Validate pipeline exists and return it.
    
    Args:
        db: Database session
        pipeline_id: Pipeline ID to validate
        
    Returns:
        Pipeline object
        
    Raises:
        HTTPException: If pipeline not found
    """
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    
    if not pipeline:
        logger.warning(
            "pipeline_not_found",
            pipeline_id=pipeline_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline with ID {pipeline_id} not found"
        )
    
    return pipeline


def validate_pipeline_executable(pipeline: Pipeline) -> None:
    """Validate pipeline is in executable state.
    
    Args:
        pipeline: Pipeline object to validate
        
    Raises:
        HTTPException: If pipeline cannot be executed
    """
    if pipeline.status not in [PipelineStatus.ACTIVE, PipelineStatus.DRAFT]:
        logger.warning(
            "pipeline_not_executable",
            pipeline_id=pipeline.id,
            status=pipeline.status.value,
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Pipeline must be ACTIVE or DRAFT to trigger jobs. Current status: {pipeline.status.value}"
        )


def check_existing_job(db: Session, pipeline_id: int) -> None:
    """Check for existing running/pending jobs.
    
    Args:
        db: Database session
        pipeline_id: Pipeline ID to check
        
    Raises:
        HTTPException: If job already running
    """
    existing_job = (
        db.query(Job)
        .filter(
            Job.pipeline_id == pipeline_id,
            Job.status.in_([JobStatus.PENDING, JobStatus.RUNNING])
        )
        .first()
    )
    
    if existing_job:
        logger.warning(
            "job_already_running",
            pipeline_id=pipeline_id,
            job_id=existing_job.id,
            status=existing_job.status.value,
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Pipeline already has a {existing_job.status.value} job (ID: {existing_job.id})"
        )


def validate_job_exists(db: Session, job_id: int) -> Job:
    """Validate job exists and return it.
    
    Args:
        db: Database session
        job_id: Job ID to validate
        
    Returns:
        Job object
        
    Raises:
        HTTPException: If job not found
    """
    job = JobService.get_job(db, job_id)
    
    if not job:
        logger.warning(
            "job_not_found",
            job_id=job_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job with ID {job_id} not found"
        )
    
    return job


# ===================================================================
# Job Triggering Endpoints
# ===================================================================

@router.post(
    "/{pipeline_id}",
    response_model=JobSchema,
    status_code=status.HTTP_201_CREATED,
    summary="Trigger pipeline job",
)
def trigger_pipeline_job(
    *,
    db: Session = Depends(get_db),
    pipeline_id: int,
) -> Any:
    """Trigger a new job for a specific pipeline.
    
    Creates a job record and enqueues it for execution by Celery workers.
    Validates pipeline state and checks for conflicting jobs.
    
    Args:
        pipeline_id: ID of the pipeline to execute
        db: Database session
        
    Returns:
        Created job object with Celery task ID
        
    Raises:
        HTTPException: If pipeline not found, not executable, or job already running
    """
    logger.info(
        "job_trigger_requested",
        pipeline_id=pipeline_id,
    )
    
    try:
        # Validate pipeline exists and is executable
        pipeline = validate_pipeline(db, pipeline_id)
        validate_pipeline_executable(pipeline)
        
        # Check for existing running/pending jobs
        check_existing_job(db, pipeline_id)
        
        # Create job record
        job_in = JobCreate(pipeline_id=pipeline_id, status=JobStatus.PENDING)
        job = JobService.create_job(db, job_in)
        
        logger.info(
            "job_created",
            job_id=job.id,
            pipeline_id=pipeline_id,
        )
        
        # Enqueue Celery task
        try:
            task = celery_app.send_task("app.worker.run_pipeline", args=[job.id])
            
            # Update job with Celery task ID
            job.celery_task_id = task.id
            db.add(job)
            db.commit()
            db.refresh(job)
            
            logger.info(
                "job_enqueued",
                job_id=job.id,
                celery_task_id=task.id,
                pipeline_id=pipeline_id,
            )
            
            return job
            
        except Exception as e:
            logger.error(
                "celery_enqueue_failed",
                job_id=job.id,
                error=str(e),
                exc_info=True,
            )
            
            # Update job status to failed
            JobService.update_job_status(
                db,
                job.id,
                JobStatus.FAILED,
                error_message=f"Failed to enqueue task: {str(e)}"
            )
            
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to enqueue job: {str(e)}"
            )
    
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        logger.error(
            "database_error_job_trigger",
            pipeline_id=pipeline_id,
            error=str(e),
            exc_info=True,
        )
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )
    except Exception as e:
        logger.error(
            "unexpected_error_job_trigger",
            pipeline_id=pipeline_id,
            error=str(e),
            exc_info=True,
        )
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error: {str(e)}"
        )


# ===================================================================
# Job Statistics Endpoints
# ===================================================================

@router.get(
    "/summary",
    response_model=DataResponse[Dict],
    summary="Get job summary statistics",
)
def get_jobs_summary(db: Session = Depends(get_db)) -> Any:
    """Get comprehensive summary of job statistics.
    
    Provides aggregate metrics including job counts by status,
    record processing totals, success rate, and average duration.
    
    Args:
        db: Database session
        
    Returns:
        Dictionary with job statistics and metrics
    """
    logger.debug("job_summary_requested")
    
    try:
        # Job counts by status
        total_jobs = db.query(func.count(Job.id)).scalar() or 0
        successful_jobs = db.query(func.count(Job.id)).filter(Job.status == JobStatus.SUCCESS).scalar() or 0
        failed_jobs = db.query(func.count(Job.id)).filter(Job.status == JobStatus.FAILED).scalar() or 0
        running_jobs = db.query(func.count(Job.id)).filter(Job.status == JobStatus.RUNNING).scalar() or 0
        pending_jobs = db.query(func.count(Job.id)).filter(Job.status == JobStatus.PENDING).scalar() or 0
        cancelled_jobs = db.query(func.count(Job.id)).filter(Job.status == JobStatus.CANCELLED).scalar() or 0
        
        # Record metrics
        total_records_extracted = db.query(func.sum(Job.records_extracted)).scalar() or 0
        total_records_loaded = db.query(func.sum(Job.records_loaded)).scalar() or 0
        total_records_failed = db.query(func.sum(Job.records_failed)).scalar() or 0
        
        # Average duration for successful jobs
        avg_duration = (
            db.query(func.avg(Job.duration_seconds))
            .filter(Job.status == JobStatus.SUCCESS, Job.duration_seconds.isnot(None))
            .scalar()
        )
        
        # Success rate
        success_rate = (successful_jobs / total_jobs * 100) if total_jobs > 0 else 0
        
        summary = {
            "total_jobs": total_jobs,
            "successful_jobs": successful_jobs,
            "failed_jobs": failed_jobs,
            "running_jobs": running_jobs,
            "pending_jobs": pending_jobs,
            "cancelled_jobs": cancelled_jobs,
            "total_records_extracted": total_records_extracted,
            "total_records_loaded": total_records_loaded,
            "total_records_failed": total_records_failed,
            "average_duration_seconds": round(avg_duration, 2) if avg_duration else None,
            "success_rate_percent": round(success_rate, 2)
        }
        
        logger.debug(
            "job_summary_retrieved",
            total_jobs=total_jobs,
            success_rate=success_rate,
        )
        
        return DataResponse(
            success=True,
            message="Job summary retrieved successfully",
            data=summary
        )
    
    except SQLAlchemyError as e:
        logger.error(
            "database_error_job_summary",
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )


# ===================================================================
# Job Listing and Details Endpoints
# ===================================================================

@router.get(
    "/",
    response_model=list[JobSchema],
    summary="List jobs",
)
def list_jobs(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    pipeline_id: Optional[int] = Query(None, description="Filter by pipeline ID"),
    status: Optional[JobStatus] = Query(None, description="Filter by job status"),
) -> Any:
    """Retrieve a paginated list of jobs with optional filtering.
    
    Supports filtering by pipeline and status with pagination.
    
    Args:
        db: Database session
        skip: Pagination offset (default: 0)
        limit: Max records to return (default: 100, max: 1000)
        pipeline_id: Optional filter by pipeline ID
        status: Optional filter by job status
        
    Returns:
        List of job objects matching the filters
    """
    logger.debug(
        "job_list_requested",
        skip=skip,
        limit=limit,
        pipeline_id=pipeline_id,
        status=status.value if status else None,
    )
    
    try:
        # Validate pipeline exists if filtering by pipeline_id
        if pipeline_id:
            validate_pipeline(db, pipeline_id)
        
        jobs = JobService.get_jobs(
            db,
            skip=skip,
            limit=limit,
            pipeline_id=pipeline_id,
            status=status
        )
        
        logger.debug(
            "job_list_retrieved",
            count=len(jobs),
        )
        
        return jobs
    
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        logger.error(
            "database_error_job_list",
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )


@router.get(
    "/{job_id}",
    response_model=JobSchema,
    summary="Get job details",
)
def read_job(
    job_id: int,
    db: Session = Depends(get_db),
) -> Any:
    """Get detailed information about a specific job.
    
    Returns complete job information including status, metrics,
    and error details if applicable.
    
    Args:
        job_id: ID of the job to retrieve
        db: Database session
        
    Returns:
        Job object with all details
        
    Raises:
        HTTPException: If job not found
    """
    logger.debug(
        "job_details_requested",
        job_id=job_id,
    )
    
    try:
        job = validate_job_exists(db, job_id)
        
        logger.debug(
            "job_details_retrieved",
            job_id=job_id,
            status=job.status.value,
        )
        
        return job
    
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        logger.error(
            "database_error_job_details",
            job_id=job_id,
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )


# ===================================================================
# Job Logs Endpoint
# ===================================================================

@router.get(
    "/{job_id}/logs",
    response_model=list[JobLog],
    summary="Get job logs",
)
def read_job_logs(
    *,
    db: Session = Depends(get_db),
    job_id: int,
    level: Optional[str] = Query(None, description="Filter by log level (INFO, WARNING, ERROR)"),
    limit: Optional[int] = Query(None, ge=1, le=10000, description="Maximum number of logs to return"),
) -> Any:
    """Get logs for a specific job with optional filtering.
    
    Retrieves log entries for job execution with optional level filtering
    and result limiting.
    
    Args:
        job_id: ID of the job
        db: Database session
        level: Optional log level filter (INFO, WARNING, ERROR, DEBUG, CRITICAL)
        limit: Maximum number of logs to return
        
    Returns:
        List of job log entries
        
    Raises:
        HTTPException: If job not found or invalid log level
    """
    logger.debug(
        "job_logs_requested",
        job_id=job_id,
        level=level,
        limit=limit,
    )
    
    try:
        # Verify job exists
        validate_job_exists(db, job_id)
        
        # Validate log level
        valid_levels = ["INFO", "WARNING", "ERROR", "DEBUG", "CRITICAL"]
        if level and level.upper() not in valid_levels:
            logger.warning(
                "invalid_log_level",
                job_id=job_id,
                level=level,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid log level: {level}. Must be one of: {', '.join(valid_levels)}"
            )
        
        logs = JobService.get_job_logs(
            db,
            job_id,
            level=level.upper() if level else None,
            limit=limit
        )
        
        logger.debug(
            "job_logs_retrieved",
            job_id=job_id,
            count=len(logs),
        )
        
        return logs
    
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        logger.error(
            "database_error_job_logs",
            job_id=job_id,
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )


# ===================================================================
# Job Control Endpoints
# ===================================================================

@router.post(
    "/{job_id}/cancel",
    response_model=JobSchema,
    summary="Cancel job",
)
def cancel_job(
    *,
    db: Session = Depends(get_db),
    job_id: int,
) -> Any:
    """Cancel a pending or running job.
    
    Attempts to revoke the Celery task and updates job status to CANCELLED.
    Only pending or running jobs can be cancelled.
    
    Args:
        job_id: ID of the job to cancel
        db: Database session
        
    Returns:
        Updated job object with CANCELLED status
        
    Raises:
        HTTPException: If job not found or cannot be cancelled
    """
    logger.info(
        "job_cancel_requested",
        job_id=job_id,
    )
    
    try:
        job = validate_job_exists(db, job_id)
        
        # Check if job can be cancelled
        if job.status not in [JobStatus.PENDING, JobStatus.RUNNING]:
            logger.warning(
                "job_cannot_be_cancelled",
                job_id=job_id,
                status=job.status.value,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cannot cancel job in status: {job.status.value}"
            )
        
        # Attempt to revoke Celery task
        if job.celery_task_id:
            try:
                celery_app.control.revoke(job.celery_task_id, terminate=True)
                logger.info(
                    "celery_task_revoked",
                    celery_task_id=job.celery_task_id,
                )
            except Exception as e:
                logger.warning(
                    "celery_task_revoke_failed",
                    celery_task_id=job.celery_task_id,
                    error=str(e),
                )
        
        # Update job status
        cancelled_job = JobService.cancel_job(db, job_id)
        
        logger.info(
            "job_cancelled",
            job_id=job_id,
        )
        
        return cancelled_job
    
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        logger.error(
            "database_error_job_cancel",
            job_id=job_id,
            error=str(e),
            exc_info=True,
        )
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )


@router.delete(
    "/{job_id}",
    response_model=JobSchema,
    summary="Delete job",
)
def delete_job(
    *,
    db: Session = Depends(get_db),
    job_id: int,
) -> Any:
    """Delete a job record.
    
    Only completed, failed, or cancelled jobs can be deleted.
    Active jobs must be cancelled first.
    
    Args:
        job_id: ID of the job to delete
        db: Database session
        
    Returns:
        Deleted job object
        
    Raises:
        HTTPException: If job not found or is still active
    """
    logger.info(
        "job_delete_requested",
        job_id=job_id,
    )
    
    try:
        job = validate_job_exists(db, job_id)
        
        # Prevent deletion of running jobs
        if job.status in [JobStatus.PENDING, JobStatus.RUNNING]:
            logger.warning(
                "job_cannot_be_deleted",
                job_id=job_id,
                status=job.status.value,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cannot delete job in status: {job.status.value}. Cancel it first."
            )
        
        # Delete job
        db.delete(job)
        db.commit()
        
        logger.info(
            "job_deleted",
            job_id=job_id,
        )
        
        return job
    
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        logger.error(
            "database_error_job_delete",
            job_id=job_id,
            error=str(e),
            exc_info=True,
        )
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )