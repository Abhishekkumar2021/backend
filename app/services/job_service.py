"""Service for managing job-related operations."""

import json
from datetime import UTC, datetime
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.models.database import Job, JobLog, JobStatus
from app.schemas.job import Job as JobSchema
from app.schemas.job import JobCreate
from app.services.cache import get_cache
from app.core.logging import get_logger

logger = get_logger(__name__)


class JobService:
    """Service for managing job-related operations."""

    @staticmethod
    def _publish_job_update(job_id: int, job_data: JobSchema) -> None:
        """Publish job updates to Redis Pub/Sub for real-time monitoring.
        
        Args:
            job_id: Job ID
            job_data: Job schema with updated data
        """
        try:
            cache = get_cache()
            if cache.is_available():
                redis_client = cache.redis_client
                channel = f"job_updates:{job_id}"
                message = json.dumps(job_data.model_dump(mode='json'), default=str)
                redis_client.publish(channel, message)
                logger.debug("Published job update: job_id=%d, status=%s", job_id, job_data.status)
        except Exception as e:
            logger.warning("Failed to publish job update for job %d: %s", job_id, e)

    @staticmethod
    def create_job(db: Session, job_in: JobCreate) -> Job:
        """Create a new job record.
        
        Args:
            db: Database session
            job_in: Job creation schema
            
        Returns:
            Created Job object
        """
        logger.info("Creating job for pipeline_id=%d", job_in.pipeline_id)
        
        db_job = Job(
            pipeline_id=job_in.pipeline_id,
            status=job_in.status,
            created_at=datetime.now(UTC),
        )
        db.add(db_job)
        db.commit()
        db.refresh(db_job)
        
        logger.info("Job created: job_id=%d, pipeline_id=%d", db_job.id, job_in.pipeline_id)
        
        # Publish initial job state
        JobService._publish_job_update(db_job.id, JobSchema.model_validate(db_job))
        
        return db_job

    @staticmethod
    def get_job(db: Session, job_id: int) -> Optional[Job]:
        """Get job by ID.
        
        Args:
            db: Database session
            job_id: Job ID
            
        Returns:
            Job object or None if not found
        """
        job = db.query(Job).filter(Job.id == job_id).first()
        
        if not job:
            logger.warning("Job not found: job_id=%d", job_id)
        
        return job

    @staticmethod
    def get_jobs(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        pipeline_id: Optional[int] = None,
        status: Optional[JobStatus] = None,
    ) -> List[Job]:
        """List jobs with optional filtering.
        
        Args:
            db: Database session
            skip: Pagination offset
            limit: Max results to return
            pipeline_id: Filter by pipeline ID
            status: Filter by job status
            
        Returns:
            List of Job objects
        """
        query = db.query(Job)
        
        if pipeline_id:
            query = query.filter(Job.pipeline_id == pipeline_id)
            logger.debug("Filtering jobs by pipeline_id=%d", pipeline_id)
        
        if status:
            query = query.filter(Job.status == status)
            logger.debug("Filtering jobs by status=%s", status)
        
        jobs = query.order_by(Job.created_at.desc()).offset(skip).limit(limit).all()
        
        logger.debug("Retrieved %d job(s)", len(jobs))
        
        return jobs

    @staticmethod
    def update_job_status(
        db: Session,
        job_id: int,
        status: JobStatus,
        error_message: Optional[str] = None,
        error_traceback: Optional[str] = None,
    ) -> Optional[Job]:
        """Update job status and timestamps.
        
        Args:
            db: Database session
            job_id: Job ID
            status: New job status
            error_message: Optional error message
            error_traceback: Optional error traceback
            
        Returns:
            Updated Job object or None if not found
        """
        job = JobService.get_job(db, job_id)
        if not job:
            logger.error("Cannot update job status - job not found: job_id=%d", job_id)
            return None

        old_status = job.status
        job.status = status
        
        # Update timestamps based on status
        if status == JobStatus.RUNNING:
            job.started_at = datetime.now(UTC)
            logger.info("Job started: job_id=%d", job_id)
            
        elif status in [JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.CANCELLED]:
            job.completed_at = datetime.now(UTC)
            
            if job.started_at:
                job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
                logger.info(
                    "Job completed: job_id=%d, status=%s, duration=%.2fs",
                    job_id,
                    status.value,
                    job.duration_seconds,
                )
            else:
                logger.warning("Job completed without started_at timestamp: job_id=%d", job_id)

        # Update error details
        if error_message:
            job.error_message = error_message
            logger.error("Job error: job_id=%d, error=%s", job_id, error_message)
        
        if error_traceback:
            job.error_traceback = error_traceback

        db.commit()
        db.refresh(job)
        
        logger.info(
            "Job status updated: job_id=%d, %s -> %s",
            job_id,
            old_status.value,
            status.value,
        )
        
        # Publish updated job state
        JobService._publish_job_update(job_id, JobSchema.model_validate(job))
        
        return job

    @staticmethod
    def log_message(
        db: Session,
        job_id: int,
        level: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> JobLog:
        """Add a log entry for a job.
        
        Args:
            db: Database session
            job_id: Job ID
            level: Log level (INFO, WARNING, ERROR)
            message: Log message
            metadata: Optional metadata dictionary
            
        Returns:
            Created JobLog object
        """
        log = JobLog(
            job_id=job_id,
            level=level,
            message=message,
            log_metadata=metadata,
            timestamp=datetime.now(UTC),
        )
        db.add(log)
        db.commit()
        db.refresh(log)
        
        logger.debug("Job log added: job_id=%d, level=%s", job_id, level)
        
        return log

    @staticmethod
    def get_job_logs(
        db: Session,
        job_id: int,
        level: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[JobLog]:
        """Get logs for a job with optional filtering.
        
        Args:
            db: Database session
            job_id: Job ID
            level: Filter by log level
            limit: Max logs to return
            
        Returns:
            List of JobLog objects
        """
        query = db.query(JobLog).filter(JobLog.job_id == job_id)
        
        if level:
            query = query.filter(JobLog.level == level)
        
        query = query.order_by(JobLog.timestamp.asc())
        
        if limit:
            query = query.limit(limit)
        
        logs = query.all()
        
        logger.debug("Retrieved %d log(s) for job_id=%d", len(logs), job_id)
        
        return logs

    @staticmethod
    def update_job_metrics(
        db: Session,
        job_id: int,
        records_extracted: Optional[int] = None,
        records_loaded: Optional[int] = None,
        records_failed: Optional[int] = None,
    ) -> Optional[Job]:
        """Update job metrics (record counts).
        
        Args:
            db: Database session
            job_id: Job ID
            records_extracted: Number of records extracted
            records_loaded: Number of records loaded
            records_failed: Number of records failed
            
        Returns:
            Updated Job object or None if not found
        """
        job = JobService.get_job(db, job_id)
        if not job:
            return None

        if records_extracted is not None:
            job.records_extracted = records_extracted
        
        if records_loaded is not None:
            job.records_loaded = records_loaded
        
        if records_failed is not None:
            job.records_failed = records_failed

        db.commit()
        db.refresh(job)
        
        logger.debug(
            "Job metrics updated: job_id=%d, extracted=%s, loaded=%s, failed=%s",
            job_id,
            records_extracted,
            records_loaded,
            records_failed,
        )
        
        return job

    @staticmethod
    def cancel_job(db: Session, job_id: int) -> Optional[Job]:
        """Cancel a running job.
        
        Args:
            db: Database session
            job_id: Job ID
            
        Returns:
            Updated Job object or None if not found
        """
        job = JobService.get_job(db, job_id)
        if not job:
            return None

        if job.status not in [JobStatus.PENDING, JobStatus.RUNNING]:
            logger.warning(
                "Cannot cancel job - invalid status: job_id=%d, status=%s",
                job_id,
                job.status.value,
            )
            return job

        logger.info("Cancelling job: job_id=%d", job_id)
        
        return JobService.update_job_status(
            db,
            job_id,
            JobStatus.CANCELLED,
            error_message="Job cancelled by user",
        )