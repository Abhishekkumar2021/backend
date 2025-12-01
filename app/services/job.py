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

    # ===================================================================
    # Publish Updates to Redis
    # ===================================================================
    @staticmethod
    def _publish_job_update(job_id: int, job_data: JobSchema) -> None:
        """Publish job updates to Redis Pub/Sub for real-time monitoring."""
        try:
            cache = get_cache()

            if cache.is_available():
                redis_client = cache.redis_client
                channel = f"job_updates:{job_id}"
                message = json.dumps(job_data.model_dump(mode="json"), default=str)

                redis_client.publish(channel, message)

                logger.debug(
                    "job_update_published",
                    job_id=job_id,
                    channel=channel,
                    status=job_data.status,
                )

        except Exception as e:
            logger.warning(
                "job_update_publish_failed",
                job_id=job_id,
                error=str(e),
                exc_info=True,
            )

    # ===================================================================
    # Create Job
    # ===================================================================
    @staticmethod
    def create_job(db: Session, job_in: JobCreate) -> Job:
        logger.info(
            "job_creation_requested",
            pipeline_id=job_in.pipeline_id,
            initial_status=job_in.status.value,
        )

        db_job = Job(
            pipeline_id=job_in.pipeline_id,
            status=job_in.status,
            created_at=datetime.now(UTC),
        )
        db.add(db_job)
        db.commit()
        db.refresh(db_job)

        logger.info(
            "job_created",
            job_id=db_job.id,
            pipeline_id=db_job.pipeline_id,
            status=db_job.status.value,
        )

        # Publish initial job state
        JobService._publish_job_update(db_job.id, JobSchema.model_validate(db_job))

        return db_job

    # ===================================================================
    # Get Job
    # ===================================================================
    @staticmethod
    def get_job(db: Session, job_id: int) -> Optional[Job]:
        job = db.query(Job).filter(Job.id == job_id).first()

        if not job:
            logger.warning(
                "job_not_found",
                job_id=job_id,
            )

        return job

    # ===================================================================
    # List Jobs
    # ===================================================================
    @staticmethod
    def get_jobs(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        pipeline_id: Optional[int] = None,
        status: Optional[JobStatus] = None,
    ) -> List[Job]:
        logger.info(
            "job_list_requested",
            skip=skip,
            limit=limit,
            pipeline_id=pipeline_id,
            status=status.value if status else None,
        )

        query = db.query(Job)

        if pipeline_id:
            query = query.filter(Job.pipeline_id == pipeline_id)
            logger.debug(
                "job_list_filter_pipeline",
                pipeline_id=pipeline_id,
            )

        if status:
            query = query.filter(Job.status == status)
            logger.debug(
                "job_list_filter_status",
                status=status.value,
            )

        jobs = query.order_by(Job.created_at.desc()).offset(skip).limit(limit).all()

        logger.info(
            "job_list_completed",
            count=len(jobs),
        )

        return jobs

    # ===================================================================
    # Update Job Status
    # ===================================================================
    @staticmethod
    def update_job_status(
        db: Session,
        job_id: int,
        status: JobStatus,
        error_message: Optional[str] = None,
        error_traceback: Optional[str] = None,
    ) -> Optional[Job]:
        logger.info(
            "job_status_update_requested",
            job_id=job_id,
            new_status=status.value,
        )

        job = JobService.get_job(db, job_id)
        if not job:
            logger.error(
                "job_status_update_failed_not_found",
                job_id=job_id,
            )
            return None

        old_status = job.status
        job.status = status

        # Running
        if status == JobStatus.RUNNING:
            job.started_at = datetime.now(UTC)
            logger.info(
                "job_started",
                job_id=job_id,
                started_at=job.started_at.isoformat(),
            )

        # Finished statuses
        elif status in (JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.CANCELLED):
            job.completed_at = datetime.now(UTC)

            if job.started_at:
                job.duration_seconds = (
                    job.completed_at - job.started_at
                ).total_seconds()

                logger.info(
                    "job_completed",
                    job_id=job_id,
                    status=status.value,
                    duration_seconds=job.duration_seconds,
                )
            else:
                logger.warning(
                    "job_completed_without_start_time",
                    job_id=job_id,
                )

        # Error and traceback
        if error_message:
            job.error_message = error_message
            logger.error(
                "job_error_message_saved",
                job_id=job_id,
                error_message=error_message,
            )

        if error_traceback:
            job.error_traceback = error_traceback

        db.commit()
        db.refresh(job)

        logger.info(
            "job_status_updated",
            job_id=job_id,
            old_status=old_status.value,
            new_status=status.value,
        )

        # Publish update
        JobService._publish_job_update(job_id, JobSchema.model_validate(job))

        return job

    # ===================================================================
    # Log Message
    # ===================================================================
    @staticmethod
    def log_message(
        db: Session,
        job_id: int,
        level: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> JobLog:
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

        logger.debug(
            "job_log_added",
            job_id=job_id,
            level=level,
            message_preview=message[:120],
        )

        return log

    # ===================================================================
    # Get Job Logs
    # ===================================================================
    @staticmethod
    def get_job_logs(
        db: Session,
        job_id: int,
        level: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[JobLog]:
        logger.info(
            "job_logs_requested",
            job_id=job_id,
            level=level,
            limit=limit,
        )

        query = db.query(JobLog).filter(JobLog.job_id == job_id)

        if level:
            query = query.filter(JobLog.level == level)

        query = query.order_by(JobLog.timestamp.asc())

        if limit:
            query = query.limit(limit)

        logs = query.all()

        logger.debug(
            "job_logs_retrieved",
            job_id=job_id,
            count=len(logs),
        )

        return logs

    # ===================================================================
    # Update Job Metrics
    # ===================================================================
    @staticmethod
    def update_job_metrics(
        db: Session,
        job_id: int,
        records_extracted: Optional[int] = None,
        records_loaded: Optional[int] = None,
        records_failed: Optional[int] = None,
    ) -> Optional[Job]:
        logger.info(
            "job_metrics_update_requested",
            job_id=job_id,
        )

        job = JobService.get_job(db, job_id)
        if not job:
            logger.warning(
                "job_metrics_update_failed_not_found",
                job_id=job_id,
            )
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
            "job_metrics_updated",
            job_id=job_id,
            extracted=records_extracted,
            loaded=records_loaded,
            failed=records_failed,
        )

        return job

    # ===================================================================
    # Cancel Job
    # ===================================================================
    @staticmethod
    def cancel_job(db: Session, job_id: int) -> Optional[Job]:
        logger.info(
            "job_cancel_requested",
            job_id=job_id,
        )

        job = JobService.get_job(db, job_id)
        if not job:
            logger.warning(
                "job_cancel_failed_not_found",
                job_id=job_id,
            )
            return None

        if job.status not in (JobStatus.PENDING, JobStatus.RUNNING):
            logger.warning(
                "job_cancel_invalid_status",
                job_id=job_id,
                status=job.status.value,
            )
            return job

        logger.info(
            "job_cancelling",
            job_id=job_id,
        )

        return JobService.update_job_status(
            db,
            job_id,
            JobStatus.CANCELLED,
            error_message="Job cancelled by user",
        )