"""
Improved JobService — Safe DB Writes, Clean Events, Better Logging
------------------------------------------------------------------

Upgrades:
✓ Atomic DB writes with rollback protection
✓ Cleaner job querying + filters
✓ Strong Redis Pub/Sub publishing
✓ Consistent event metadata
✓ Structured logging
✓ Single source of truth for JobSchema conversion
"""

import json
from datetime import datetime, UTC
from typing import Optional, List, Dict, Any, Callable

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.models.database import Job, JobLog, JobStatus
from app.schemas.job import Job as JobSchema
from app.schemas.job import JobCreate
from app.services.cache import get_cache
from app.core.logging import get_logger

logger = get_logger(__name__)


# =====================================================================
# INTERNAL HELPERS
# =====================================================================
def _safe_commit(db: Session, context: str) -> bool:
    """Safely commit with rollback protection."""
    try:
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(
            "db_commit_failed",
            context=context,
            error=str(e),
            exc_info=True,
        )
        return False


def _job_to_schema(job: Job) -> JobSchema:
    """Standardized conversion."""
    return JobSchema.model_validate(job)


class JobService:
    """Service for managing job-related operations."""

    # ===================================================================
    # REDIS PUBLISHING (Realtime Updates)
    # ===================================================================
    @staticmethod
    def _publish_job_update_safe(job_id: int, job_obj: Job) -> None:
        """
        Publishes job updates to Redis Pub/Sub.
        Failures do not affect job execution.
        """
        try:
            cache = get_cache()
            if not cache.is_available():
                return

            redis_client = cache.redis_client

            channel = f"job_updates:{job_id}"

            # Use schema for fully validated payload
            payload = _job_to_schema(job_obj).model_dump(mode="json")

            redis_client.publish(channel, json.dumps(payload))

            logger.debug(
                "job_update_published",
                job_id=job_id,
                channel=channel,
                status=payload.get("status"),
            )

        except Exception as e:
            logger.warning(
                "job_update_publish_failed",
                job_id=job_id,
                error=str(e),
                exc_info=True,
            )

    # ===================================================================
    # CREATE JOB
    # ===================================================================
    @staticmethod
    def create_job(db: Session, job_in: JobCreate) -> Job:
        logger.info(
            "job_creation_requested",
            pipeline_id=job_in.pipeline_id,
            status=job_in.status.value,
        )

        job = Job(
            pipeline_id=job_in.pipeline_id,
            status=job_in.status,
            created_at=datetime.now(UTC),
        )

        db.add(job)
        if not _safe_commit(db, "create_job"):
            raise RuntimeError("Failed to create job")

        logger.info(
            "job_created",
            job_id=job.id,
            pipeline_id=job.pipeline_id,
            status=job.status.value,
        )

        # Realtime update
        JobService._publish_job_update_safe(job.id, job)
        return job

    # ===================================================================
    # GET JOB
    # ===================================================================
    @staticmethod
    def get_job(db: Session, job_id: int) -> Optional[Job]:
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            logger.warning("job_not_found", job_id=job_id)
        return job

    # ===================================================================
    # LIST JOBS
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
        if status:
            query = query.filter(Job.status == status)

        jobs = (
            query
            .order_by(Job.created_at.desc())
            .offset(skip)
            .limit(limit)
            .all()
        )

        logger.info("job_list_completed", count=len(jobs))
        return jobs

    # ===================================================================
    # UPDATE JOB STATUS
    # ===================================================================
    @staticmethod
    def update_job_status(
        db: Session,
        job_id: int,
        status: JobStatus,
        error_message: Optional[str] = None,
        error_traceback: Optional[str] = None,
    ) -> Optional[Job]:
        logger.info("job_status_update_requested", job_id=job_id, new_status=status.value)

        job = JobService.get_job(db, job_id)
        if not job:
            return None

        previous_status = job.status
        job.status = status

        # START event
        if status == JobStatus.RUNNING:
            job.started_at = datetime.now(UTC)
            logger.info("job_started", job_id=job_id, ts=job.started_at.isoformat())

        # FINISH events
        if status in (JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.CANCELLED):
            job.completed_at = datetime.now(UTC)

            # Compute duration only if start time exists
            if job.started_at:
                job.duration_seconds = (
                    job.completed_at - job.started_at
                ).total_seconds()

        # Errors
        if error_message:
            job.error_message = error_message

        if error_traceback:
            job.error_traceback = error_traceback

        if not _safe_commit(db, "update_job_status"):
            return None

        logger.info(
            "job_status_updated",
            job_id=job_id,
            old_status=previous_status.value,
            new_status=status.value,
        )

        # Realtime monitoring
        JobService._publish_job_update_safe(job_id, job)

        return job

    # ===================================================================
    # LOG MESSAGE
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
            level=level.upper(),
            message=message,
            log_metadata=metadata,
            timestamp=datetime.now(UTC),
        )

        db.add(log)
        if not _safe_commit(db, "log_message"):
            raise RuntimeError("Failed to write job log")

        logger.debug(
            "job_log_added",
            job_id=job_id,
            level=level,
            message_preview=message[:150],
        )

        return log

    # ===================================================================
    # GET JOB LOGS
    # ===================================================================
    @staticmethod
    def get_job_logs(
        db: Session,
        job_id: int,
        level: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[JobLog]:
        logger.info("job_logs_requested", job_id=job_id, level=level, limit=limit)

        query = db.query(JobLog).filter(JobLog.job_id == job_id)

        if level:
            query = query.filter(JobLog.level == level.upper())

        query = query.order_by(JobLog.timestamp.asc())

        if limit:
            query = query.limit(limit)

        logs = query.all()

        logger.debug("job_logs_retrieved", job_id=job_id, count=len(logs))
        return logs

    # ===================================================================
    # UPDATE METRICS
    # ===================================================================
    @staticmethod
    def update_job_metrics(
        db: Session,
        job_id: int,
        records_extracted: Optional[int] = None,
        records_loaded: Optional[int] = None,
        records_failed: Optional[int] = None,
    ) -> Optional[Job]:
        logger.info("job_metrics_update_requested", job_id=job_id)

        job = JobService.get_job(db, job_id)
        if not job:
            return None

        if records_extracted is not None:
            job.records_extracted = records_extracted
        if records_loaded is not None:
            job.records_loaded = records_loaded
        if records_failed is not None:
            job.records_failed = records_failed

        if not _safe_commit(db, "update_job_metrics"):
            return None

        logger.debug(
            "job_metrics_updated",
            job_id=job_id,
            extracted=records_extracted,
            loaded=records_loaded,
            failed=records_failed,
        )

        return job

    # ===================================================================
    # CANCEL JOB
    # ===================================================================
    @staticmethod
    def cancel_job(db: Session, job_id: int) -> Optional[Job]:
        logger.info("job_cancel_requested", job_id=job_id)

        job = JobService.get_job(db, job_id)
        if not job:
            return None

        # Only certain states are cancellable
        if job.status not in (JobStatus.PENDING, JobStatus.RUNNING):
            logger.warning(
                "job_cancel_invalid_status",
                job_id=job_id,
                status=job.status.value,
            )
            return job

        return JobService.update_job_status(
            db,
            job_id,
            JobStatus.CANCELLED,
            error_message="Job cancelled by user",
        )
