"""Database-driven Celery Beat scheduler for dynamic pipeline scheduling."""

from datetime import datetime, UTC
from typing import Dict, Any, Optional

import pytz
from celery import current_app
from celery.beat import Scheduler, ScheduleEntry
from celery.schedules import crontab
from sqlalchemy.orm import Session

from app.core.database import SessionLocal
from app.core.logging import get_logger
from app.core.exceptions import ConfigurationError
from app.models.database import Pipeline
from app.models.enums import PipelineStatus

logger = get_logger(__name__)


class DatabaseScheduler(Scheduler):
    """Celery Beat scheduler with database-driven schedule management.
    
    Features:
    - Polls database every UPDATE_INTERVAL seconds
    - Dynamically adds/removes pipeline schedules
    - Updates next_run_at timestamps
    - Handles invalid cron expressions gracefully
    
    Example cron format: "0 2 * * *" (daily at 2 AM)
    """

    UPDATE_INTERVAL: int = 60  # Poll database every 60 seconds
    MAX_SCHEDULES: int = 1000  # Safety limit

    def __init__(self, *args, **kwargs):
        """Initialize database scheduler."""
        self.last_update: Optional[datetime] = None
        self._schedule_cache: Dict[str, ScheduleEntry] = {}
        
        super().__init__(*args, **kwargs)
        
        logger.info(
            "database_scheduler_initialized",
            update_interval=self.UPDATE_INTERVAL,
            max_schedules=self.MAX_SCHEDULES,
        )

    def setup_schedule(self):
        """Initial setup - install defaults and sync with database."""
        logger.info("scheduler_setup_started")
        self.install_default_entries(self.schedule)
        self.sync_with_database()
        logger.info("scheduler_setup_completed")

    def sync_with_database(self) -> None:
        """Load schedules from database and update Celery Beat.
        
        Queries all active pipelines with scheduling enabled and creates
        corresponding Celery Beat schedule entries.
        """
        logger.debug("scheduler_sync_started")
        
        db: Session = SessionLocal()
        
        try:
            # Query active scheduled pipelines
            pipelines = (
                db.query(Pipeline)
                .filter(
                    Pipeline.schedule_enabled.is_(True),
                    Pipeline.schedule_cron.isnot(None),
                    Pipeline.status == PipelineStatus.ACTIVE,
                )
                .all()
            )
            
            pipeline_count = len(pipelines)
            
            # Safety check
            if pipeline_count > self.MAX_SCHEDULES:
                logger.error(
                    "too_many_schedules",
                    count=pipeline_count,
                    max_allowed=self.MAX_SCHEDULES,
                )
                raise ConfigurationError(
                    f"Too many schedules: {pipeline_count} > {self.MAX_SCHEDULES}"
                )
            
            logger.info(
                "pipelines_found",
                count=pipeline_count,
            )
            
            new_entries: Dict[str, ScheduleEntry] = {}
            
            for pipeline in pipelines:
                entry = self._create_schedule_entry(db, pipeline)
                if entry:
                    new_entries[entry.name] = entry
            
            # Update Celery Beat schedule
            self.app.conf.beat_schedule = new_entries
            self._schedule_cache = new_entries
            self.last_update = datetime.now(UTC)
            
            logger.info(
                "scheduler_sync_completed",
                schedules_loaded=len(new_entries),
                duration_seconds=(datetime.now(UTC) - self.last_update).total_seconds()
                if self.last_update else 0,
            )
            
        except Exception as e:
            logger.error(
                "scheduler_sync_failed",
                error=str(e),
                error_type=e.__class__.__name__,
                exc_info=True,
            )
        finally:
            db.close()

    def _create_schedule_entry(
        self,
        db: Session,
        pipeline: Pipeline,
    ) -> Optional[ScheduleEntry]:
        """Create a schedule entry for a pipeline.
        
        Args:
            db: Database session
            pipeline: Pipeline object
            
        Returns:
            ScheduleEntry or None if invalid
        """
        try:
            # Parse cron expression
            schedule = self._parse_cron_schedule(pipeline.schedule_cron)
            
            if not schedule:
                logger.warning(
                    "invalid_cron_expression",
                    pipeline_id=pipeline.id,
                    pipeline_name=pipeline.name,
                    cron_expression=pipeline.schedule_cron,
                )
                return None
            
            # Create unique schedule name
            entry_name = f"pipeline_{pipeline.id}_{pipeline.name}"
            
            # Create schedule entry
            entry = ScheduleEntry(
                name=entry_name,
                task="app.worker.tasks.pipeline_tasks.execute_pipeline",
                schedule=schedule,
                args=(pipeline.id,),
                kwargs={},
                options={
                    "queue": "pipelines",
                    "routing_key": "pipeline.execute",
                },
            )
            
            # Update next_run_at in database
            self._update_next_run_at(db, pipeline, schedule)
            
            logger.debug(
                "schedule_entry_created",
                pipeline_id=pipeline.id,
                pipeline_name=pipeline.name,
                cron_expression=pipeline.schedule_cron,
                next_run_at=pipeline.next_run_at.isoformat() if pipeline.next_run_at else None,
            )
            
            return entry
            
        except Exception as e:
            logger.error(
                "schedule_entry_creation_failed",
                pipeline_id=pipeline.id,
                pipeline_name=pipeline.name,
                error=str(e),
                exc_info=True,
            )
            return None

    def _parse_cron_schedule(self, cron_string: str) -> Optional[crontab]:
        """Parse cron string into Celery crontab schedule.
        
        Args:
            cron_string: Cron expression (e.g., "0 2 * * *")
            
        Returns:
            crontab object or None if invalid
            
        Format: "minute hour day_of_month month_of_year day_of_week"
        """
        try:
            parts = cron_string.strip().split()
            
            if len(parts) != 5:
                logger.error(
                    "invalid_cron_format",
                    cron_string=cron_string,
                    parts_count=len(parts),
                    expected=5,
                )
                return None
            
            minute, hour, day_of_month, month_of_year, day_of_week = parts
            
            schedule = crontab(
                minute=minute,
                hour=hour,
                day_of_month=day_of_month,
                month_of_year=month_of_year,
                day_of_week=day_of_week
            )
            
            return schedule
            
        except Exception as e:
            logger.error(
                "cron_parsing_failed",
                cron_string=cron_string,
                error=str(e),
            )
            return None

    def _update_next_run_at(
        self,
        db: Session,
        pipeline: Pipeline,
        schedule: crontab,
    ) -> None:
        """Update pipeline.next_run_at using Celery 5.x schedule API."""
        try:
            now = current_app.now()  # Always timezone-aware UTC datetime
            
            # Celery 5.x+ way to compute next ETA
            eta = schedule.remaining_estimate(now)
            next_run = now + eta
            
            if pipeline.next_run_at != next_run:
                pipeline.next_run_at = next_run
                db.add(pipeline)
                db.commit()

                logger.debug(
                    "next_run_updated",
                    pipeline_id=pipeline.id,
                    next_run_at=next_run.isoformat(),
                )
        except Exception as e:
            logger.warning(
                "next_run_update_failed",
                pipeline_id=pipeline.id,
                error=str(e),
            )
            db.rollback()


    def tick(self, *args, **kwargs):
        """Called periodically by Celery Beat.
        
        Checks if UPDATE_INTERVAL has passed and re-syncs with database.
        """
        should_sync = (
            self.last_update is None
            or (datetime.now(UTC) - self.last_update).total_seconds() >= self.UPDATE_INTERVAL
        )
        
        if should_sync:
            logger.debug("scheduler_tick_sync_triggered")
            self.sync_with_database()
        
        return super().tick(*args, **kwargs)

    @property
    def schedule(self):
        """Return current schedule.
        
        Returns:
            Dictionary of schedule entries
        """
        return self._schedule_cache