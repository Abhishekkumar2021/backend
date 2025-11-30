"""Custom Celery Beat scheduler that reads schedules from database.

Enables dynamic pipeline scheduling without restart - schedules are loaded
from the Pipeline table and updated every 60 seconds.
"""

from datetime import datetime, UTC
from typing import Dict, Any

import pytz
from celery import current_app
from celery.beat import Scheduler, ScheduleEntry
from celery.schedules import crontab
from sqlalchemy.orm import Session

from app.core.database import SessionLocal
from app.models.db_models import Pipeline, PipelineStatus
from app.utils.logging import get_logger

logger = get_logger(__name__)


class DatabaseScheduler(Scheduler):
    """Celery Beat scheduler that loads pipeline schedules from database.
    
    Polls the database every UPDATE_INTERVAL seconds to detect new/updated
    pipeline schedules and dynamically adds them to Celery Beat.
    """

    # How often to poll database for schedule changes (seconds)
    UPDATE_INTERVAL = 60

    def __init__(self, *args, **kwargs):
        """Initialize the database scheduler."""
        super().__init__(*args, **kwargs)
        self.last_update = None
        logger.info("DatabaseScheduler initialized")

    def setup_schedule(self):
        """Initial setup - install defaults and sync with database."""
        logger.info("Setting up scheduler")
        self.install_default_entries()
        self.sync_with_database()

    def sync_with_database(self):
        """Load schedules from database and update Celery Beat schedule.
        
        Queries all active pipelines with scheduling enabled and creates
        corresponding Celery Beat schedule entries.
        """
        logger.debug("Syncing schedules with database")
        db: Session = SessionLocal()
        
        try:
            new_entries: Dict[str, ScheduleEntry] = {}

            # Query active pipelines with schedules enabled
            pipelines = (
                db.query(Pipeline)
                .filter(
                    Pipeline.schedule_enabled.is_(True),
                    Pipeline.schedule_cron.isnot(None),
                    Pipeline.status == PipelineStatus.ACTIVE,
                )
                .all()
            )

            logger.info("Found %d scheduled pipeline(s)", len(pipelines))

            for pipeline in pipelines:
                try:
                    # Parse cron expression
                    schedule = self._parse_cron_schedule(pipeline.schedule_cron)
                    
                    if not schedule:
                        logger.warning(
                            "Invalid cron expression for pipeline %d: %s",
                            pipeline.id,
                            pipeline.schedule_cron,
                        )
                        continue

                    # Create unique schedule entry name
                    name = f"pipeline_{pipeline.id}_{pipeline.name}_schedule"

                    # Create schedule entry
                    entry = ScheduleEntry(
                        name=name,
                        task="app.worker.run_pipeline",
                        schedule=schedule,
                        args=(pipeline.id,),  # Pass pipeline_id to task
                        kwargs={},
                        options={
                            "queue": "default",
                            "exchange": "default",
                            "routing_key": "default",
                        },
                    )
                    
                    new_entries[name] = entry
                    logger.debug("Added schedule: %s (cron: %s)", name, pipeline.schedule_cron)

                    # Update next_run_at in database
                    self._update_next_run(db, pipeline, schedule)

                except Exception as e:
                    logger.error(
                        "Error creating schedule for pipeline %d: %s",
                        pipeline.id,
                        e,
                        exc_info=True,
                    )

            # Replace current schedule with new entries
            self.app.conf.beat_schedule = new_entries
            self.last_update = datetime.now(UTC)
            
            logger.info("Schedule sync complete: %d schedule(s) loaded", len(new_entries))

        except Exception as e:
            logger.exception("Error syncing schedules with database")
        finally:
            db.close()

    def _parse_cron_schedule(self, cron_string: str) -> Any:
        """Parse cron string into Celery crontab schedule.
        
        Args:
            cron_string: Standard cron format "minute hour day_of_week day_of_month month_of_year"
            
        Returns:
            crontab schedule object or None if invalid
        """
        try:
            cron_parts = cron_string.split()
            
            if len(cron_parts) != 5:
                logger.error("Invalid cron format (expected 5 parts): %s", cron_string)
                return None

            minute, hour, day_of_month, month_of_year, day_of_week = cron_parts

            schedule = crontab(
                minute=minute,
                hour=hour,
                day_of_week=day_of_week,
                day_of_month=day_of_month,
                month_of_year=month_of_year,
                tz=pytz.utc,  # All schedules in UTC
            )
            
            return schedule

        except Exception as e:
            logger.error("Error parsing cron string '%s': %s", cron_string, e)
            return None

    def _update_next_run(self, db: Session, pipeline: Pipeline, schedule: crontab) -> None:
        """Update pipeline.next_run_at based on schedule.
        
        Args:
            db: Database session
            pipeline: Pipeline object
            schedule: Crontab schedule
        """
        try:
            current_app.set_default_tz("UTC")
            next_run = schedule.next(current_app.now())
            
            if pipeline.next_run_at != next_run:
                pipeline.next_run_at = next_run
                db.add(pipeline)
                db.commit()
                logger.debug(
                    "Updated next_run_at for pipeline %d: %s",
                    pipeline.id,
                    next_run,
                )

        except Exception as e:
            logger.warning("Failed to update next_run_at for pipeline %d: %s", pipeline.id, e)
            db.rollback()

    def tick(self, *args, **kwargs):
        """Called periodically by Celery Beat.
        
        Checks if UPDATE_INTERVAL has passed and re-syncs with database
        to pick up new/updated schedules.
        """
        if self.last_update is None or (
            datetime.now(UTC) - self.last_update
        ).total_seconds() >= self.UPDATE_INTERVAL:
            logger.debug("Update interval elapsed, syncing schedules")
            self.sync_with_database()
        
        return super().tick(*args, **kwargs)

    @property
    def schedule(self):
        """Return current schedule (syncs with DB first)."""
        self.sync_with_database()
        return self.app.conf.beat_schedule