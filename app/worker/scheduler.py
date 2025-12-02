# app/worker/scheduler.py
"""
Database-driven Celery Beat scheduler (full modern rewrite)

Responsibilities:
- Load scheduled pipelines from DB
- Create a corresponding Job (so execute_pipeline gets a job_id)
- Generate Celery ScheduleEntry objects (does NOT clobber other schedules)
- Update pipeline.next_run_at in DB (timezone-aware UTC)
- Use celery.schedules.crontab_parser for robust cron parsing
- Safe error handling and structured logging
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, Tuple

from celery import current_app
from celery.beat import Scheduler, ScheduleEntry
from celery.schedules import crontab, crontab_parser
from sqlalchemy.orm import Session

from app.core.database import SessionLocal
from app.core.logging import get_logger, bind_trace_ids
from app.core.exceptions import ConfigurationError
from app.models.database import Pipeline
from app.models.enums import PipelineStatus

logger = get_logger(__name__)


# Marker used to identify schedule entries that this scheduler manages.
# We will prefix all created schedule names with this so we can merge/update
# without touching unrelated entries in the app.conf.beat_schedule
_MANAGED_PREFIX = "db_pipeline_schedule:"


class DatabaseScheduler(Scheduler):
    """
    Database-driven Celery Beat scheduler.

    This scheduler:
    - periodically (UPDATE_INTERVAL) syncs DB pipelines into Celery beat schedules
    - creates a Job row (attempt) for scheduled executions and passes job_id
    - updates pipeline.next_run_at using Celery schedule API
    - merges schedules with existing beat schedule (doesn't clobber)
    """

    UPDATE_INTERVAL: int = 60  # seconds
    MAX_SCHEDULES: int = 1000  # safety limit to avoid huge schedule maps

    def __init__(self, *args, **kwargs):
        self._last_sync: Optional[datetime] = None
        # maps schedule_name -> ScheduleEntry for schedules created from DB
        self._managed_schedules: Dict[str, ScheduleEntry] = {}
        super().__init__(*args, **kwargs)
        logger.info(
            "database_scheduler_initialized",
            update_interval=self.UPDATE_INTERVAL,
            max_schedules=self.MAX_SCHEDULES,
        )

    # -------------------------
    # lifecycle
    # -------------------------
    def setup_schedule(self) -> None:
        """
        Called once on scheduler startup. Install defaults and perform initial sync.
        """
        logger.info("scheduler_setup_started")
        # keep built-in/default configured schedules intact
        self.install_default_entries(self.schedule)
        # do initial DB sync
        try:
            self.sync_with_database()
        except Exception:
            logger.exception("initial_scheduler_sync_failed")
        logger.info("scheduler_setup_completed")

    # -------------------------
    # main sync loop
    # -------------------------
    def sync_with_database(self) -> None:
        """
        Query database for active scheduled pipelines and update the beat schedule.
        - Creates an internal mapping of managed schedules (_managed_schedules)
        - Merges managed schedules into the app.conf.beat_schedule dict (without overwriting non-managed entries)
        - Removes stale managed schedules that no longer exist in DB
        """
        logger.debug("scheduler_sync_started")
        db: Session = SessionLocal()
        sync_start = datetime.now(timezone.utc)
        try:
            # fetch pipelines that are scheduled and active
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
            logger.info("pipelines_for_scheduling_found", count=pipeline_count)

            if pipeline_count > self.MAX_SCHEDULES:
                logger.error(
                    "too_many_scheduled_pipelines",
                    found=pipeline_count,
                    max_allowed=self.MAX_SCHEDULES,
                )
                raise ConfigurationError(f"Too many schedules ({pipeline_count}) > {self.MAX_SCHEDULES}")

            new_managed: Dict[str, ScheduleEntry] = {}

            for pipeline in pipelines:
                try:
                    entry = self._build_entry_for_pipeline(db, pipeline)
                    if entry:
                        new_managed[entry.name] = entry
                except Exception as e:
                    logger.exception("failed_building_schedule_for_pipeline", pipeline_id=pipeline.id)

            # Merge new_managed into global beat schedule carefully.
            # We will keep non-managed entries intact and replace only our managed entries.
            merged_schedule: Dict[str, Any] = {}

            # Start from existing config (could come from file/other schedulers)
            existing = getattr(self.app.conf, "beat_schedule", {}) or {}
            # copy non-managed entries
            for name, val in existing.items():
                if not name.startswith(_MANAGED_PREFIX):
                    merged_schedule[name] = val

            # add managed entries
            for name, entry in new_managed.items():
                # convert ScheduleEntry to serializable mapping that celery expects in conf
                merged_schedule[name] = self._entry_to_conf(entry)

            # set beat_schedule in app.conf
            # this won't necessarily persist between restarts, but Celery Beat reads conf at runtime
            self.app.conf.beat_schedule = merged_schedule

            # replace our in-memory cache
            self._managed_schedules = new_managed
            self._last_sync = datetime.now(timezone.utc)

            duration = (datetime.now(timezone.utc) - sync_start).total_seconds()
            logger.info(
                "scheduler_sync_completed",
                managed_count=len(new_managed),
                duration_seconds=duration,
            )

        except Exception as e:
            logger.exception("scheduler_sync_failed")
        finally:
            db.close()

    # -------------------------
    # build schedule entry
    # -------------------------
    def _build_entry_for_pipeline(self, db: Session, pipeline: Pipeline) -> Optional[ScheduleEntry]:
        """
        Build a ScheduleEntry for a pipeline.
        Also ensures a scheduled Job exists (so the worker's execute_pipeline(job_id) will succeed).
        """
        # parse cron
        schedule_obj = self._parse_cron(pipeline.schedule_cron)
        if not schedule_obj:
            logger.warning(
                "invalid_cron_for_pipeline",
                pipeline_id=pipeline.id,
                cron_expr=pipeline.schedule_cron,
            )
            return None

        # ensure a Job exists for the scheduled run and get job_id
        job_id = self._ensure_scheduled_job(db, pipeline)

        # create a safe unique name for beat schedule (managed by us)
        entry_name = f"{_MANAGED_PREFIX}{pipeline.id}:{pipeline.name}"

        # Build the ScheduleEntry. Celery will handle next_run_at calculation for it.
        entry = ScheduleEntry(
            name=entry_name,
            task="app.worker.tasks.pipeline_tasks.execute_pipeline",
            schedule=schedule_obj,
            args=(job_id,),
            kwargs={},
            options={"queue": "pipelines", "routing_key": "pipeline.execute"},
        )

        # update next_run_at in DB using schedule API
        try:
            self._update_next_run_at(db, pipeline, schedule_obj)
        except Exception:
            logger.exception("failed_to_update_next_run_at", pipeline_id=pipeline.id)

        logger.debug(
            "schedule_entry_built",
            pipeline_id=pipeline.id,
            entry_name=entry_name,
            job_id=job_id,
        )

        return entry

    # -------------------------
    # cron parsing
    # -------------------------
    def _parse_cron(self, cron_expr: str) -> Optional[crontab]:
        """
        Parse a cron expression (5 fields) using celery's crontab_parser.
        Accepts standard 5-field cron: minute hour day month day_of_week
        """
        try:
            # Validate and parse using parser (raises if invalid)
            # The parser returns an object we can pass to crontab constructor too.
            crontab_parser(cron_expr)  # will raise if invalid

            # The crontab constructor accepts the same five fields
            parts = cron_expr.strip().split()
            if len(parts) != 5:
                logger.error("cron_expr_wrong_field_count", expr=cron_expr, parts=len(parts))
                return None

            minute, hour, day_of_month, month_of_year, day_of_week = parts
            schedule = crontab(
                minute=minute,
                hour=hour,
                day_of_month=day_of_month,
                month_of_year=month_of_year,
                day_of_week=day_of_week,
            )
            return schedule

        except Exception as e:
            logger.warning("cron_parse_failed", cron=cron_expr, error=str(e))
            return None

    # -------------------------
    # next run calculation
    # -------------------------
    def _update_next_run_at(self, db: Session, pipeline: Pipeline, schedule: crontab) -> None:
        """
        Compute and store the next_run_at using schedule.remaining_estimate(now).
        Commits only if next_run_at changed.
        """
        try:
            now = current_app.now()  # timezone-aware UTC
            # remaining_estimate returns a timedelta until next run
            remaining = schedule.remaining_estimate(now)
            next_run = now + remaining

            # Normalize types (DB column might accept naive or aware datetimes;
            # store timezone-aware UTC which is recommended)
            next_run_utc = next_run.astimezone(timezone.utc)

            if pipeline.next_run_at != next_run_utc:
                pipeline.next_run_at = next_run_utc
                db.add(pipeline)
                db.commit()
                logger.debug(
                    "pipeline_next_run_updated",
                    pipeline_id=pipeline.id,
                    next_run_at=next_run_utc.isoformat(),
                )
        except Exception as e:
            logger.exception("next_run_update_failed", pipeline_id=pipeline.id)

    # -------------------------
    # job creation helper
    # -------------------------
    def _ensure_scheduled_job(self, db: Session, pipeline: Pipeline) -> int:
        """
        Ensure that a Job exists for the scheduled execution and return job_id.

        This tries to use JobService.create_scheduled_job(db, pipeline_id, schedule_name)
        if available; if not available, it attempts to insert a Job model directly.
        The function is defensive — if both fail, it will log and return pipeline.id as a fallback.
        """
        try:
            # Lazy import to avoid circular dependency on module import time
            from app.services.job import JobService

            try:
                job = JobService.create_scheduled_job(db, pipeline.id)
                logger.debug("scheduled_job_created_via_service", pipeline_id=pipeline.id, job_id=job.id)
                return job.id
            except AttributeError:
                # Service doesn't provide create_scheduled_job; fallback to generic create
                logger.debug("JobService missing create_scheduled_job; falling back to direct insertion")
        except Exception:
            logger.debug("JobService not available or import failed; falling back to direct SQL insert")

        # Fallback: try to insert into Job model directly if it exists
        try:
            from app.models.database import Job  # type: ignore

            job = Job(
                pipeline_id=pipeline.id,
                status="scheduled",
                created_at=current_app.now(),
                scheduled=True,
            )
            db.add(job)
            db.commit()
            logger.debug("scheduled_job_created_direct", pipeline_id=pipeline.id, job_id=job.id)
            return job.id
        except Exception as e:
            logger.warning(
                "failed_to_create_scheduled_job",
                pipeline_id=pipeline.id,
                error=str(e),
            )

        # As a last resort, return pipeline.id (execute_pipeline will likely fail if it expects a job)
        logger.warning("falling_back_to_pipeline_id_as_job_id", pipeline_id=pipeline.id)
        return pipeline.id

    # -------------------------
    # util: convert ScheduleEntry to beat_schedule conf dict
    # -------------------------
    def _entry_to_conf(self, entry: ScheduleEntry) -> Dict[str, Any]:
        """
        Convert ScheduleEntry to the configuration mapping that Celery expects in app.conf.beat_schedule.
        We include 'task', 'schedule', 'args', 'kwargs', and 'options'.
        """
        return {
            "task": entry.task,
            "schedule": entry.schedule,
            "args": entry.args,
            "kwargs": entry.kwargs,
            "options": entry.options,
        }

    # -------------------------
    # tick override
    # -------------------------
    def tick(self, *args, **kwargs):
        """
        Called frequently by Celery beat. We only resync from DB after UPDATE_INTERVAL elapsed.
        """
        try:
            now = datetime.now(timezone.utc)
            should_sync = (
                self._last_sync is None
                or (now - self._last_sync).total_seconds() >= self.UPDATE_INTERVAL
            )
            if should_sync:
                logger.debug("scheduler_tick_triggering_sync")
                self.sync_with_database()
        except Exception:
            logger.exception("scheduler_tick_exception")
        return super().tick(*args, **kwargs)

    # -------------------------
    # schedule property (compat)
    # -------------------------
    @property
    def schedule(self) -> Dict[str, Any]:
        """
        Return the current Celery beat schedule mapping.
        We return the latest app.conf.beat_schedule (which includes merged entries).
        """
        return getattr(self.app.conf, "beat_schedule", {}) or {}
