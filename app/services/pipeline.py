"""
Improved PipelineService
-----------------------------------------
✓ Safe DB commits with rollback protection
✓ Standardized versioning + snapshots
✓ Cleaner CRUD logic
✓ Better logging metadata
✓ Reduced duplication
"""

from datetime import datetime, UTC
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.database import Pipeline, PipelineStatus, PipelineVersion
from app.schemas.pipeline import PipelineCreate, PipelineUpdate
from app.core.logging import get_logger

logger = get_logger(__name__)


# =====================================================================
# INTERNAL HELPERS
# =====================================================================
def _safe_commit(db: Session, context: str) -> bool:
    """Safely commit DB transaction, rollback on error."""
    try:
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        logger.error(
            "db_commit_failed",
            context=context,
            error=str(e),
            exc_info=True,
        )
        return False


def _pipeline_snapshot(p: Pipeline) -> Dict[str, Any]:
    """Consistent snapshot of pipeline config."""
    return {
        "name": p.name,
        "description": p.description,
        "source_connection_id": p.source_connection_id,
        "destination_connection_id": p.destination_connection_id,
        "source_config": p.source_config,
        "destination_config": p.destination_config,
        "transform_config": p.transform_config,
        "schedule_cron": p.schedule_cron,
        "schedule_enabled": p.schedule_enabled,
        "sync_mode": p.sync_mode,
        "status": p.status.value,
    }


# =====================================================================
# PIPELINE SERVICE
# =====================================================================
class PipelineService:
    """Service for managing pipeline CRUD + versioning."""

    # ------------------------------------------------------------------
    # CREATE PIPELINE
    # ------------------------------------------------------------------
    @staticmethod
    def create_pipeline(db: Session, pipeline_in: PipelineCreate) -> Pipeline:
        logger.info(
            "pipeline_creation_requested",
            name=pipeline_in.name,
            source=pipeline_in.source_connection_id,
            destination=pipeline_in.destination_connection_id,
        )

        pipeline = Pipeline(
            name=pipeline_in.name,
            description=pipeline_in.description,
            source_connection_id=pipeline_in.source_connection_id,
            destination_connection_id=pipeline_in.destination_connection_id,
            source_config=pipeline_in.source_config.model_dump(),
            destination_config=pipeline_in.destination_config.model_dump(),
            transform_config=(
                pipeline_in.transform_config.model_dump()
                if pipeline_in.transform_config else None
            ),
            schedule_cron=pipeline_in.schedule_cron,
            schedule_enabled=pipeline_in.schedule_enabled,
            sync_mode=pipeline_in.sync_mode,
            status=PipelineStatus.DRAFT,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )

        db.add(pipeline)
        if not _safe_commit(db, "create_pipeline"):
            raise RuntimeError("Failed to create pipeline")

        logger.info("pipeline_created", pipeline_id=pipeline.id)

        # Create initial version snapshot
        PipelineService.create_pipeline_version(
            db,
            pipeline.id,
            description="Initial pipeline creation",
        )

        return pipeline

    # ------------------------------------------------------------------
    # GET
    # ------------------------------------------------------------------
    @staticmethod
    def get_pipeline(db: Session, pipeline_id: int) -> Optional[Pipeline]:
        pipeline = db.query(Pipeline).filter_by(id=pipeline_id).first()
        if not pipeline:
            logger.warning("pipeline_not_found", pipeline_id=pipeline_id)
        return pipeline

    @staticmethod
    def get_pipeline_by_name(db: Session, name: str) -> Optional[Pipeline]:
        return db.query(Pipeline).filter_by(name=name).first()

    # ------------------------------------------------------------------
    # LIST
    # ------------------------------------------------------------------
    @staticmethod
    def get_pipelines(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        status: Optional[PipelineStatus] = None,
    ) -> List[Pipeline]:

        query = db.query(Pipeline)

        if status:
            query = query.filter(Pipeline.status == status)

        pipelines = (
            query.order_by(Pipeline.created_at.desc())
            .offset(skip)
            .limit(limit)
            .all()
        )

        logger.info("pipeline_list_retrieved", count=len(pipelines))
        return pipelines

    # ------------------------------------------------------------------
    # UPDATE PIPELINE
    # ------------------------------------------------------------------
    @staticmethod
    def update_pipeline(
        db: Session,
        pipeline_id: int,
        pipeline_in: PipelineUpdate,
    ) -> Optional[Pipeline]:

        pipeline = PipelineService.get_pipeline(db, pipeline_id)
        if not pipeline:
            return None

        logger.info("pipeline_update_requested", pipeline_id=pipeline_id)

        update_data = pipeline_in.model_dump(exclude_unset=True)

        # Config fields that cause versioning if modified
        version_fields = {
            "source_config",
            "destination_config",
            "transform_config",
            "schedule_cron",
            "sync_mode",
        }

        version_needed = False

        # Apply updates
        for field, value in update_data.items():
            if getattr(pipeline, field) != value:
                if field in version_fields:
                    version_needed = True

                setattr(pipeline, field, value)
                logger.debug("pipeline_field_updated", field=field)

        pipeline.updated_at = datetime.now(UTC)

        if not _safe_commit(db, "update_pipeline"):
            return None

        logger.info("pipeline_updated", pipeline_id=pipeline_id)

        # Create version snapshot if meaningful config changed
        if version_needed:
            PipelineService.create_pipeline_version(
                db,
                pipeline_id,
                description="Pipeline configuration updated",
            )

        return pipeline

    # ------------------------------------------------------------------
    # DELETE PIPELINE
    # ------------------------------------------------------------------
    @staticmethod
    def delete_pipeline(db: Session, pipeline_id: int) -> Optional[Pipeline]:
        pipeline = PipelineService.get_pipeline(db, pipeline_id)
        if not pipeline:
            return None

        logger.warning("pipeline_deletion_requested", pipeline_id=pipeline_id)

        db.delete(pipeline)
        if not _safe_commit(db, "delete_pipeline"):
            return None

        logger.info("pipeline_deleted", pipeline_id=pipeline_id)
        return pipeline

    # ------------------------------------------------------------------
    # ACTIVATE / PAUSE
    # ------------------------------------------------------------------
    @staticmethod
    def activate_pipeline(db: Session, pipeline_id: int) -> Optional[Pipeline]:
        pipeline = PipelineService.get_pipeline(db, pipeline_id)
        if not pipeline:
            return None

        pipeline.status = PipelineStatus.ACTIVE
        pipeline.updated_at = datetime.now(UTC)

        if not _safe_commit(db, "activate_pipeline"):
            return None

        logger.info("pipeline_activated", pipeline_id=pipeline_id)
        return pipeline

    @staticmethod
    def pause_pipeline(db: Session, pipeline_id: int) -> Optional[Pipeline]:
        pipeline = PipelineService.get_pipeline(db, pipeline_id)
        if not pipeline:
            return None

        pipeline.status = PipelineStatus.PAUSED
        pipeline.updated_at = datetime.now(UTC)

        if not _safe_commit(db, "pause_pipeline"):
            return None

        logger.info("pipeline_paused", pipeline_id=pipeline_id)
        return pipeline

    # ------------------------------------------------------------------
    # VERSIONING
    # ------------------------------------------------------------------
    @staticmethod
    def _next_version(db: Session, pipeline_id: int) -> int:
        max_version = (
            db.query(func.max(PipelineVersion.version_number))
            .filter(PipelineVersion.pipeline_id == pipeline_id)
            .scalar()
        )
        return (max_version or 0) + 1

    @staticmethod
    def create_pipeline_version(
        db: Session,
        pipeline_id: int,
        description: Optional[str] = None,
        created_by: Optional[str] = None,
    ) -> PipelineVersion:

        pipeline = PipelineService.get_pipeline(db, pipeline_id)
        if not pipeline:
            raise ValueError(f"Pipeline {pipeline_id} not found")

        version_number = PipelineService._next_version(db, pipeline_id)

        version = PipelineVersion(
            pipeline_id=pipeline_id,
            version_number=version_number,
            description=description,
            created_by=created_by,
            created_at=datetime.now(UTC),
            config_snapshot=_pipeline_snapshot(pipeline),
        )

        db.add(version)
        if not _safe_commit(db, "create_pipeline_version"):
            raise RuntimeError("Failed to create pipeline version")

        logger.info(
            "pipeline_version_created",
            pipeline_id=pipeline_id,
            version_number=version_number,
        )

        return version

    # ------------------------------------------------------------------
    # VERSION LIST / GET
    # ------------------------------------------------------------------
    @staticmethod
    def get_pipeline_versions(
        db: Session,
        pipeline_id: int,
        skip: int = 0,
        limit: int = 100,
    ) -> List[PipelineVersion]:

        versions = (
            db.query(PipelineVersion)
            .filter_by(pipeline_id=pipeline_id)
            .order_by(PipelineVersion.version_number.desc())
            .offset(skip)
            .limit(limit)
            .all()
        )

        logger.info("pipeline_versions_retrieved", count=len(versions))
        return versions

    @staticmethod
    def get_pipeline_version(
        db: Session,
        version_id: int,
    ) -> Optional[PipelineVersion]:
        return db.query(PipelineVersion).filter_by(id=version_id).first()

    @staticmethod
    def get_pipeline_version_by_number(
        db: Session,
        pipeline_id: int,
        version_number: int,
    ) -> Optional[PipelineVersion]:

        return (
            db.query(PipelineVersion)
            .filter_by(pipeline_id=pipeline_id, version_number=version_number)
            .first()
        )

    # ------------------------------------------------------------------
    # RESTORE VERSION
    # ------------------------------------------------------------------
    @staticmethod
    def restore_pipeline_version(
        db: Session,
        pipeline_id: int,
        version_number: int,
    ) -> Optional[Pipeline]:

        version = PipelineService.get_pipeline_version_by_number(
            db, pipeline_id, version_number
        )
        if not version:
            logger.error(
                "pipeline_restore_failed_version_not_found",
                pipeline_id=pipeline_id,
                version_number=version_number,
            )
            return None

        pipeline = PipelineService.get_pipeline(db, pipeline_id)
        if not pipeline:
            return None

        logger.info(
            "pipeline_restore_requested",
            pipeline_id=pipeline_id,
            version_number=version_number,
        )

        snapshot = version.config_snapshot

        # Restore snapshot
        for key, value in snapshot.items():
            if hasattr(pipeline, key):
                setattr(pipeline, key, value)

        pipeline.updated_at = datetime.now(UTC)

        if not _safe_commit(db, "restore_pipeline_version"):
            return None

        # Create a new version indicating restoration event
        PipelineService.create_pipeline_version(
            db,
            pipeline_id,
            description=f"Restored to version {version_number}",
        )

        logger.info("pipeline_restored", pipeline_id=pipeline_id)
        return pipeline
