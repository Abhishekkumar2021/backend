"""Service for managing pipeline-related operations."""

from datetime import UTC, datetime
from typing import Optional, List

from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.database import Pipeline, PipelineStatus, PipelineVersion
from app.schemas.pipeline import PipelineCreate, PipelineUpdate
from app.core.logging import get_logger

logger = get_logger(__name__)


class PipelineService:
    """Service for managing pipeline CRUD and versioning operations."""

    # ===================================================================
    # Create Pipeline
    # ===================================================================
    @staticmethod
    def create_pipeline(db: Session, pipeline_in: PipelineCreate) -> Pipeline:
        logger.info(
            "pipeline_creation_requested",
            name=pipeline_in.name,
            source_connection_id=pipeline_in.source_connection_id,
            destination_connection_id=pipeline_in.destination_connection_id,
        )

        db_pipeline = Pipeline(
            name=pipeline_in.name,
            description=pipeline_in.description,
            source_connection_id=pipeline_in.source_connection_id,
            destination_connection_id=pipeline_in.destination_connection_id,
            source_config=pipeline_in.source_config,
            destination_config=pipeline_in.destination_config,
            transform_config=pipeline_in.transform_config,
            schedule_cron=pipeline_in.schedule_cron,
            schedule_enabled=pipeline_in.schedule_enabled,
            sync_mode=pipeline_in.sync_mode,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            status=PipelineStatus.DRAFT,
        )
        db.add(db_pipeline)
        db.commit()
        db.refresh(db_pipeline)

        logger.info(
            "pipeline_created",
            pipeline_id=db_pipeline.id,
            name=db_pipeline.name,
        )

        # Create initial version
        PipelineService.create_pipeline_version(
            db,
            db_pipeline.id,
            description="Initial pipeline creation",
        )

        return db_pipeline

    # ===================================================================
    # Get Pipeline
    # ===================================================================
    @staticmethod
    def get_pipeline(db: Session, pipeline_id: int) -> Optional[Pipeline]:
        pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()

        if not pipeline:
            logger.warning(
                "pipeline_not_found",
                pipeline_id=pipeline_id,
            )

        return pipeline

    # ===================================================================
    # Get by Name
    # ===================================================================
    @staticmethod
    def get_pipeline_by_name(db: Session, name: str) -> Optional[Pipeline]:
        return db.query(Pipeline).filter(Pipeline.name == name).first()

    # ===================================================================
    # Get Pipelines (List)
    # ===================================================================
    @staticmethod
    def get_pipelines(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        status: Optional[PipelineStatus] = None,
    ) -> List[Pipeline]:
        logger.info(
            "pipeline_list_requested",
            skip=skip,
            limit=limit,
            status=status.value if status else None,
        )

        query = db.query(Pipeline)

        if status:
            query = query.filter(Pipeline.status == status)
            logger.debug(
                "pipeline_list_filter_status",
                status=status.value,
            )

        pipelines = query.offset(skip).limit(limit).all()

        logger.info(
            "pipeline_list_retrieved",
            count=len(pipelines),
        )

        return pipelines

    # ===================================================================
    # Update Pipeline
    # ===================================================================
    @staticmethod
    def update_pipeline(
        db: Session,
        pipeline_id: int,
        pipeline_in: PipelineUpdate,
    ) -> Optional[Pipeline]:
        db_pipeline = PipelineService.get_pipeline(db, pipeline_id)
        if not db_pipeline:
            logger.error(
                "pipeline_update_failed_not_found",
                pipeline_id=pipeline_id,
            )
            return None

        logger.info(
            "pipeline_update_requested",
            pipeline_id=pipeline_id,
            name=db_pipeline.name,
        )

        update_data = pipeline_in.model_dump(exclude_unset=True)

        # Fields that require versioning if changed
        config_fields = [
            "source_config",
            "destination_config",
            "transform_config",
            "schedule_cron",
            "sync_mode",
        ]

        version_needed = any(
            field in update_data and getattr(db_pipeline, field) != update_data[field]
            for field in config_fields
        )

        # Apply updates
        for field, value in update_data.items():
            setattr(db_pipeline, field, value)
            logger.debug(
                "pipeline_field_updated",
                field=field,
            )

        db_pipeline.updated_at = datetime.now(UTC)
        db.commit()
        db.refresh(db_pipeline)

        logger.info(
            "pipeline_updated",
            pipeline_id=pipeline_id,
        )

        # Create a version if config changed
        if version_needed:
            logger.info(
                "pipeline_version_required",
                pipeline_id=pipeline_id,
            )
            PipelineService.create_pipeline_version(
                db,
                pipeline_id,
                description="Pipeline configuration updated via API",
            )

        return db_pipeline

    # ===================================================================
    # Delete Pipeline
    # ===================================================================
    @staticmethod
    def delete_pipeline(db: Session, pipeline_id: int) -> Optional[Pipeline]:
        db_pipeline = PipelineService.get_pipeline(db, pipeline_id)
        if not db_pipeline:
            logger.error(
                "pipeline_delete_failed_not_found",
                pipeline_id=pipeline_id,
            )
            return None

        logger.warning(
            "pipeline_deletion_requested",
            pipeline_id=pipeline_id,
            name=db_pipeline.name,
        )

        db.delete(db_pipeline)
        db.commit()

        logger.info(
            "pipeline_deleted",
            pipeline_id=pipeline_id,
        )

        return db_pipeline

    # ===================================================================
    # Activate Pipeline
    # ===================================================================
    @staticmethod
    def activate_pipeline(db: Session, pipeline_id: int) -> Optional[Pipeline]:
        db_pipeline = PipelineService.get_pipeline(db, pipeline_id)
        if not db_pipeline:
            logger.warning(
                "pipeline_activate_not_found",
                pipeline_id=pipeline_id,
            )
            return None

        logger.info(
            "pipeline_activated",
            pipeline_id=pipeline_id,
        )

        db_pipeline.status = PipelineStatus.ACTIVE
        db_pipeline.updated_at = datetime.now(UTC)
        db.commit()
        db.refresh(db_pipeline)

        return db_pipeline

    # ===================================================================
    # Pause Pipeline
    # ===================================================================
    @staticmethod
    def pause_pipeline(db: Session, pipeline_id: int) -> Optional[Pipeline]:
        db_pipeline = PipelineService.get_pipeline(db, pipeline_id)
        if not db_pipeline:
            logger.warning(
                "pipeline_pause_not_found",
                pipeline_id=pipeline_id,
            )
            return None

        logger.info(
            "pipeline_paused",
            pipeline_id=pipeline_id,
        )

        db_pipeline.status = PipelineStatus.PAUSED
        db_pipeline.updated_at = datetime.now(UTC)
        db.commit()
        db.refresh(db_pipeline)

        return db_pipeline

    # ===================================================================
    # Version Helpers
    # ===================================================================
    @staticmethod
    def _get_next_version_number(db: Session, pipeline_id: int) -> int:
        max_version = (
            db.query(func.max(PipelineVersion.version_number))
            .filter(PipelineVersion.pipeline_id == pipeline_id)
            .scalar()
        )
        return (max_version or 0) + 1

    # ===================================================================
    # Create Pipeline Version
    # ===================================================================
    @staticmethod
    def create_pipeline_version(
        db: Session,
        pipeline_id: int,
        description: Optional[str] = None,
        created_by: Optional[str] = None,
    ) -> PipelineVersion:
        pipeline = PipelineService.get_pipeline(db, pipeline_id)
        if not pipeline:
            logger.error(
                "pipeline_version_creation_failed_not_found",
                pipeline_id=pipeline_id,
            )
            raise ValueError(f"Pipeline with ID {pipeline_id} not found")

        next_version = PipelineService._get_next_version_number(db, pipeline_id)

        logger.info(
            "pipeline_version_creation_requested",
            pipeline_id=pipeline_id,
            next_version=next_version,
        )

        config_snapshot = {
            "name": pipeline.name,
            "description": pipeline.description,
            "source_connection_id": pipeline.source_connection_id,
            "destination_connection_id": pipeline.destination_connection_id,
            "source_config": pipeline.source_config,
            "destination_config": pipeline.destination_config,
            "transform_config": pipeline.transform_config,
            "schedule_cron": pipeline.schedule_cron,
            "schedule_enabled": pipeline.schedule_enabled,
            "sync_mode": pipeline.sync_mode,
            "status": pipeline.status.value,
        }

        db_version = PipelineVersion(
            pipeline_id=pipeline_id,
            version_number=next_version,
            description=description,
            config_snapshot=config_snapshot,
            created_by=created_by,
            created_at=datetime.now(UTC),
        )
        db.add(db_version)
        db.commit()
        db.refresh(db_version)

        logger.info(
            "pipeline_version_created",
            version_id=db_version.id,
            pipeline_id=pipeline_id,
            version_number=next_version,
        )

        return db_version

    # ===================================================================
    # List Versions
    # ===================================================================
    @staticmethod
    def get_pipeline_versions(
        db: Session,
        pipeline_id: int,
        skip: int = 0,
        limit: int = 100,
    ) -> List[PipelineVersion]:
        versions = (
            db.query(PipelineVersion)
            .filter(PipelineVersion.pipeline_id == pipeline_id)
            .order_by(PipelineVersion.version_number.desc())
            .offset(skip)
            .limit(limit)
            .all()
        )

        logger.debug(
            "pipeline_versions_retrieved",
            pipeline_id=pipeline_id,
            count=len(versions),
        )

        return versions

    # ===================================================================
    # Get Version by ID
    # ===================================================================
    @staticmethod
    def get_pipeline_version(db: Session, version_id: int) -> Optional[PipelineVersion]:
        return (
            db.query(PipelineVersion)
            .filter(PipelineVersion.id == version_id)
            .first()
        )

    # ===================================================================
    # Get Version by Number
    # ===================================================================
    @staticmethod
    def get_pipeline_version_by_number(
        db: Session,
        pipeline_id: int,
        version_number: int,
    ) -> Optional[PipelineVersion]:
        return (
            db.query(PipelineVersion)
            .filter(
                PipelineVersion.pipeline_id == pipeline_id,
                PipelineVersion.version_number == version_number,
            )
            .first()
        )

    # ===================================================================
    # Restore Version
    # ===================================================================
    @staticmethod
    def restore_pipeline_version(
        db: Session,
        pipeline_id: int,
        version_number: int,
    ) -> Optional[Pipeline]:
        db_version = PipelineService.get_pipeline_version_by_number(
            db,
            pipeline_id,
            version_number,
        )

        if not db_version:
            logger.error(
                "pipeline_restore_failed_version_not_found",
                pipeline_id=pipeline_id,
                version_number=version_number,
            )
            return None

        pipeline = PipelineService.get_pipeline(db, pipeline_id)
        if not pipeline:
            logger.error(
                "pipeline_restore_failed_pipeline_not_found",
                pipeline_id=pipeline_id,
            )
            return None

        logger.info(
            "pipeline_restore_requested",
            pipeline_id=pipeline_id,
            version_number=version_number,
        )

        snapshot = db_version.config_snapshot

        # Restore snapshot fields
        pipeline.description = snapshot.get("description", pipeline.description)
        pipeline.source_connection_id = snapshot.get(
            "source_connection_id", pipeline.source_connection_id
        )
        pipeline.destination_connection_id = snapshot.get(
            "destination_connection_id", pipeline.destination_connection_id
        )
        pipeline.source_config = snapshot.get("source_config", pipeline.source_config)
        pipeline.destination_config = snapshot.get(
            "destination_config", pipeline.destination_config
        )
        pipeline.transform_config = snapshot.get(
            "transform_config", pipeline.transform_config
        )
        pipeline.schedule_cron = snapshot.get("schedule_cron", pipeline.schedule_cron)
        pipeline.schedule_enabled = snapshot.get(
            "schedule_enabled", pipeline.schedule_enabled
        )
        pipeline.sync_mode = snapshot.get("sync_mode", pipeline.sync_mode)
        pipeline.status = PipelineStatus(snapshot.get("status", pipeline.status.value))

        pipeline.updated_at = datetime.now(UTC)
        db.commit()
        db.refresh(pipeline)

        logger.info(
            "pipeline_restored",
            pipeline_id=pipeline_id,
            version_number=version_number,
        )

        # Version entry for restoration
        PipelineService.create_pipeline_version(
            db,
            pipeline_id,
            description=f"Restored to version {version_number}",
        )

        return pipeline
