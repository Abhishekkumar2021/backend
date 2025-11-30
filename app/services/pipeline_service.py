"""Service for managing pipeline-related operations."""

from datetime import UTC, datetime
from typing import Optional, List

from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.db_models import Pipeline, PipelineStatus, PipelineVersion
from app.schemas.pipeline import PipelineCreate, PipelineUpdate
from app.utils.logging import get_logger

logger = get_logger(__name__)


class PipelineService:
    """Service for managing pipeline CRUD and versioning operations."""

    @staticmethod
    def create_pipeline(db: Session, pipeline_in: PipelineCreate) -> Pipeline:
        """Create a new pipeline with initial version.
        
        Args:
            db: Database session
            pipeline_in: Pipeline creation schema
            
        Returns:
            Created Pipeline object
        """
        logger.info("Creating pipeline: name=%s", pipeline_in.name)
        
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
            status=PipelineStatus.DRAFT,  # Start as draft
        )
        db.add(db_pipeline)
        db.commit()
        db.refresh(db_pipeline)
        
        logger.info("Pipeline created: pipeline_id=%d, name=%s", db_pipeline.id, db_pipeline.name)
        
        # Create initial version
        PipelineService.create_pipeline_version(
            db,
            db_pipeline.id,
            "Initial pipeline creation"
        )
        
        return db_pipeline

    @staticmethod
    def get_pipeline(db: Session, pipeline_id: int) -> Optional[Pipeline]:
        """Get pipeline by ID.
        
        Args:
            db: Database session
            pipeline_id: Pipeline ID
            
        Returns:
            Pipeline object or None if not found
        """
        pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
        
        if not pipeline:
            logger.warning("Pipeline not found: pipeline_id=%d", pipeline_id)
        
        return pipeline

    @staticmethod
    def get_pipeline_by_name(db: Session, name: str) -> Optional[Pipeline]:
        """Get pipeline by name.
        
        Args:
            db: Database session
            name: Pipeline name
            
        Returns:
            Pipeline object or None if not found
        """
        return db.query(Pipeline).filter(Pipeline.name == name).first()

    @staticmethod
    def get_pipelines(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        status: Optional[PipelineStatus] = None,
    ) -> List[Pipeline]:
        """List pipelines with optional filtering.
        
        Args:
            db: Database session
            skip: Pagination offset
            limit: Max results to return
            status: Filter by status
            
        Returns:
            List of Pipeline objects
        """
        query = db.query(Pipeline)
        
        if status:
            query = query.filter(Pipeline.status == status)
            logger.debug("Filtering pipelines by status=%s", status.value)
        
        pipelines = query.offset(skip).limit(limit).all()
        
        logger.debug("Retrieved %d pipeline(s)", len(pipelines))
        
        return pipelines

    @staticmethod
    def update_pipeline(
        db: Session,
        pipeline_id: int,
        pipeline_in: PipelineUpdate,
    ) -> Optional[Pipeline]:
        """Update pipeline and create version if config changed.
        
        Args:
            db: Database session
            pipeline_id: Pipeline ID
            pipeline_in: Pipeline update schema
            
        Returns:
            Updated Pipeline object or None if not found
        """
        db_pipeline = PipelineService.get_pipeline(db, pipeline_id)
        if not db_pipeline:
            logger.error("Cannot update pipeline - not found: pipeline_id=%d", pipeline_id)
            return None

        logger.info("Updating pipeline: pipeline_id=%d, name=%s", pipeline_id, db_pipeline.name)
        
        update_data = pipeline_in.model_dump(exclude_unset=True)
        
        # Check if configuration fields changed (requires new version)
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
            logger.debug("Updated field: %s", field)

        db_pipeline.updated_at = datetime.now(UTC)
        db.add(db_pipeline)
        db.commit()
        db.refresh(db_pipeline)
        
        logger.info("Pipeline updated: pipeline_id=%d", pipeline_id)
        
        # Create new version if config changed
        if version_needed:
            logger.info("Configuration changed, creating new version for pipeline_id=%d", pipeline_id)
            PipelineService.create_pipeline_version(
                db,
                db_pipeline.id,
                "Pipeline configuration updated via API"
            )

        return db_pipeline

    @staticmethod
    def delete_pipeline(db: Session, pipeline_id: int) -> Optional[Pipeline]:
        """Delete pipeline and all associated versions.
        
        Args:
            db: Database session
            pipeline_id: Pipeline ID
            
        Returns:
            Deleted Pipeline object or None if not found
        """
        db_pipeline = PipelineService.get_pipeline(db, pipeline_id)
        if not db_pipeline:
            logger.error("Cannot delete pipeline - not found: pipeline_id=%d", pipeline_id)
            return None

        logger.warning("Deleting pipeline: pipeline_id=%d, name=%s", pipeline_id, db_pipeline.name)
        
        db.delete(db_pipeline)
        db.commit()
        
        logger.info("Pipeline deleted: pipeline_id=%d", pipeline_id)
        
        return db_pipeline

    @staticmethod
    def activate_pipeline(db: Session, pipeline_id: int) -> Optional[Pipeline]:
        """Activate a pipeline (enable for scheduling).
        
        Args:
            db: Database session
            pipeline_id: Pipeline ID
            
        Returns:
            Updated Pipeline object or None if not found
        """
        db_pipeline = PipelineService.get_pipeline(db, pipeline_id)
        if not db_pipeline:
            return None

        logger.info("Activating pipeline: pipeline_id=%d", pipeline_id)
        
        db_pipeline.status = PipelineStatus.ACTIVE
        db_pipeline.updated_at = datetime.now(UTC)
        db.commit()
        db.refresh(db_pipeline)
        
        return db_pipeline

    @staticmethod
    def pause_pipeline(db: Session, pipeline_id: int) -> Optional[Pipeline]:
        """Pause a pipeline (disable scheduling).
        
        Args:
            db: Database session
            pipeline_id: Pipeline ID
            
        Returns:
            Updated Pipeline object or None if not found
        """
        db_pipeline = PipelineService.get_pipeline(db, pipeline_id)
        if not db_pipeline:
            return None

        logger.info("Pausing pipeline: pipeline_id=%d", pipeline_id)
        
        db_pipeline.status = PipelineStatus.PAUSED
        db_pipeline.updated_at = datetime.now(UTC)
        db.commit()
        db.refresh(db_pipeline)
        
        return db_pipeline

    # ========== Version Management ==========

    @staticmethod
    def _get_next_version_number(db: Session, pipeline_id: int) -> int:
        """Get next version number for a pipeline.
        
        Args:
            db: Database session
            pipeline_id: Pipeline ID
            
        Returns:
            Next version number (1-based)
        """
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
        """Create a version snapshot of the pipeline.
        
        Args:
            db: Database session
            pipeline_id: Pipeline ID
            description: Version description
            created_by: User who created the version
            
        Returns:
            Created PipelineVersion object
            
        Raises:
            ValueError: If pipeline not found
        """
        pipeline = PipelineService.get_pipeline(db, pipeline_id)
        if not pipeline:
            raise ValueError(f"Pipeline with ID {pipeline_id} not found")

        next_version_number = PipelineService._get_next_version_number(db, pipeline_id)
        
        logger.info(
            "Creating pipeline version: pipeline_id=%d, version=%d",
            pipeline_id,
            next_version_number,
        )

        # Create configuration snapshot
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
            version_number=next_version_number,
            description=description,
            config_snapshot=config_snapshot,
            created_by=created_by,
            created_at=datetime.now(UTC),
        )
        db.add(db_version)
        db.commit()
        db.refresh(db_version)
        
        logger.info(
            "Pipeline version created: version_id=%d, pipeline_id=%d, version=%d",
            db_version.id,
            pipeline_id,
            next_version_number,
        )
        
        return db_version

    @staticmethod
    def get_pipeline_versions(
        db: Session,
        pipeline_id: int,
        skip: int = 0,
        limit: int = 100,
    ) -> List[PipelineVersion]:
        """Get all versions for a pipeline.
        
        Args:
            db: Database session
            pipeline_id: Pipeline ID
            skip: Pagination offset
            limit: Max results to return
            
        Returns:
            List of PipelineVersion objects
        """
        versions = (
            db.query(PipelineVersion)
            .filter(PipelineVersion.pipeline_id == pipeline_id)
            .order_by(PipelineVersion.version_number.desc())
            .offset(skip)
            .limit(limit)
            .all()
        )
        
        logger.debug("Retrieved %d version(s) for pipeline_id=%d", len(versions), pipeline_id)
        
        return versions

    @staticmethod
    def get_pipeline_version(db: Session, version_id: int) -> Optional[PipelineVersion]:
        """Get a specific pipeline version by ID.
        
        Args:
            db: Database session
            version_id: Version ID
            
        Returns:
            PipelineVersion object or None if not found
        """
        return db.query(PipelineVersion).filter(PipelineVersion.id == version_id).first()

    @staticmethod
    def get_pipeline_version_by_number(
        db: Session,
        pipeline_id: int,
        version_number: int,
    ) -> Optional[PipelineVersion]:
        """Get a pipeline version by pipeline ID and version number.
        
        Args:
            db: Database session
            pipeline_id: Pipeline ID
            version_number: Version number
            
        Returns:
            PipelineVersion object or None if not found
        """
        return (
            db.query(PipelineVersion)
            .filter(
                PipelineVersion.pipeline_id == pipeline_id,
                PipelineVersion.version_number == version_number,
            )
            .first()
        )

    @staticmethod
    def restore_pipeline_version(
        db: Session,
        pipeline_id: int,
        version_number: int,
    ) -> Optional[Pipeline]:
        """Restore a pipeline to a previous version.
        
        Args:
            db: Database session
            pipeline_id: Pipeline ID
            version_number: Version number to restore
            
        Returns:
            Updated Pipeline object or None if version not found
        """
        db_version = PipelineService.get_pipeline_version_by_number(
            db, pipeline_id, version_number
        )
        if not db_version:
            logger.error(
                "Cannot restore - version not found: pipeline_id=%d, version=%d",
                pipeline_id,
                version_number,
            )
            return None

        pipeline = PipelineService.get_pipeline(db, pipeline_id)
        if not pipeline:
            logger.error("Cannot restore - pipeline not found: pipeline_id=%d", pipeline_id)
            return None

        logger.info(
            "Restoring pipeline to version: pipeline_id=%d, version=%d",
            pipeline_id,
            version_number,
        )

        # Apply snapshot configuration
        snapshot = db_version.config_snapshot
        
        pipeline.description = snapshot.get("description", pipeline.description)
        pipeline.source_connection_id = snapshot.get("source_connection_id", pipeline.source_connection_id)
        pipeline.destination_connection_id = snapshot.get("destination_connection_id", pipeline.destination_connection_id)
        pipeline.source_config = snapshot.get("source_config", pipeline.source_config)
        pipeline.destination_config = snapshot.get("destination_config", pipeline.destination_config)
        pipeline.transform_config = snapshot.get("transform_config", pipeline.transform_config)
        pipeline.schedule_cron = snapshot.get("schedule_cron", pipeline.schedule_cron)
        pipeline.schedule_enabled = snapshot.get("schedule_enabled", pipeline.schedule_enabled)
        pipeline.sync_mode = snapshot.get("sync_mode", pipeline.sync_mode)
        pipeline.status = PipelineStatus(snapshot.get("status", pipeline.status.value))

        pipeline.updated_at = datetime.now(UTC)
        db.add(pipeline)
        db.commit()
        db.refresh(pipeline)
        
        logger.info("Pipeline restored: pipeline_id=%d", pipeline_id)

        # Create version entry for restoration
        PipelineService.create_pipeline_version(
            db,
            pipeline_id,
            f"Restored to version {version_number}",
        )

        return pipeline