"""Pipeline execution tasks for Celery workers.

Handles background job execution, connector initialization, and ETL processing.
"""


from datetime import datetime, UTC
from typing import Optional, Dict, Any, Tuple
from sqlalchemy.orm import Session
from app.connectors.base import ConnectorFactory
from app.connectors.base import SourceConnector, DestinationConnector
from app.core.database import SessionLocal
from app.core.logging import get_logger
from app.models.database import Pipeline, SystemConfig, PipelineState
from app.models.enums import JobStatus
from app.pipeline.engine import PipelineEngine, PipelineResult
from app.pipeline.processors.base import BaseProcessor
from app.pipeline.processors.noop import NoOpProcessor
from app.pipeline.processors.sql import DuckDBProcessor
from app.pipeline.processors.transformer import ExampleTransformerProcessor
from app.services.alert_service import AlertService
from app.services.encryption import get_encryption_service, initialize_encryption_service
from app.services.job_service import JobService
from app.worker.app import celery_app
from app.core.config import settings

logger = get_logger(__name__)

# Processor registry - maps processor names to classes
PROCESSOR_REGISTRY: Dict[str, type[BaseProcessor]] = {
    "noop": NoOpProcessor,
    "example_transformer": ExampleTransformerProcessor,
    "duckdb_sql_transformer": DuckDBProcessor,
}


@celery_app.task(
    name="app.worker.tasks.pipeline_tasks.execute_pipeline",
    bind=True,
    max_retries=3,
    default_retry_delay=60,
)
def execute_pipeline(self, job_id: int) -> Dict[str, Any]:
    """Execute a data pipeline job.
    
    This is the main Celery task that orchestrates pipeline execution:
    1. Initialize encryption service
    2. Load pipeline configuration
    3. Create source and destination connectors
    4. Execute pipeline with processors
    5. Update job status and metrics
    
    Args:
        self: Celery task instance (bound)
        job_id: ID of the job record to execute
        
    Returns:
        Dictionary with execution result
        
    Example:
        >>> result = execute_pipeline.delay(job_id=123)
        >>> print(result.get())
        {"status": "success", "records_written": 1000}
    """
    logger.info(
        "pipeline_job_started",
        job_id=job_id,
        task_id=self.request.id,
    )
    
    db = SessionLocal()
    pipeline = None
    
    try:
        # Step 1: Initialize encryption
        master_password = _get_master_password()
        if not _initialize_encryption(db, job_id, master_password):
            return {"status": "failed", "error": "Encryption initialization failed"}
        
        # Step 2: Load job and pipeline
        job = JobService.get_job(db, job_id)
        if not job:
            logger.error("job_not_found", job_id=job_id)
            return {"status": "failed", "error": "Job not found"}
        
        pipeline = db.query(Pipeline).filter(Pipeline.id == job.pipeline_id).first()
        if not pipeline:
            error_msg = "Pipeline not found"
            logger.error("pipeline_not_found", job_id=job_id, pipeline_id=job.pipeline_id)
            JobService.update_job_status(db, job_id, JobStatus.FAILED, error_message=error_msg)
            return {"status": "failed", "error": error_msg}
        
        # Step 3: Update job status to RUNNING
        JobService.update_job_status(db, job_id, JobStatus.RUNNING)
        JobService.log_message(
            db,
            job_id,
            "INFO",
            f"Starting pipeline execution: {pipeline.name}",
        )
        
        logger.info(
            "pipeline_execution_started",
            job_id=job_id,
            pipeline_id=pipeline.id,
            pipeline_name=pipeline.name,
        )
        
        # Step 4: Create connectors
        source, destination = _create_connectors(db, job_id, pipeline)
        if not source or not destination:
            return {"status": "failed", "error": "Connector creation failed"}
        
        # Step 5: Create processors
        processors = _create_processors(db, job_id, pipeline)
        if processors is None:
            return {"status": "failed", "error": "Processor creation failed"}
        
        # Step 6: Get incremental state
        current_state = _get_pipeline_state(db, pipeline)
        
        # Step 7: Execute pipeline
        result = _execute_pipeline(
            db=db,
            job_id=job_id,
            pipeline=pipeline,
            source=source,
            destination=destination,
            processors=processors,
            state=current_state,
        )
        
        # Step 8: Handle result
        if result.status == "success":
            _handle_success(db, job_id, pipeline, result)
            return result.to_dict()
        else:
            _handle_failure(db, job_id, pipeline, result)
            return result.to_dict()
            
    except Exception as e:
        error_msg = f"Unexpected worker error: {e}"
        
        logger.error(
            "pipeline_job_failed",
            job_id=job_id,
            error=str(e),
            error_type=e.__class__.__name__,
            exc_info=True,
        )
        
        JobService.update_job_status(db, job_id, JobStatus.FAILED, error_message=error_msg)
        
        if pipeline:
            AlertService.trigger_job_failure_alert(db, job_id, pipeline.id, error_msg)
        else:
            AlertService.trigger_generic_alert(db, job_id, error_msg)
        
        return {"status": "failed", "error": error_msg}
        
    finally:
        db.close()


# ===================================================================
# Helper Functions
# ===================================================================

def _get_master_password() -> str:
    """Get master password from environment.
    
    Returns:
        Master password string
        
    Raises:
        EncryptionError: If master password not set
    """
    settings.validate_master_password()
    
    return settings.MASTER_PASSWORD


def _initialize_encryption(db: Session, job_id: int, master_password: str) -> bool:
    """Initialize encryption service.
    
    Args:
        db: Database session
        job_id: Job ID for error logging
        master_password: Master password
        
    Returns:
        True if successful, False otherwise
    """
    try:
        salt_config = db.query(SystemConfig).filter(
            SystemConfig.key == "dek_salt"
        ).first()
        
        dek_config = db.query(SystemConfig).filter(
            SystemConfig.key == "encrypted_dek"
        ).first()

        if not salt_config or not dek_config:
            error_msg = "System not initialized (missing salt/DEK)"
            logger.error("encryption_config_missing")
            JobService.update_job_status(db, job_id, JobStatus.FAILED, error_message=error_msg)
            return False

        initialize_encryption_service(
            dek_config.value,
            master_password,
            salt_config.value,
        )
        
        logger.debug("encryption_service_initialized")
        return True
        
    except Exception as e:
        error_msg = f"Failed to unlock encryption: {e}"
        
        logger.error(
            "encryption_initialization_failed",
            error=str(e),
            exc_info=True,
        )
        
        JobService.update_job_status(db, job_id, JobStatus.FAILED, error_message=error_msg)
        return False


def _create_connectors(
    db,
    job_id: int,
    pipeline: Pipeline,
) -> Tuple[Optional[SourceConnector], Optional[DestinationConnector]]:
    """Create source and destination connectors.
    
    Args:
        db: Database session
        job_id: Job ID for error logging
        pipeline: Pipeline object
        
    Returns:
        Tuple of (source, destination) or (None, None) on error
    """
    try:
        encryption_service = get_encryption_service()
        
        # Create source connector
        logger.debug(
            "creating_source_connector",
            connector_type=pipeline.source_connection.connector_type.value,
        )
        
        source_conn = pipeline.source_connection
        source_config = encryption_service.decrypt_config(source_conn.config_encrypted)
        full_source_config = {**source_config, **pipeline.source_config}
        
        source = ConnectorFactory.create_source(
            source_conn.connector_type.value,
            full_source_config,
        )
        
        logger.info(
            "source_connector_created",
            connector_type=source_conn.connector_type.value,
        )
        
        # Create destination connector
        logger.debug(
            "creating_destination_connector",
            connector_type=pipeline.destination_connection.connector_type.value,
        )
        
        dest_conn = pipeline.destination_connection
        dest_config = encryption_service.decrypt_config(dest_conn.config_encrypted)
        full_dest_config = {**dest_config, **pipeline.destination_config}
        
        destination = ConnectorFactory.create_destination(
            dest_conn.connector_type.value,
            full_dest_config,
        )
        
        logger.info(
            "destination_connector_created",
            connector_type=dest_conn.connector_type.value,
        )
        
        return source, destination
        
    except Exception as e:
        error_msg = f"Failed to create connectors: {e}"
        
        logger.error(
            "connector_creation_failed",
            error=str(e),
            exc_info=True,
        )
        
        JobService.update_job_status(db, job_id, JobStatus.FAILED, error_message=error_msg)
        AlertService.trigger_job_failure_alert(db, job_id, pipeline.id, error_msg)
        
        return None, None


def _create_processors(
    db,
    job_id: int,
    pipeline: Pipeline,
) -> Optional[list[BaseProcessor]]:
    """Create data processors from pipeline config.
    
    Args:
        db: Database session
        job_id: Job ID for error logging
        pipeline: Pipeline object
        
    Returns:
        List of processors or None on error
    """
    try:
        processor_name = (
            pipeline.transform_config.get("processor_type", "noop")
            if pipeline.transform_config
            else "noop"
        )
        
        processor_class = PROCESSOR_REGISTRY.get(processor_name)
        
        if not processor_class:
            error_msg = f"Unknown processor type: {processor_name}"
            
            logger.error(
                "unknown_processor_type",
                processor_type=processor_name,
                available_processors=list(PROCESSOR_REGISTRY.keys()),
            )
            
            JobService.update_job_status(db, job_id, JobStatus.FAILED, error_message=error_msg)
            AlertService.trigger_job_failure_alert(db, job_id, pipeline.id, error_msg)
            return None
        
        processor_config = (
            pipeline.transform_config.get("config", {})
            if pipeline.transform_config
            else {}
        )
        
        processor = processor_class(**processor_config)
        
        logger.info(
            "processor_created",
            processor_type=processor_name,
        )
        
        return [processor]
        
    except Exception as e:
        error_msg = f"Failed to create processor: {e}"
        
        logger.error(
            "processor_creation_failed",
            error=str(e),
            exc_info=True,
        )
        
        JobService.update_job_status(db, job_id, JobStatus.FAILED, error_message=error_msg)
        return None


def _get_pipeline_state(db, pipeline: Pipeline) -> Optional[Dict[str, Any]]:
    """Get current pipeline state for incremental sync.
    
    Args:
        db: Database session
        pipeline: Pipeline object
        
    Returns:
        State dictionary or None
    """
    if pipeline.sync_mode != "incremental":
        return None
    
    try:
        pipeline_state_record = (
            db.query(PipelineState)
            .filter(PipelineState.pipeline_id == pipeline.id)
            .first()
        )
        
        if pipeline_state_record and pipeline_state_record.state_data:
            logger.info(
                "pipeline_state_loaded",
                pipeline_id=pipeline.id,
                state_data=pipeline_state_record.state_data,
            )
            return pipeline_state_record.state_data
        else:
            logger.info(
                "no_previous_state",
                pipeline_id=pipeline.id,
            )
            return None
            
    except Exception as e:
        logger.warning(
            "state_retrieval_failed",
            pipeline_id=pipeline.id,
            error=str(e),
        )
        return None


def _execute_pipeline(
    db,
    job_id: int,
    pipeline: Pipeline,
    source: SourceConnector,
    destination: DestinationConnector,
    processors: list[BaseProcessor],
    state: Optional[Dict[str, Any]],
) -> PipelineResult:
    """Execute the pipeline.
    
    Args:
        db: Database session
        job_id: Job ID
        pipeline: Pipeline object
        source: Source connector
        destination: Destination connector
        processors: List of processors
        state: Incremental state
        
    Returns:
        PipelineResult object
    """
    stream = pipeline.source_config.get("table_name")
    query = pipeline.source_config.get("query")
    
    logger.info(
        "executing_pipeline",
        stream=stream,
        sync_mode=pipeline.sync_mode,
        has_query=query is not None,
    )
    
    # Create pipeline engine
    engine = PipelineEngine(source, destination, processors)
    
    # Run pipeline
    result = engine.run(
        stream=stream,
        state=state,
        query=query,
    )
    
    return result


def _handle_success(
    db,
    job_id: int,
    pipeline: Pipeline,
    result: PipelineResult,
) -> None:
    """Handle successful pipeline execution.
    
    Args:
        db: Database session
        job_id: Job ID
        pipeline: Pipeline object
        result: Pipeline result
    """
    records_count = result.metrics.records_written
    
    # Update job statistics
    job = JobService.get_job(db, job_id)
    if job:
        job.records_extracted = result.metrics.records_read
        job.records_loaded = result.metrics.records_written
        job.records_failed = result.metrics.records_failed
        db.commit()
    
    # Save state for incremental sync
    if pipeline.sync_mode == "incremental":
        _save_pipeline_state(db, pipeline, records_count)
    
    # Update job status
    JobService.log_message(
        db,
        job_id,
        "INFO",
        f"Successfully transferred {records_count} records",
    )
    
    JobService.update_job_status(db, job_id, JobStatus.SUCCESS)
    
    logger.info(
        "pipeline_execution_succeeded",
        job_id=job_id,
        pipeline_id=pipeline.id,
        **result.metrics.to_dict(),
    )


def _handle_failure(
    db,
    job_id: int,
    pipeline: Pipeline,
    result: PipelineResult,
) -> None:
    """Handle failed pipeline execution.
    
    Args:
        db: Database session
        job_id: Job ID
        pipeline: Pipeline object
        result: Pipeline result
    """
    error_msg = result.error or "Unknown error"
    
    JobService.log_message(
        db,
        job_id,
        "ERROR",
        f"Pipeline execution failed: {error_msg}",
    )
    
    JobService.update_job_status(db, job_id, JobStatus.FAILED, error_message=error_msg)
    AlertService.trigger_job_failure_alert(db, job_id, pipeline.id, error_msg)
    
    logger.error(
        "pipeline_execution_failed",
        job_id=job_id,
        pipeline_id=pipeline.id,
        error=error_msg,
        **result.metrics.to_dict(),
    )


def _save_pipeline_state(db, pipeline: Pipeline, record_count: int) -> None:
    """Save updated pipeline state after successful sync.
    
    Args:
        db: Database session
        pipeline: Pipeline object
        record_count: Number of records processed
    """
    try:
        new_state_data = {
            "last_synced_at": datetime.now(UTC).isoformat(),
            "record_count": record_count,
        }
        
        pipeline_state_record = (
            db.query(PipelineState)
            .filter(PipelineState.pipeline_id == pipeline.id)
            .first()
        )
        
        if pipeline_state_record:
            # Update existing state
            pipeline_state_record.state_data = new_state_data
            pipeline_state_record.last_record_count = record_count
            pipeline_state_record.total_records_synced += record_count
            
            logger.debug(
                "pipeline_state_updated",
                pipeline_id=pipeline.id,
            )
        else:
            # Create new state record
            pipeline_state_record = PipelineState(
                pipeline_id=pipeline.id,
                state_data=new_state_data,
                last_record_count=record_count,
                total_records_synced=record_count,
            )
            db.add(pipeline_state_record)
            
            logger.debug(
                "pipeline_state_created",
                pipeline_id=pipeline.id,
            )
        
        db.commit()
        
        logger.info(
            "pipeline_state_saved",
            pipeline_id=pipeline.id,
            last_synced_at=new_state_data["last_synced_at"],
        )
        
    except Exception as e:
        logger.warning(
            "pipeline_state_save_failed",
            pipeline_id=pipeline.id,
            error=str(e),
        )
        db.rollback()