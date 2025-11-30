"""Celery worker tasks for pipeline execution.

Handles background job execution, connector initialization, and ETL processing.
"""

import os
from datetime import datetime, timezone
from typing import Optional

from app.connectors.base import ConnectorFactory, State
from app.core.celery_app import celery_app
from app.core.database import SessionLocal
from app.models.db_models import JobStatus, Pipeline, SystemConfig, PipelineState
from app.pipeline.engine import PipelineEngine
from app.pipeline.processors import (
    NoOpProcessor,
    BaseProcessor,
    ExampleTransformerProcessor,
    DuckDBProcessor,
)
from app.services.encryption import get_encryption_service, initialize_encryption_service
from app.services.job_service import JobService
from app.services.alert_service import AlertService
from app.utils.logging import get_logger

logger = get_logger(__name__)

# Map processor names to their classes
PROCESSOR_MAP = {
    "noop": NoOpProcessor,
    "example_transformer": ExampleTransformerProcessor,
    "duckdb_sql_transformer": DuckDBProcessor,
}


@celery_app.task(name="app.worker.run_pipeline", bind=True)
def run_pipeline(self, job_id: int):
    """Execute a data pipeline job.

    Args:
        self: Celery task instance (bound)
        job_id: ID of the job record
    """
    logger.info("Starting pipeline job: job_id=%d, celery_task_id=%s", job_id, self.request.id)
    
    db = SessionLocal()
    pipeline = None
    
    try:
        # 1. Check Master Password
        master_password = os.getenv("MASTER_PASSWORD")
        if not master_password:
            error_msg = "MASTER_PASSWORD environment variable not set in worker"
            logger.error("❌ %s", error_msg)
            JobService.update_job_status(db, job_id, JobStatus.FAILED, error_message=error_msg)
            return {"status": "failed", "error": error_msg}

        # 2. Initialize Encryption Service
        logger.debug("Initializing encryption service")
        if not _initialize_encryption(db, job_id, master_password):
            return {"status": "failed", "error": "Encryption initialization failed"}

        # 3. Get Job and Pipeline
        job = JobService.get_job(db, job_id)
        if not job:
            logger.error("Job not found: job_id=%d", job_id)
            return {"status": "failed", "error": "Job not found"}

        pipeline = db.query(Pipeline).filter(Pipeline.id == job.pipeline_id).first()
        if not pipeline:
            error_msg = "Pipeline not found"
            logger.error("❌ %s: pipeline_id=%d", error_msg, job.pipeline_id)
            JobService.update_job_status(db, job_id, JobStatus.FAILED, error_message=error_msg)
            return {"status": "failed", "error": error_msg}

        # 4. Update Status to RUNNING
        JobService.update_job_status(db, job_id, JobStatus.RUNNING)
        JobService.log_message(db, job_id, "INFO", f"Starting pipeline: {pipeline.name}")
        logger.info("Pipeline started: pipeline_id=%d, name=%s", pipeline.id, pipeline.name)

        # 5. Create Connectors
        source, destination = _create_connectors(db, job_id, pipeline)
        if not source or not destination:
            return {"status": "failed", "error": "Connector creation failed"}

        # 6. Create Processors
        processors = _create_processors(db, job_id, pipeline)
        if processors is None:
            return {"status": "failed", "error": "Processor creation failed"}

        # 7. Get State for Incremental Sync
        current_state = _get_pipeline_state(db, job_id, pipeline)

        # 8. Execute Pipeline
        stream = pipeline.source_config.get("table_name")
        query = pipeline.source_config.get("query")
        
        logger.info("Executing pipeline: stream=%s, sync_mode=%s", stream, pipeline.sync_mode)
        
        # Create pipeline engine
        engine = PipelineEngine(source, destination, processors)
        
        # Run pipeline
        result = engine.run(
            stream=stream,
            state=current_state.to_dict() if current_state else None,
            query=query,
        )
        
        if result["status"] == "success":
            records_count = result["records_written"]
            
            # 9. Update Job Statistics
            job.records_extracted = records_count
            job.records_loaded = records_count
            db.commit()
            
            # 10. Save State for Incremental Sync
            if pipeline.sync_mode == "incremental":
                _save_pipeline_state(db, pipeline, records_count)
            
            # 11. Update Job Status
            JobService.log_message(db, job_id, "INFO", f"✅ Successfully transferred {records_count} records")
            JobService.update_job_status(db, job_id, JobStatus.SUCCESS)
            logger.info("✅ Pipeline completed: job_id=%d, records=%d", job_id, records_count)
            
            return {"status": "success", "records_written": records_count}
        else:
            error_msg = result.get("error", "Unknown error")
            JobService.log_message(db, job_id, "ERROR", f"Pipeline execution failed: {error_msg}")
            JobService.update_job_status(db, job_id, JobStatus.FAILED, error_message=error_msg)
            AlertService.trigger_job_failure_alert(db, job_id, pipeline.id, error_msg)
            logger.error("❌ Pipeline failed: job_id=%d, error=%s", job_id, error_msg)
            
            return {"status": "failed", "error": error_msg}

    except Exception as e:
        error_msg = f"Unexpected worker error: {e!s}"
        logger.exception("❌ Worker error: job_id=%d", job_id)
        
        JobService.update_job_status(db, job_id, JobStatus.FAILED, error_message=error_msg)
        
        if pipeline:
            AlertService.trigger_job_failure_alert(db, job_id, pipeline.id, error_msg)
        else:
            AlertService.trigger_generic_alert(db, job_id, error_msg)
        
        return {"status": "failed", "error": error_msg}
        
    finally:
        db.close()


def _initialize_encryption(db, job_id: int, master_password: str) -> bool:
    """Initialize encryption service.
    
    Returns:
        True if successful, False otherwise
    """
    try:
        salt_config = db.query(SystemConfig).filter(SystemConfig.key == "dek_salt").first()
        dek_config = db.query(SystemConfig).filter(SystemConfig.key == "encrypted_dek").first()

        if not salt_config or not dek_config:
            error_msg = "System not initialized (missing salt/DEK)"
            logger.error("❌ %s", error_msg)
            JobService.update_job_status(db, job_id, JobStatus.FAILED, error_message=error_msg)
            return False

        initialize_encryption_service(dek_config.value, master_password, salt_config.value)
        logger.debug("Encryption service initialized")
        return True
        
    except Exception as e:
        error_msg = f"Failed to unlock encryption: {e!s}"
        logger.exception("❌ Encryption initialization failed")
        JobService.update_job_status(db, job_id, JobStatus.FAILED, error_message=error_msg)
        return False


def _create_connectors(db, job_id: int, pipeline: Pipeline) -> tuple[Optional[object], Optional[object]]:
    """Create source and destination connectors.
    
    Returns:
        Tuple of (source, destination) or (None, None) on error
    """
    try:
        encryption_service = get_encryption_service()
        
        # Source Connector
        logger.debug("Creating source connector: type=%s", pipeline.source_connection.connector_type)
        source_conn = pipeline.source_connection
        source_config = encryption_service.decrypt_config(source_conn.config_encrypted)
        full_source_config = {**source_config, **pipeline.source_config}
        
        source = ConnectorFactory.create_source(
            source_conn.connector_type.value,
            full_source_config
        )
        logger.info("✅ Source connector created: %s", source_conn.connector_type.value)
        
        # Destination Connector
        logger.debug("Creating destination connector: type=%s", pipeline.destination_connection.connector_type)
        dest_conn = pipeline.destination_connection
        dest_config = encryption_service.decrypt_config(dest_conn.config_encrypted)
        full_dest_config = {**dest_config, **pipeline.destination_config}
        
        destination = ConnectorFactory.create_destination(
            dest_conn.connector_type.value,
            full_dest_config
        )
        logger.info("✅ Destination connector created: %s", dest_conn.connector_type.value)
        
        return source, destination
        
    except Exception as e:
        error_msg = f"Failed to create connectors: {e!s}"
        logger.exception("❌ Connector creation failed")
        JobService.update_job_status(db, job_id, JobStatus.FAILED, error_message=error_msg)
        AlertService.trigger_job_failure_alert(db, job_id, pipeline.id, error_msg)
        return None, None


def _create_processors(db, job_id: int, pipeline: Pipeline) -> Optional[list[BaseProcessor]]:
    """Create data processors from pipeline config.
    
    Returns:
        List of processors or None on error
    """
    try:
        processor_name = (
            pipeline.transform_config.get("processor_type", "noop")
            if pipeline.transform_config
            else "noop"
        )
        
        processor_class = PROCESSOR_MAP.get(processor_name)
        
        if not processor_class:
            error_msg = f"Unknown processor type: {processor_name}"
            logger.error("❌ %s", error_msg)
            JobService.update_job_status(db, job_id, JobStatus.FAILED, error_message=error_msg)
            AlertService.trigger_job_failure_alert(db, job_id, pipeline.id, error_msg)
            return None
        
        processor_config = (
            pipeline.transform_config.get("config", {})
            if pipeline.transform_config
            else {}
        )
        
        processor = processor_class(**processor_config)
        logger.info("Processor created: type=%s", processor_name)
        
        return [processor]
        
    except Exception as e:
        error_msg = f"Failed to create processor: {e!s}"
        logger.exception("❌ Processor creation failed")
        JobService.update_job_status(db, job_id, JobStatus.FAILED, error_message=error_msg)
        return None


def _get_pipeline_state(db, job_id: int, pipeline: Pipeline) -> Optional[State]:
    """Get current pipeline state for incremental sync.
    
    Returns:
        State object or None
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
            state = State(stream=pipeline.name, **pipeline_state_record.state_data)
            logger.info("Resuming from state: cursor_value=%s", state.cursor_value)
            JobService.log_message(db, job_id, "INFO", f"Resuming from state: {state}")
            return state
        else:
            logger.info("No previous state found for incremental sync")
            JobService.log_message(db, job_id, "INFO", "No previous state found for incremental sync")
            return State(stream=pipeline.name, metadata={})
            
    except Exception as e:
        logger.warning("Failed to retrieve pipeline state: %s", e)
        return None


def _save_pipeline_state(db, pipeline: Pipeline, record_count: int) -> None:
    """Save updated pipeline state after successful sync.
    
    Args:
        db: Database session
        pipeline: Pipeline object
        record_count: Number of records processed
    """
    try:
        new_state_data = {
            "last_synced_at": datetime.now(timezone.utc).isoformat(),
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
            logger.debug("Updated existing pipeline state")
        else:
            # Create new state record
            pipeline_state_record = PipelineState(
                pipeline_id=pipeline.id,
                state_data=new_state_data,
                last_record_count=record_count,
                total_records_synced=record_count,
            )
            db.add(pipeline_state_record)
            logger.debug("Created new pipeline state")
        
        db.commit()
        logger.info("Saved pipeline state: last_synced_at=%s", new_state_data["last_synced_at"])
        
    except Exception as e:
        logger.warning("Failed to save pipeline state: %s", e)
        db.rollback()