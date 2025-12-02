"""
Celery Task — Execute Pipeline Run (Clean & Modern)
--------------------------------------------------

Responsibilities:
- Unlock encryption
- Load Job & Pipeline
- Create PipelineRun (with run_number)
- Build connectors + processors
- Run universal PipelineEngine (operator-aware)
- Persist OperatorRuns & Logs via DBCallbacks
- Update PipelineRun result
- Update Job
- Save incremental state
- Trigger alerts
"""

from __future__ import annotations
from datetime import datetime, UTC
from typing import Any, Dict, Optional, Tuple, List

from sqlalchemy.orm import Session

from app.core.logging import get_logger, bind_trace_ids, clear_trace_ids
from app.core.database import SessionLocal
from app.core.config import settings
from app.models.enums import JobStatus, PipelineRunStatus
from app.models.database import (
    Pipeline,
    PipelineRun,
    PipelineState,
    SystemConfig,
    Transformer,
)
from app.connectors.base import ConnectorFactory, SourceConnector, DestinationConnector
from app.pipeline.engine import PipelineEngine, PipelineResult
from app.pipeline.pipeline_callbacks import DBCallbacks
from app.pipeline.processors.registry import get_processor_class
from app.pipeline.processors.base import BaseProcessor

from app.services.job import JobService
from app.services.alert import AlertService
from app.services.encryption import (
    get_encryption_service,
    initialize_encryption_service,
)

from app.worker.app import celery_app

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# MAIN TASK
# ---------------------------------------------------------------------------
@celery_app.task(
    name="pipeline.execute",
    bind=True,
    max_retries=3,
    default_retry_delay=60,
)
def execute_pipeline(self, job_id: int) -> Dict[str, Any]:
    """
    Celery task entrypoint to execute a pipeline run.
    Returns a serializable dict summarizing the run result.
    """
    task_id = self.request.id
    bind_trace_ids(task_id=task_id, job_id=job_id)
    logger.info("pipeline_job_started", job_id=job_id)

    db: Session = SessionLocal()
    pipeline: Optional[Pipeline] = None
    pipeline_run: Optional[PipelineRun] = None
    source: Optional[SourceConnector] = None
    destination: Optional[DestinationConnector] = None

    try:
        # ------------------------------------------------------------------
        # 1. Validate master password & init encryption
        # ------------------------------------------------------------------
        settings.validate_master_password()
        if not _init_encryption(db, job_id):
            return _failed_response("Encryption initialization failed")

        # ------------------------------------------------------------------
        # 2. Load job & pipeline
        # ------------------------------------------------------------------
        job = JobService.get_job(db, job_id)
        if not job:
            return _failed_response("Job not found")

        pipeline = db.query(Pipeline).filter_by(id=job.pipeline_id).first()
        if not pipeline:
            JobService.update_job_status(db, job_id, JobStatus.FAILED, "Pipeline not found")
            return _failed_response("Pipeline not found")

        JobService.update_job_status(db, job_id, JobStatus.RUNNING)
        logger.info("pipeline_execution_started", pipeline_id=pipeline.id, job_id=job_id)

        # ------------------------------------------------------------------
        # 3. Create PipelineRun
        # ------------------------------------------------------------------
        pipeline_run = _create_pipeline_run(db, pipeline, job_id)
        bind_trace_ids(pipeline_run_id=pipeline_run.id, pipeline_id=pipeline.id)

        # ------------------------------------------------------------------
        # 4. Build connectors
        # ------------------------------------------------------------------
        source, destination = _create_connectors(db, pipeline)
        if source is None or destination is None:
            raise RuntimeError("Failed to instantiate source or destination connector")

        # ------------------------------------------------------------------
        # 5. Build processors
        # ------------------------------------------------------------------
        processors = _build_processors(db, pipeline)
        if processors is None:
            raise RuntimeError("Processor initialization failed")

        # ------------------------------------------------------------------
        # 6. Load incremental state (if any)
        # ------------------------------------------------------------------
        state_dict = _load_state(db, pipeline)

        # ------------------------------------------------------------------
        # 7. Prepare callbacks, engine and run
        # ------------------------------------------------------------------
        callbacks = DBCallbacks(db, pipeline_run)
        engine = PipelineEngine(
            source=source,
            destination=destination,
            processors=processors,
            callbacks=callbacks,
        )

        # Let connectors decide stream/query semantics
        stream = None
        query = None
        try:
            stream = source.get_stream_identifier(pipeline.source_config)
        except Exception:
            # connector may not implement stream semantics; proceed
            stream = None

        try:
            query = source.get_query(pipeline.source_config)
        except Exception:
            query = None

        logger.info("pipeline_engine_invoking", pipeline_id=pipeline.id, pipeline_run_id=pipeline_run.id)

        result: PipelineResult = engine.run(stream=stream, state=state_dict, query=query)

        # ------------------------------------------------------------------
        # 8. Handle result (success / failure), persist state/alerts/job updates
        # ------------------------------------------------------------------
        if result.status == "success":
            _on_success(db, pipeline, pipeline_run, result, job_id)
        else:
            _on_failure(db, pipeline, pipeline_run, result, job_id)

        return result.to_dict()

    except Exception as exc:
        # Generic failure handling
        logger.exception("pipeline_job_failed_unexpected", error=str(exc))
        if pipeline_run:
            try:
                pipeline_run.status = PipelineRunStatus.FAILED
                pipeline_run.error_message = str(exc)
                pipeline_run.completed_at = datetime.now(UTC)
                db.commit()
            except Exception:
                db.rollback()

        JobService.update_job_status(db, job_id, JobStatus.FAILED, error_message=str(exc))
        if pipeline:
            try:
                AlertService.trigger_job_failure_alert(db, job_id, pipeline.id, str(exc))
            except Exception:
                logger.exception("alert_trigger_failed", pipeline_id=(pipeline.id if pipeline else None))

        return _failed_response(str(exc))

    finally:
        # ensure connectors are closed
        try:
            if source:
                try:
                    source.disconnect()
                except Exception:
                    logger.debug("error_closing_source_connector", exc_info=True)
            if destination:
                try:
                    destination.disconnect()
                except Exception:
                    logger.debug("error_closing_destination_connector", exc_info=True)
        finally:
            clear_trace_ids()
            db.close()


# ---------------------------------------------------------------------------
# ENCRYPTION INITIALIZATION
# ---------------------------------------------------------------------------
def _init_encryption(db: Session, job_id: int) -> bool:
    try:
        salt = db.query(SystemConfig).filter_by(key="dek_salt").first()
        dek = db.query(SystemConfig).filter_by(key="encrypted_dek").first()

        if not salt or not dek:
            JobService.update_job_status(db, job_id, JobStatus.FAILED, "System not initialized")
            return False

        initialize_encryption_service(dek.value, settings.MASTER_PASSWORD, salt.value)
        return True

    except Exception as e:
        logger.exception("encryption_init_failed", error=str(e))
        return False


# ---------------------------------------------------------------------------
# PIPELINE RUN CREATION
# ---------------------------------------------------------------------------
def _create_pipeline_run(db: Session, pipeline: Pipeline, job_id: int) -> PipelineRun:
    last = (
        db.query(PipelineRun.run_number)
        .filter_by(pipeline_id=pipeline.id)
        .order_by(PipelineRun.run_number.desc())
        .first()
    )
    run_number = (last[0] + 1) if last else 1

    run = PipelineRun(
        pipeline_id=pipeline.id,
        job_id=job_id,
        run_number=run_number,
        status=PipelineRunStatus.RUNNING,
        started_at=datetime.now(UTC),
    )
    db.add(run)
    db.commit()
    logger.info("pipeline_run_created", pipeline_run_id=run.id, run_number=run_number)
    return run


# ---------------------------------------------------------------------------
# CONNECTORS
# ---------------------------------------------------------------------------
def _create_connectors(db: Session, pipeline: Pipeline) -> Tuple[Optional[SourceConnector], Optional[DestinationConnector]]:
    try:
        enc = get_encryption_service()

        # Source connector config merge: decrypted connection config + pipeline overrides
        src_conn = pipeline.source_connection
        src_cfg = enc.decrypt_config(src_conn.config_encrypted)
        final_src = {**src_cfg, **(pipeline.source_config or {})}
        source = ConnectorFactory.create_source(src_conn.connector_type.value, final_src)

        # Destination connector
        dst_conn = pipeline.destination_connection
        dst_cfg = enc.decrypt_config(dst_conn.config_encrypted)
        final_dst = {**dst_cfg, **(pipeline.destination_config or {})}
        destination = ConnectorFactory.create_destination(dst_conn.connector_type.value, final_dst)

        return source, destination

    except Exception as e:
        logger.exception("connector_creation_failed", error=str(e))
        return None, None


# ---------------------------------------------------------------------------
# PROCESSORS
# ---------------------------------------------------------------------------
def _build_processors(db: Session, pipeline: Pipeline) -> Optional[List[BaseProcessor]]:
    try:
        processors: List[BaseProcessor] = []
        cfg = pipeline.transform_config or {}

        # New-style 'transformers' list that may reference stored Transformer records
        if isinstance(cfg, dict) and "transformers" in cfg:
            for item in cfg["transformers"]:
                transformer_id = item.get("transformer_id")
                override = item.get("config_override", {}) or {}

                if transformer_id:
                    t = db.query(Transformer).filter_by(id=transformer_id).first()
                    if not t:
                        logger.warning("transformer_not_found", transformer_id=transformer_id)
                        continue
                    type_ = t.type
                    cfg_ = {**(t.config or {}), **override}
                else:
                    type_ = item.get("type", "noop")
                    cfg_ = item.get("config", {})

                cls = get_processor_class(type_)
                processors.append(cls(**cfg_))

        # Legacy single-processor config
        elif isinstance(cfg, dict) and "processor_type" in cfg:
            cls = get_processor_class(cfg["processor_type"])
            processors.append(cls(**cfg.get("config", {})))

        logger.info("processors_loaded", count=len(processors))
        return processors

    except Exception as e:
        logger.exception("processor_init_failed", error=str(e))
        return None


# ---------------------------------------------------------------------------
# STATE LOADING & SAVING
# ---------------------------------------------------------------------------
def _load_state(db: Session, pipeline: Pipeline) -> Optional[Dict[str, Any]]:
    if pipeline.sync_mode != "incremental":
        return None

    st = db.query(PipelineState).filter_by(pipeline_id=pipeline.id).first()
    return st.state_data if st else None


def _save_state(db: Session, pipeline: Pipeline, result: PipelineResult) -> None:
    """
    Persist incremental state after successful run.
    """
    written = result.metrics.records_written if result and result.metrics else 0

    next_state = {
        "last_synced_at": datetime.now(UTC).isoformat(),
        "records_written": written,
    }

    st = db.query(PipelineState).filter_by(pipeline_id=pipeline.id).first()

    try:
        if st:
            st.state_data = next_state
            st.last_record_count = written
            st.total_records_synced = (st.total_records_synced or 0) + written
        else:
            st = PipelineState(
                pipeline_id=pipeline.id,
                state_data=next_state,
                last_record_count=written,
                total_records_synced=written,
            )
            db.add(st)
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("pipeline_state_save_failed", pipeline_id=pipeline.id)


# ---------------------------------------------------------------------------
# RESULT HANDLERS
# ---------------------------------------------------------------------------
def _on_success(
    db: Session,
    pipeline: Pipeline,
    run: PipelineRun,
    result: PipelineResult,
    job_id: int,
):
    try:
        run.status = PipelineRunStatus.COMPLETED
        run.completed_at = datetime.now(UTC)
        run.duration_seconds = result.metrics.duration_seconds if result.metrics else None

        # Persist totals (if engine collected them)
        if result.metrics:
            run.total_extracted = result.metrics.records_read
            run.total_transformed = result.metrics.records_processed
            run.total_loaded = result.metrics.records_written
            run.total_failed = result.metrics.records_failed

        db.commit()
    except Exception:
        db.rollback()
        logger.exception("pipeline_run_update_failed_on_success", pipeline_run_id=getattr(run, "id", None))

    # Save incremental state if incremental pipeline
    try:
        _save_state(db, pipeline, result)
    except Exception:
        logger.exception("save_state_failed_on_success", pipeline_id=pipeline.id)

    # Update job and record a success message
    try:
        JobService.update_job_status(db, job_id, JobStatus.SUCCESS)
        JobService.log_message(
            db,
            job_id,
            "INFO",
            f"Success: {result.metrics.records_written if result.metrics else 0} records transferred",
        )
    except Exception:
        logger.exception("job_update_failed_on_success", job_id=job_id)

    logger.info("pipeline_run_successful", pipeline_run_id=run.id)


def _on_failure(
    db: Session,
    pipeline: Pipeline,
    run: PipelineRun,
    result: PipelineResult,
    job_id: int,
):
    try:
        run.status = PipelineRunStatus.FAILED
        run.error_message = result.error
        run.completed_at = datetime.now(UTC)
        run.duration_seconds = result.metrics.duration_seconds if result.metrics else None

        if result.metrics:
            run.total_extracted = result.metrics.records_read
            run.total_transformed = result.metrics.records_processed
            run.total_loaded = result.metrics.records_written
            run.total_failed = result.metrics.records_failed

        db.commit()
    except Exception:
        db.rollback()
        logger.exception("pipeline_run_update_failed_on_failure", pipeline_run_id=getattr(run, "id", None))

    try:
        JobService.update_job_status(db, job_id, JobStatus.FAILED, error_message=result.error)
    except Exception:
        logger.exception("job_update_failed_on_failure", job_id=job_id)

    try:
        AlertService.trigger_job_failure_alert(db, job_id, pipeline.id, result.error)
    except Exception:
        logger.exception("alert_trigger_failed_on_failure", pipeline_id=pipeline.id)

    logger.error("pipeline_run_failed", pipeline_run_id=run.id, error=result.error)


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
def _failed_response(msg: str) -> Dict[str, Any]:
    return {"status": "failed", "error": msg}
