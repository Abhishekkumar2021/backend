"""
DB Callbacks for Pipeline Engine — WITH CORRELATION ID
-------------------------------------------------------

Updates:
✓ Propagates correlation_id to OperatorRuns
✓ Adds correlation_id to OperatorRunLogs
✓ Binds operator context to structlog
"""

from datetime import datetime, UTC
from typing import Optional, Dict, Any

from sqlalchemy.orm import Session

from app.core.logging import get_logger, bind_trace_ids
from app.models.enums import OperatorRunStatus, OperatorType
from app.models.database import PipelineRun, OperatorRun, OperatorRunLog

logger = get_logger(__name__)


class DBCallbacks:
    """
    Database-backed callbacks for PipelineEngine.
    Persists OperatorRun records and logs with correlation_id.
    """
    
    def __init__(
        self,
        db: Session,
        pipeline_run: PipelineRun,
        correlation_id: Optional[str] = None
    ):
        self.db = db
        self.pipeline_run = pipeline_run
        self.correlation_id = correlation_id or "unknown"
        self._current_operator_run: Optional[OperatorRun] = None
    
    # -------------------------------------------------------------------------
    # PIPELINE LIFECYCLE
    # -------------------------------------------------------------------------
    
    def on_pipeline_start(self):
        """Called when pipeline starts."""
        logger.info(
            "pipeline_started",
            pipeline_run_id=self.pipeline_run.id,
        )
    
    def on_pipeline_complete(self, metrics: dict):
        """Called when pipeline completes successfully."""
        logger.info(
            "pipeline_completed",
            pipeline_run_id=self.pipeline_run.id,
            **metrics
        )
    
    def on_pipeline_fail(self, error: str, metrics: dict):
        """Called when pipeline fails."""
        logger.error(
            "pipeline_failed",
            pipeline_run_id=self.pipeline_run.id,
            error=error,
            **metrics
        )
    
    # -------------------------------------------------------------------------
    # OPERATOR LIFECYCLE (with correlation_id)
    # -------------------------------------------------------------------------
    
    def on_operator_start(
        self,
        operator_type: str,
        operator_name: str,
        stream: Optional[str] = None
    ):
        """
        Called when an operator starts.
        Creates OperatorRun record WITH correlation_id.
        """
        try:
            # Map operator_type string to enum
            op_type_enum = self._map_operator_type(operator_type)
            
            # Create OperatorRun
            self._current_operator_run = OperatorRun(
                pipeline_run_id=self.pipeline_run.id,
                operator_type=op_type_enum,
                operator_name=operator_name,
                status=OperatorRunStatus.RUNNING,
                started_at=datetime.now(UTC),
                correlation_id=self.correlation_id,  # ✅ Propagate correlation_id
            )
            self.db.add(self._current_operator_run)
            self.db.commit()
            
            # Bind to structlog context
            bind_trace_ids(
                operator_run_id=self._current_operator_run.id,
                operator_name=operator_name,
                operator_type=operator_type,
            )
            
            logger.info(
                "operator_started",
                operator_run_id=self._current_operator_run.id,
                operator_name=operator_name,
                stream=stream,
            )
        
        except Exception as e:
            logger.exception("operator_start_callback_failed", error=str(e))
            self.db.rollback()
    
    def on_operator_progress(
        self,
        message: str,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Called periodically during operator execution."""
        if not self._current_operator_run:
            return
        
        try:
            # Create log entry WITH correlation_id
            log = OperatorRunLog(
                operator_run_id=self._current_operator_run.id,
                level="INFO",
                message=message,
                log_metadata=metadata,
                timestamp=datetime.now(UTC),
                correlation_id=self.correlation_id,  # ✅ Add to logs
            )
            self.db.add(log)
            self.db.commit()
            
            logger.debug(
                "operator_progress",
                operator_run_id=self._current_operator_run.id,
                message=message,
                **(metadata or {})
            )
        
        except Exception as e:
            logger.warning("operator_progress_callback_failed", error=str(e))
            self.db.rollback()
    
    def on_operator_complete(
        self,
        operator_type: str,
        operator_name: str,
        records_in: int,
        records_out: int,
        records_failed: int,
        duration_seconds: float,
    ):
        """Called when operator completes successfully."""
        if not self._current_operator_run:
            logger.warning(
                "operator_complete_called_without_start",
                operator_name=operator_name
            )
            return
        
        try:
            self._current_operator_run.status = OperatorRunStatus.SUCCESS
            self._current_operator_run.completed_at = datetime.now(UTC)
            self._current_operator_run.duration_seconds = duration_seconds
            self._current_operator_run.records_in = records_in
            self._current_operator_run.records_out = records_out
            self._current_operator_run.records_failed = records_failed
            
            self.db.commit()
            
            logger.info(
                "operator_completed",
                operator_run_id=self._current_operator_run.id,
                operator_name=operator_name,
                records_in=records_in,
                records_out=records_out,
                records_failed=records_failed,
                duration_seconds=duration_seconds,
            )
            
            self._current_operator_run = None
        
        except Exception as e:
            logger.exception("operator_complete_callback_failed", error=str(e))
            self.db.rollback()
    
    def on_operator_fail(
        self,
        operator_type: str,
        operator_name: str,
        error: str,
        records_in: int,
        records_out: int,
        records_failed: int,
    ):
        """Called when operator fails."""
        if not self._current_operator_run:
            logger.warning(
                "operator_fail_called_without_start",
                operator_name=operator_name
            )
            return
        
        try:
            self._current_operator_run.status = OperatorRunStatus.FAILED
            self._current_operator_run.completed_at = datetime.now(UTC)
            self._current_operator_run.error_message = error
            self._current_operator_run.records_in = records_in
            self._current_operator_run.records_out = records_out
            self._current_operator_run.records_failed = records_failed
            
            self.db.commit()
            
            logger.error(
                "operator_failed",
                operator_run_id=self._current_operator_run.id,
                operator_name=operator_name,
                error=error,
                records_in=records_in,
                records_out=records_out,
                records_failed=records_failed,
            )
            
            self._current_operator_run = None
        
        except Exception as e:
            logger.exception("operator_fail_callback_failed", error=str(e))
            self.db.rollback()
    
    # -------------------------------------------------------------------------
    # HELPERS
    # -------------------------------------------------------------------------
    
    def _map_operator_type(self, operator_type: str) -> OperatorType:
        """Map string operator type to enum."""
        mapping = {
            "extract": OperatorType.EXTRACT,
            "transform": OperatorType.TRANSFORM,
            "load": OperatorType.LOAD,
            "custom": OperatorType.CUSTOM,
            "noop": OperatorType.NOOP,
        }
        return mapping.get(operator_type.lower(), OperatorType.CUSTOM)