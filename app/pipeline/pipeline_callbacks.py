"""
Pipeline Engine → Database Callback Handler
-------------------------------------------

This handles persistence of:
✓ PipelineRun lifecycle
✓ OperatorRun lifecycle
✓ Structured operator logs
✓ Metrics (read / processed / written per operator)
✓ Stream-level logging
✓ Error reporting
✓ State checkpoint logging
"""

from __future__ import annotations

from datetime import datetime, UTC
from typing import Optional, Dict, Any

from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.models.database import PipelineRun, OperatorRun, OperatorRunLog
from app.models.enums import (
    OperatorType,
    OperatorRunStatus,
    PipelineRunStatus,
)

logger = get_logger(__name__)


class DBCallbacks:
    """
    DB callback handler for the Pipeline Engine.

    All methods are *safe*, meaning they will:
    • never raise exceptions back into the engine
    • log fallback errors
    """

    def __init__(self, db: Session, pipeline_run: PipelineRun):
        self.db = db
        self.pipeline_run = pipeline_run
        self.current_operator: Optional[OperatorRun] = None

    # ===================================================================
    # INTERNAL SAFE COMMIT
    # ===================================================================
    def _safe_commit(self):
        try:
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            logger.error("db_callbacks_commit_failed", error=str(e), exc_info=True)

    # ===================================================================
    # PIPELINE EVENTS
    # ===================================================================
    def on_pipeline_start(self):
        try:
            self.pipeline_run.status = PipelineRunStatus.RUNNING
            self.pipeline_run.started_at = datetime.now(UTC)
            self._safe_commit()
            logger.info(
                "pipeline_run_started",
                pipeline_run_id=self.pipeline_run.id,
            )
        except Exception:
            logger.error("db_callbacks_on_pipeline_start_failed", exc_info=True)

    def on_pipeline_complete(self, metrics: Dict):
        try:
            self.pipeline_run.status = PipelineRunStatus.COMPLETED
            self.pipeline_run.completed_at = datetime.now(UTC)
            self.pipeline_run.duration_seconds = metrics.get("duration_seconds")

            # total metrics
            self.pipeline_run.total_extracted = metrics.get("records_read", 0)
            self.pipeline_run.total_transformed = metrics.get("records_processed", 0)
            self.pipeline_run.total_loaded = metrics.get("records_written", 0)

            self._safe_commit()

            logger.info(
                "pipeline_run_completed",
                pipeline_run_id=self.pipeline_run.id,
                metrics=metrics,
            )

        except Exception:
            logger.error("db_callbacks_on_pipeline_complete_failed", exc_info=True)

    def on_pipeline_fail(self, error: str, metrics: Dict):
        try:
            self.pipeline_run.status = PipelineRunStatus.FAILED
            self.pipeline_run.error_message = error
            self.pipeline_run.completed_at = datetime.now(UTC)
            self.pipeline_run.duration_seconds = metrics.get("duration_seconds")
            self._safe_commit()

            logger.warning(
                "pipeline_run_failed",
                pipeline_run_id=self.pipeline_run.id,
                error=error,
            )

        except Exception:
            logger.error("db_callbacks_on_pipeline_fail_failed", exc_info=True)

    # ===================================================================
    # OPERATOR EVENTS
    # ===================================================================
    def on_operator_start(self, operator_type: str, operator_name: str, stream: str | None = None):
        """
        Called whenever a new operator block starts:
        - source reader
        - processor execution
        - destination writer
        """

        try:
            op = OperatorRun(
                pipeline_run_id=self.pipeline_run.id,
                operator_type=OperatorType(operator_type.upper()),
                operator_name=operator_name,
                status=OperatorRunStatus.RUNNING,
                started_at=datetime.now(UTC),
                stream_name=stream,
            )
            self.db.add(op)
            self._safe_commit()
            self.current_operator = op

            logger.info(
                "operator_started",
                operator_name=operator_name,
                operator_type=operator_type,
                stream=stream,
                operator_run_id=op.id,
            )

        except Exception:
            logger.error("db_callbacks_on_operator_start_failed", exc_info=True)

    def on_operator_progress(self, message: str, metadata: Dict | None = None):
        if not self.current_operator:
            return

        try:
            log = OperatorRunLog(
                operator_run_id=self.current_operator.id,
                level="INFO",
                message=message,
                metadata=metadata,
            )
            self.db.add(log)
            self._safe_commit()

        except Exception:
            logger.error("db_callbacks_on_operator_progress_failed", exc_info=True)

    # NEW → Useful for batch-based connectors / processors
    def on_record_batch(self, batch_size: int, stream: str | None = None):
        """Log per-batch progress."""
        if not self.current_operator:
            return

        try:
            log = OperatorRunLog(
                operator_run_id=self.current_operator.id,
                level="INFO",
                message=f"Processed batch of {batch_size} records.",
                metadata={"batch_size": batch_size, "stream": stream},
            )
            self.db.add(log)
            self._safe_commit()

        except Exception:
            logger.error("db_callbacks_on_record_batch_failed", exc_info=True)

    def on_state_checkpoint(self, state: Dict[str, Any]):
        """Log incremental sync state updates."""
        if not self.current_operator:
            return

        try:
            log = OperatorRunLog(
                operator_run_id=self.current_operator.id,
                level="INFO",
                message="State checkpoint updated.",
                metadata=state,
            )
            self.db.add(log)
            self._safe_commit()

        except Exception:
            logger.error("db_callbacks_on_state_checkpoint_failed", exc_info=True)

    def on_operator_complete(
        self,
        records_in: int,
        records_out: int,
        records_failed: int,
        duration_seconds: float,
    ):
        if not self.current_operator:
            return

        try:
            op = self.current_operator
            op.status = OperatorRunStatus.COMPLETED
            op.completed_at = datetime.now(UTC)
            op.duration_seconds = duration_seconds
            op.records_in = records_in
            op.records_out = records_out
            op.records_failed = records_failed

            self._safe_commit()

            logger.info(
                "operator_completed",
                operator_run_id=op.id,
                records_in=records_in,
                records_out=records_out,
                records_failed=records_failed,
                duration_seconds=duration_seconds,
            )

        except Exception:
            logger.error("db_callbacks_on_operator_complete_failed", exc_info=True)

    def on_operator_fail(self, error: str, records_in: int, records_out: int, records_failed: int):
        if not self.current_operator:
            return

        try:
            op = self.current_operator
            op.status = OperatorRunStatus.FAILED
            op.error_message = error
            op.completed_at = datetime.now(UTC)
            op.records_in = records_in
            op.records_out = records_out
            op.records_failed = records_failed

            self._safe_commit()

            logger.warning(
                "operator_failed",
                operator_run_id=op.id,
                error=error,
            )

        except Exception:
            logger.error("db_callbacks_on_operator_fail_failed", exc_info=True)
