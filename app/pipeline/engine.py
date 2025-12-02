"""
Universal Pipeline Engine (Operator-Aware) — Improved
-----------------------------------------------------

Streaming, operator-aware pipeline engine that:
• executes: extract -> processors... -> load
• emits operator lifecycle events to callbacks (DB/UI/logger)
• keeps accurate per-operator metrics without materializing full datasets
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Iterator, Optional, Any, List, Protocol, Callable, Generator

from app.core.logging import get_logger
from app.connectors.base import SourceConnector, DestinationConnector, State, Record
from app.pipeline.processors.base import BaseProcessor

logger = get_logger(__name__)


# =============================================================================
# CALLBACK INTERFACE
# =============================================================================
class PipelineCallbacks(Protocol):
    def on_pipeline_start(self): ...
    def on_pipeline_complete(self, metrics: dict): ...
    def on_pipeline_fail(self, error: str, metrics: dict): ...

    def on_operator_start(self, operator_type: str, operator_name: str, stream: Optional[str] = None): ...
    def on_operator_progress(self, message: str, metadata: dict | None = None): ...
    def on_operator_complete(
        self,
        operator_type: str,
        operator_name: str,
        records_in: int,
        records_out: int,
        records_failed: int,
        duration_seconds: float,
    ): ...
    def on_operator_fail(
        self,
        operator_type: str,
        operator_name: str,
        error: str,
        records_in: int,
        records_out: int,
        records_failed: int,
    ): ...


# =============================================================================
# METRICS / RESULT
# =============================================================================
@dataclass
class PipelineMetrics:
    records_read: int = 0
    records_processed: int = 0
    records_written: int = 0
    records_failed: int = 0

    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    completed_at: Optional[datetime] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        if not self.completed_at:
            return None
        return (self.completed_at - self.started_at).total_seconds()

    def finish(self):
        self.completed_at = datetime.now(UTC)

    def to_dict(self):
        return {
            "records_read": self.records_read,
            "records_processed": self.records_processed,
            "records_written": self.records_written,
            "records_failed": self.records_failed,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
        }


@dataclass
class PipelineResult:
    status: str      # "success", "failed", "partial"
    metrics: PipelineMetrics
    error: Optional[str] = None

    def to_dict(self):
        base = {"status": self.status, **self.metrics.to_dict()}
        if self.error:
            base["error"] = self.error
        return base


# =============================================================================
# PIPELINE ENGINE
# =============================================================================
class PipelineEngine:
    """
    Streaming pipeline engine:
    - Extraction = source.read()
    - Transform  = chain of processors (each exposes .process(iterator))
    - Load       = destination.write(iterator)
    """

    def __init__(
        self,
        source: SourceConnector,
        destination: DestinationConnector,
        processors: Optional[List[BaseProcessor]] = None,
        callbacks: Optional[PipelineCallbacks] = None,
    ):
        self.source = source
        self.destination = destination
        self.processors = processors or []
        self.callbacks = callbacks

        logger.info(
            "engine_initialized",
            source=source.__class__.__name__,
            destination=destination.__class__.__name__,
            processor_count=len(self.processors),
        )

    # ---------------------------------------------------------------------
    # RUN
    # ---------------------------------------------------------------------
    def run(
        self,
        stream: Optional[str],
        state: Optional[dict],
        query: Optional[Any],
    ) -> PipelineResult:
        metrics = PipelineMetrics()

        if self.callbacks:
            try:
                self.callbacks.on_pipeline_start()
            except Exception:
                logger.exception("callback_on_pipeline_start_failed")

        try:
            state_obj = self._prepare_state(stream, state)

            # Extract: get metered iterator
            extract_iter = self._run_extract(stream, state_obj, query, metrics)

            # Transform chain: returns an iterator that yields processed records
            transformed_iter = self._run_transform_chain(extract_iter, metrics)

            # Load: wrap counts and call destination.write which consumes the iterator
            self._run_load(transformed_iter, metrics)

            # success
            metrics.finish()
            if self.callbacks:
                try:
                    self.callbacks.on_pipeline_complete(metrics.to_dict())
                except Exception:
                    logger.exception("callback_on_pipeline_complete_failed")

            return PipelineResult(status="success", metrics=metrics)

        except Exception as e:
            metrics.finish()
            if self.callbacks:
                try:
                    self.callbacks.on_pipeline_fail(str(e), metrics.to_dict())
                except Exception:
                    logger.exception("callback_on_pipeline_fail_failed")

            logger.error("pipeline_failed", error=str(e), exc_info=True)
            return PipelineResult(status="failed", metrics=metrics, error=str(e))

    # ---------------------------------------------------------------------
    # EXTRACT
    # ---------------------------------------------------------------------
    def _run_extract(
        self,
        stream: Optional[str],
        state_obj: Optional[State],
        query: Optional[Any],
        metrics: PipelineMetrics,
    ) -> Iterator[Record]:
        op_type = "extract"
        op_name = "extract"
        start_ts = datetime.now(UTC)

        if self.callbacks:
            try:
                self.callbacks.on_operator_start(op_type, op_name, stream)
            except Exception:
                logger.exception("callback_on_operator_start_failed_extract")

        try:
            raw_iter = self.source.read(stream=stream, state=state_obj, query=query)
        except Exception as e:
            if self.callbacks:
                try:
                    self.callbacks.on_operator_fail(op_type, op_name, str(e), 0, 0, 0)
                except Exception:
                    logger.exception("callback_on_operator_fail_failed_extract")
            raise

        def metered() -> Iterator[Record]:
            for rec in raw_iter:
                metrics.records_read += 1
                # periodic progress reporting
                if self.callbacks and (metrics.records_read % 1000 == 0):
                    try:
                        self.callbacks.on_operator_progress(
                            message="extraction_progress",
                            metadata={"records_read": metrics.records_read, "stream": stream},
                        )
                    except Exception:
                        logger.exception("callback_on_operator_progress_failed_extract")
                yield rec

        # fire complete operator once extraction iterator is returned (note: duration is minimal here)
        duration = (datetime.now(UTC) - start_ts).total_seconds()
        if self.callbacks:
            try:
                self.callbacks.on_operator_complete(
                    operator_type=op_type,
                    operator_name=op_name,
                    records_in=0,
                    records_out=metrics.records_read,
                    records_failed=0,
                    duration_seconds=duration,
                )
            except Exception:
                logger.exception("callback_on_operator_complete_failed_extract")

        return metered()

    # ---------------------------------------------------------------------
    # TRANSFORM CHAIN
    # ---------------------------------------------------------------------
    def _run_transform_chain(
        self,
        upstream: Iterator[Record],
        metrics: PipelineMetrics,
    ) -> Iterator[Record]:
        """
        Apply processors sequentially, returning a generator that yields processed records.

        Each processor receives an iterator and must itself be lazy/streaming.
        We report operator events for each processor and update metrics.
        """

        current_iter: Iterator[Record] = upstream

        for processor in self.processors:
            proc_name = processor.__class__.__name__
            op_type = "transform"
            op_start = datetime.now(UTC)

            records_in = 0
            records_out = 0
            records_failed = 0

            if self.callbacks:
                try:
                    self.callbacks.on_operator_start(op_type, proc_name)
                except Exception:
                    logger.exception("callback_on_operator_start_failed_transform")

            # Create wrapper iterators that count inputs and outputs
            def counting_input(it: Iterator[Record]) -> Iterator[Record]:
                nonlocal records_in
                for r in it:
                    records_in += 1
                    yield r

            try:
                processed_iter = processor.process(counting_input(current_iter))

                # Wrap processed_iter to count outputs as they are consumed downstream
                def counting_output(it: Iterator[Record]) -> Iterator[Record]:
                    nonlocal records_out, records_failed
                    for r in it:
                        try:
                            records_out += 1
                            metrics.records_processed += 1
                            yield r
                        except Exception as e_inner:
                            records_failed += 1
                            metrics.records_failed += 1
                            # log but continue
                            if self.callbacks:
                                try:
                                    self.callbacks.on_operator_progress(
                                        message="processor_record_failed",
                                        metadata={"processor": proc_name, "error": str(e_inner)},
                                    )
                                except Exception:
                                    logger.exception("callback_on_operator_progress_failed_processor")
                            # swallow record-level exception and continue
                            continue

                # Set current_iter to the new generator for next processor (do not materialize)
                current_iter = counting_output(processed_iter)

                # After wiring the processor we cannot report records_in/out/failure counts
                # until the iterator has been consumed by downstream (load or next processor).
                # So we will only emit on_operator_complete when the next processor (or load) consumes.
                # To make operator events timely, we wrap current_iter so that when it is fully
                # consumed we call on_operator_complete for this processor.
                current_iter = self._wrap_with_completion_event(
                    current_iter,
                    on_complete=lambda: self._emit_processor_complete(
                        op_type,
                        proc_name,
                        op_start,
                        lambda: records_in,
                        lambda: records_out,
                        lambda: records_failed,
                    ),
                    on_fail=lambda err: self._emit_processor_fail(
                        op_type,
                        proc_name,
                        err,
                        lambda: records_in,
                        lambda: records_out,
                        lambda: records_failed,
                    ),
                )

            except Exception as e:
                # immediate processor-level failure (before consumption)
                if self.callbacks:
                    try:
                        self.callbacks.on_operator_fail(op_type, proc_name, str(e), records_in, records_out, 1)
                    except Exception:
                        logger.exception("callback_on_operator_fail_failed_transform")
                raise

        # After chaining all processors, return the composed iterator
        return current_iter

    # ---------------------------------------------------------------------
    # LOAD
    # ---------------------------------------------------------------------
    def _run_load(self, iterator: Iterator[Record], metrics: PipelineMetrics):
        op_type = "load"
        op_name = "destination_write"
        start_ts = datetime.now(UTC)

        if self.callbacks:
            try:
                self.callbacks.on_operator_start(op_type, op_name)
            except Exception:
                logger.exception("callback_on_operator_start_failed_load")

        records_in = 0

        # Wrap iterator so we can count records as destination consumes them
        def counting_iterator(it: Iterator[Record]) -> Iterator[Record]:
            nonlocal records_in
            for r in it:
                records_in += 1
                yield r

        wrapped_iter = counting_iterator(iterator)

        try:
            # Pass the wrapped iterator to destination.write (it will consume the iterator)
            written = self.destination.write(wrapped_iter)
            metrics.records_written = written

            duration = (datetime.now(UTC) - start_ts).total_seconds()

            if self.callbacks:
                try:
                    self.callbacks.on_operator_complete(
                        operator_type=op_type,
                        operator_name=op_name,
                        records_in=records_in,
                        records_out=written,
                        records_failed=0,
                        duration_seconds=duration,
                    )
                except Exception:
                    logger.exception("callback_on_operator_complete_failed_load")

        except Exception as e:
            if self.callbacks:
                try:
                    self.callbacks.on_operator_fail(
                        operator_type=op_type,
                        operator_name=op_name,
                        error=str(e),
                        records_in=records_in,
                        records_out=0,
                        records_failed=records_in,
                    )
                except Exception:
                    logger.exception("callback_on_operator_fail_failed_load")
            raise

    # ---------------------------------------------------------------------
    # HELPERS: state and iterator utilities
    # ---------------------------------------------------------------------
    def _prepare_state(self, stream: Optional[str], state: Optional[dict]) -> Optional[State]:
        if not state:
            return None
        return State(
            stream=stream,
            cursor_field=state.get("cursor_field"),
            cursor_value=state.get("cursor_value"),
            metadata=state.get("metadata", {}),
        )

    def _wrap_with_completion_event(
        self,
        it: Iterator[Record],
        on_complete: Callable[[], None],
        on_fail: Callable[[str], None],
    ) -> Iterator[Record]:
        """
        Wrap an iterator so that when it is exhausted we call on_complete().
        If an exception is raised while iterating, call on_fail(error) and re-raise.
        """
        def wrapped() -> Generator[Record, None, None]:
            try:
                for r in it:
                    yield r
            except Exception as e:
                try:
                    on_fail(str(e))
                except Exception:
                    logger.exception("on_fail_callback_failed")
                raise
            else:
                try:
                    on_complete()
                except Exception:
                    logger.exception("on_complete_callback_failed")
        return wrapped()

    def _emit_processor_complete(
        self,
        operator_type: str,
        operator_name: str,
        start_ts: datetime,
        get_in: Callable[[], int],
        get_out: Callable[[], int],
        get_failed: Callable[[], int],
    ):
        """
        Called once a processor's iterator is fully consumed.
        """
        try:
            duration = (datetime.now(UTC) - start_ts).total_seconds()
            if self.callbacks:
                self.callbacks.on_operator_complete(
                    operator_type=operator_type,
                    operator_name=operator_name,
                    records_in=get_in(),
                    records_out=get_out(),
                    records_failed=get_failed(),
                    duration_seconds=duration,
                )
        except Exception:
            logger.exception("emit_processor_complete_failed")

    def _emit_processor_fail(
        self,
        operator_type: str,
        operator_name: str,
        error: str,
        get_in: Callable[[], int],
        get_out: Callable[[], int],
        get_failed: Callable[[], int],
    ):
        try:
            if self.callbacks:
                self.callbacks.on_operator_fail(
                    operator_type=operator_type,
                    operator_name=operator_name,
                    error=error,
                    records_in=get_in(),
                    records_out=get_out(),
                    records_failed=get_failed(),
                )
        except Exception:
            logger.exception("emit_processor_fail_failed")
