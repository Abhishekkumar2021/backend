"""
Base class for all ETL Processors.
Supports:
✓ Iterator chaining
✓ Row-level and batch-level transformations
✓ Structured logging via trace IDs
✓ TransformError wrapping
✓ Optional setup/teardown lifecycle hooks
"""

from typing import Optional, Iterator, Iterable, Callable, List, Any
from app.connectors.base import Record
from app.core.logging import get_logger, bind_trace_ids
from app.core.exceptions import TransformError

logger = get_logger(__name__)


class BaseProcessor:
    """
    Base processor class for ETL pipelines.

    Responsibilities:
    -----------------
    ✓ Transform records (row-level)
    ✓ Filter records (return None)
    ✓ Support streaming iterator chaining
    ✓ Allow child classes to override:
        - setup()
        - teardown()
        - transform_record()
        - process_batch()

    All processors must implement *at least one* of:
    - transform_record()
    - process_batch()
    """

    # ---------------------------------------------------------
    # LIFECYCLE HOOKS
    # ---------------------------------------------------------
    def setup(self) -> None:
        """Optional initialization hook (called once)."""
        pass

    def teardown(self) -> None:
        """Optional cleanup hook."""
        pass

    # ---------------------------------------------------------
    # MAIN PROCESS METHOD
    # ---------------------------------------------------------
    def process(self, records: Iterator[Record]) -> Iterator[Record]:
        """
        Wraps the iterator and applies transformations.

        Detects whether:
        ✓ batch mode (process_batch implemented)
        ✓ record-by-record mode (transform_record implemented)
        """

        bind_trace_ids(operator_id=self.__class__.__name__)

        self.setup()

        if self._is_batch_processor():
            return self._process_in_batches(records)

        return self._process_record_by_record(records)

    # ---------------------------------------------------------
    # RECORD-BY-RECORD PROCESSING
    # ---------------------------------------------------------
    def _process_record_by_record(self, records: Iterator[Record]) -> Iterator[Record]:
        """Default mode: transform each record individually."""
        for record in records:
            try:
                result = self.transform_record(record)
            except Exception as e:
                logger.error(
                    "record_transform_failed",
                    processor=self.__class__.__name__,
                    record_preview=str(record)[:500],
                    error=str(e),
                    exc_info=True,
                )
                raise TransformError(
                    f"{self.__class__.__name__}.transform_record() failed: {e}"
                ) from e

            if result is None:
                continue

            yield result

    # ---------------------------------------------------------
    # BATCH PROCESSING
    # ---------------------------------------------------------
    def _process_in_batches(self, records: Iterator[Record], batch_size: int = 1000) -> Iterator[Record]:
        """
        If process_batch() is implemented, we accumulate records and send batches.
        """
        buffer: List[Record] = []

        for record in records:
            buffer.append(record)
            if len(buffer) >= batch_size:
                yield from self._execute_batch(buffer)
                buffer = []

        if buffer:
            yield from self._execute_batch(buffer)

    def _execute_batch(self, batch: List[Record]) -> Iterator[Record]:
        try:
            processed = self.process_batch(batch)
        except Exception as e:
            logger.error(
                "batch_transform_failed",
                processor=self.__class__.__name__,
                batch_size=len(batch),
                error=str(e),
                exc_info=True,
            )
            raise TransformError(
                f"{self.__class__.__name__}.process_batch() failed: {e}"
            ) from e

        for item in processed:
            yield item

    # ---------------------------------------------------------
    # API FOR SUBCLASSES TO OVERRIDE
    # ---------------------------------------------------------
    def transform_record(self, record: Record) -> Optional[Record]:
        """
        Override this for simple row-level transformations.

        Return:
            - a new Record
            - the same Record
            - None (to drop the record)
        """
        return record

    def process_batch(self, batch: List[Record]) -> Iterable[Record]:
        """
        Optional batch transformation mode.
        Override to process a list of records at once.
        """
        raise NotImplementedError("Batch mode not implemented for this processor")

    # ---------------------------------------------------------
    # INTROSPECTION
    # ---------------------------------------------------------
    def _is_batch_processor(self) -> bool:
        """Detect if batch processing is implemented."""
        return (
            self.process_batch.__func__
            is not BaseProcessor.process_batch.__func__
        )
