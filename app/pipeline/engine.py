"""Pipeline execution engine with structured logging and error handling.

This module contains the core pipeline execution logic that orchestrates
data flow from source through processors to destination.
"""

from typing import Any, Optional, Iterator
from dataclasses import dataclass, field
from datetime import datetime, UTC

from app.connectors.base import DestinationConnector, SourceConnector, Record, State
from app.pipeline.processors.base import BaseProcessor
from app.core.logging import get_logger
from app.core.exceptions import (
    ConnectorError,
)

logger = get_logger(__name__)

@dataclass
class PipelineMetrics:
    """Pipeline execution metrics."""
    
    records_read: int = 0
    records_processed: int = 0
    records_filtered: int = 0
    records_written: int = 0
    records_failed: int = 0
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    completed_at: Optional[datetime] = None
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate execution duration in seconds."""
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            "records_read": self.records_read,
            "records_processed": self.records_processed,
            "records_filtered": self.records_filtered,
            "records_written": self.records_written,
            "records_failed": self.records_failed,
            "duration_seconds": self.duration_seconds,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


@dataclass
class PipelineResult:
    """Pipeline execution result."""
    
    status: str  # "success" | "failed" | "partial"
    stream: str
    metrics: PipelineMetrics
    error: Optional[str] = None
    errors: list[str] = field(default_factory=list)
    
    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary."""
        result = {
            "status": self.status,
            "stream": self.stream,
            **self.metrics.to_dict(),
        }
        
        if self.error:
            result["error"] = self.error
        
        if self.errors:
            result["errors"] = self.errors
        
        return result


class PipelineEngine:
    """Core engine to execute a data pipeline.
    
    Orchestrates data flow: Source → Processors → Destination
    
    Features:
    - Automatic schema discovery and creation
    - Batch processing for performance
    - Structured error handling and logging
    - Detailed execution metrics
    - Graceful degradation on errors
    
    Example:
        >>> engine = PipelineEngine(
        ...     source=PostgreSQLSource(config),
        ...     destination=SnowflakeDestination(config),
        ...     processors=[TransformProcessor(), ValidationProcessor()],
        ... )
        >>> result = engine.run(stream="users", batch_size=1000)
        >>> print(f"Wrote {result.metrics.records_written} records")
    """

    def __init__(
        self,
        source: SourceConnector,
        destination: DestinationConnector,
        processors: Optional[list[BaseProcessor]] = None,
    ):
        """Initialize pipeline engine.
        
        Args:
            source: Source connector instance
            destination: Destination connector instance
            processors: Optional list of data processors
        """
        self.source = source
        self.destination = destination
        self.processors = processors or []
        
        logger.info(
            "pipeline_engine_initialized",
            source_type=source.__class__.__name__,
            destination_type=destination.__class__.__name__,
            processor_count=len(self.processors),
        )

    def run(
        self,
        stream: str,
        state: Optional[dict[str, Any]] = None,
        query: Optional[str] = None,
        batch_size: int = 1000,
    ) -> PipelineResult:
        """Execute the pipeline for a specific stream.
        
        Args:
            stream: Name of the stream/table to process
            state: Optional state for incremental sync
            query: Optional custom SQL query (for SQL sources)
            batch_size: Batch size for processing (default: 1000)
            
        Returns:
            PipelineResult with execution metrics and status
            
        Raises:
            PipelineExecutionError: If pipeline execution fails critically
        """
        metrics = PipelineMetrics()
        
        logger.info(
            "pipeline_started",
            stream=stream,
            has_state=state is not None,
            has_query=query is not None,
            batch_size=batch_size,
        )

        try:
            # 1. Convert state dict to State object
            state_obj = self._prepare_state(stream, state)
            
            # 2. Initialize source reader
            source_iterator = self._initialize_source(stream, state_obj, query)
            
            # 3. Ensure destination stream exists
            self._ensure_destination_stream(stream)
            
            # 4. Process and write records
            metrics.records_written = self._process_and_write(
                stream=stream,
                source_iterator=source_iterator,
                metrics=metrics,
            )
            
            # 5. Mark completion
            metrics.completed_at = datetime.now(UTC)
            
            logger.info(
                "pipeline_completed",
                stream=stream,
                status="success",
                **metrics.to_dict(),
            )
            
            return PipelineResult(
                status="success",
                stream=stream,
                metrics=metrics,
            )

        except Exception as e:
            metrics.completed_at = datetime.now(UTC)
            
            logger.error(
                "pipeline_failed",
                stream=stream,
                error=str(e),
                error_type=e.__class__.__name__,
                **metrics.to_dict(),
                exc_info=True,
            )
            
            return PipelineResult(
                status="failed",
                stream=stream,
                metrics=metrics,
                error=str(e),
            )

    def _prepare_state(
        self,
        stream: str,
        state: Optional[dict[str, Any]],
    ) -> Optional[State]:
        """Convert state dictionary to State object.
        
        Args:
            stream: Stream name
            state: State dictionary
            
        Returns:
            State object or None
        """
        if not state:
            return None
        
        state_obj = State(
            stream=stream,
            cursor_field=state.get("cursor_field"),
            cursor_value=state.get("cursor_value"),
            metadata=state.get("metadata", {}),
        )
        
        logger.debug(
            "state_prepared",
            stream=stream,
            cursor_field=state_obj.cursor_field,
            cursor_value=state_obj.cursor_value,
        )
        
        return state_obj

    def _initialize_source(
        self,
        stream: str,
        state: Optional[State],
        query: Optional[str],
    ) -> Iterator[Record]:
        """Initialize source data reader.
        
        Args:
            stream: Stream name
            state: Optional state for incremental sync
            query: Optional custom query
            
        Returns:
            Iterator of Record objects
            
        Raises:
            ConnectorError: If source initialization fails
        """
        try:
            logger.debug(
                "initializing_source",
                stream=stream,
                has_state=state is not None,
                has_query=query is not None,
            )
            
            return self.source.read(stream=stream, state=state, query=query)
            
        except Exception as e:
            logger.error(
                "source_initialization_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            raise ConnectorError(f"Failed to initialize source: {e}") from e

    def _ensure_destination_stream(self, stream: str) -> None:
        """Ensure destination stream/table exists.
        
        Attempts to discover schema from source and create destination table.
        Gracefully handles cases where schema discovery is not supported.
        
        Args:
            stream: Stream name
        """
        try:
            logger.debug("discovering_schema", stream=stream)
            
            schema = self.source.discover_schema()
            
            # Find matching table schema
            table_schema = next(
                (table.columns for table in schema.tables if table.name == stream),
                None
            )
            
            if table_schema:
                logger.debug(
                    "creating_destination_stream",
                    stream=stream,
                    column_count=len(table_schema),
                )
                
                self.destination.create_stream(stream, table_schema)
                
                logger.info(
                    "destination_stream_ready",
                    stream=stream,
                    column_count=len(table_schema),
                )
            else:
                logger.warning(
                    "schema_not_found",
                    stream=stream,
                    available_tables=[t.name for t in schema.tables],
                )
                
        except NotImplementedError:
            logger.debug(
                "schema_discovery_not_supported",
                source_type=self.source.__class__.__name__,
            )
            
        except Exception as e:
            logger.warning(
                "stream_creation_failed",
                stream=stream,
                error=str(e),
                message="Continuing pipeline execution",
            )

    def _process_and_write(
        self,
        stream: str,
        source_iterator: Iterator[Record],
        metrics: PipelineMetrics,
    ) -> int:
        """Process records and write to destination.
        
        Args:
            stream: Stream name
            source_iterator: Iterator of source records
            metrics: Metrics object to update
            
        Returns:
            Number of records written
            
        Raises:
            ConnectorError: If writing fails critically
        """
        logger.debug(
            "processing_started",
            stream=stream,
            processor_count=len(self.processors),
        )
        
        # Apply processors and write
        processed_iterator = self._apply_processors(source_iterator, metrics)
        
        try:
            logger.info("writing_to_destination", stream=stream)
            
            records_written = self.destination.write(processed_iterator)
            
            logger.info(
                "write_completed",
                stream=stream,
                records_written=records_written,
            )
            
            return records_written
            
        except Exception as e:
            logger.error(
                "destination_write_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            raise ConnectorError(f"Failed to write to destination: {e}") from e

    def _apply_processors(
        self,
        source_iterator: Iterator[Record],
        metrics: PipelineMetrics,
    ) -> Iterator[Record]:
        """Apply processors to source records using iterator chaining.
        
        Args:
            source_iterator: Iterator of source records
            metrics: Metrics object to update
            
        Yields:
            Processed Record objects
        """
        # 1. Wrap source iterator to count reads
        def metering_iterator(iterator):
            for record in iterator:
                metrics.records_read += 1
                if metrics.records_read % 1000 == 0:
                    logger.debug(
                        "processing_progress",
                        records_read=metrics.records_read,
                    )
                yield record

        # 2. Build the chain
        current_iterator = metering_iterator(source_iterator)
        
        for processor in self.processors:
            try:
                current_iterator = processor.process(current_iterator)
            except Exception as e:
                logger.error(
                    "processor_chain_construction_failed",
                    processor=processor.__class__.__name__,
                    error=str(e),
                )
                raise

        # 3. Consume the chain and count processed
        # We iterate here to yield to the writer
        for record in current_iterator:
            metrics.records_processed += 1
            yield record

        logger.info(
            "processing_complete",
            records_read=metrics.records_read,
            records_processed=metrics.records_processed,
        )



class PipelineEngineBuilder:
    """Builder pattern for creating PipelineEngine instances.
    
    Example:
        >>> engine = (
        ...     PipelineEngineBuilder()
        ...     .with_source(PostgreSQLSource(config))
        ...     .with_destination(SnowflakeDestination(config))
        ...     .with_processor(ValidationProcessor())
        ...     .with_processor(TransformProcessor())
        ...     .build()
        ... )
    """
    
    def __init__(self):
        self._source: Optional[SourceConnector] = None
        self._destination: Optional[DestinationConnector] = None
        self._processors: list[BaseProcessor] = []
    
    def with_source(self, source: SourceConnector) -> "PipelineEngineBuilder":
        """Set the source connector."""
        self._source = source
        return self
    
    def with_destination(self, destination: DestinationConnector) -> "PipelineEngineBuilder":
        """Set the destination connector."""
        self._destination = destination
        return self
    
    def with_processor(self, processor: BaseProcessor) -> "PipelineEngineBuilder":
        """Add a processor to the pipeline."""
        self._processors.append(processor)
        return self
    
    def with_processors(self, processors: list[BaseProcessor]) -> "PipelineEngineBuilder":
        """Set multiple processors."""
        self._processors = processors
        return self
    
    def build(self) -> PipelineEngine:
        """Build the PipelineEngine instance.
        
        Returns:
            Configured PipelineEngine
            
        Raises:
            ValueError: If source or destination not configured
        """
        if not self._source:
            raise ValueError("Source connector is required")
        
        if not self._destination:
            raise ValueError("Destination connector is required")
        
        return PipelineEngine(
            source=self._source,
            destination=self._destination,
            processors=self._processors,
        )