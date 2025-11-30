import logging
from typing import Any, Optional, Iterator

from app.connectors.base import DestinationConnector, SourceConnector, Record, State
from app.pipeline.processors import BaseProcessor
from app.utils.logging import get_logger

logger = get_logger(__name__)


class PipelineEngine:
    """Core engine to execute a data pipeline.
    Reads from Source -> Applies Processors -> Writes to Destination.
    """

    def __init__(
        self,
        source: SourceConnector,
        destination: DestinationConnector,
        processors: Optional[list[BaseProcessor]] = None,
    ):
        self.source = source
        self.destination = destination
        self.processors = processors or []
        logger.info(
            "Pipeline engine initialized with %d processor(s)",
            len(self.processors)
        )

    def run(
        self,
        stream: str,
        state: Optional[dict[str, Any]] = None,
        query: Optional[str] = None,
        batch_size: int = 1000,
    ) -> dict[str, Any]:
        """Run the pipeline for a specific stream.
        
        Args:
            stream: Name of the stream/table to process
            state: Optional state for incremental sync
            query: Optional custom query
            batch_size: Batch size for processing
            
        Returns:
            Dictionary with execution results
        """
        logger.info("Starting pipeline run for stream: %s", stream)
        
        records_written = 0
        records_processed = 0
        records_filtered = 0
        errors = []

        try:
            # 1. Initialize Source Stream
            logger.debug("Reading from source: stream=%s, has_state=%s", stream, state is not None)
            
            # Convert state dict to State object if provided
            state_obj = None
            if state:
                state_obj = State(
                    stream=stream,
                    cursor_field=state.get("cursor_field"),
                    cursor_value=state.get("cursor_value"),
                    metadata=state.get("metadata", {}),
                )
            
            source_iterator = self.source.read(stream=stream, state=state_obj, query=query)

            # 2. Ensure destination stream exists
            self._ensure_destination_stream(stream)

            # 3. Create Processing Iterator
            logger.debug("Applying %d processor(s) to records", len(self.processors))
            processed_iterator = self._processing_generator(source_iterator)

            # 4. Write to Destination
            logger.info("Writing records to destination")
            records_written = self.destination.write(processed_iterator)

            logger.info(
                "✅ Pipeline completed successfully: stream=%s, records_written=%d",
                stream,
                records_written,
            )

            return {
                "status": "success",
                "stream": stream,
                "records_written": records_written,
                "records_processed": records_processed,
                "records_filtered": records_filtered,
            }

        except Exception as e:
            logger.exception("❌ Pipeline execution failed for stream: %s", stream)
            return {
                "status": "failed",
                "stream": stream,
                "records_written": records_written,
                "error": str(e),
            }

    def _ensure_destination_stream(self, stream: str) -> None:
        """Ensure destination stream/table exists.
        
        Args:
            stream: Stream name
        """
        try:
            logger.debug("Discovering schema for stream: %s", stream)
            schema = self.source.discover_schema()
            
            # Find the relevant table schema
            table_schema = next(
                (t.columns for t in schema.tables if t.name == stream),
                None
            )
            
            if table_schema:
                logger.debug("Creating destination stream with %d columns", len(table_schema))
                self.destination.create_stream(stream, table_schema)
                logger.info("✅ Destination stream ready: %s", stream)
            else:
                logger.warning("No schema found for stream: %s", stream)
                
        except NotImplementedError:
            logger.debug("Schema discovery not supported for this connector")
        except Exception as e:
            logger.warning("Could not auto-create stream: %s", e)

    def _processing_generator(self, source_iterator: Iterator[Record]) -> Iterator[Record]:
        """Generator that pulls from source and applies processors.
        
        Args:
            source_iterator: Iterator of Record objects from source
            
        Yields:
            Processed Record objects
        """
        count = 0
        filtered_count = 0

        for record in source_iterator:
            count += 1
            
            if count % 1000 == 0:
                logger.debug("Processed %d records so far", count)
            
            current_record = record

            # Apply all processors in chain
            should_yield = True
            for processor in self.processors:
                try:
                    current_record = processor.process(current_record)
                    
                    if current_record is None:
                        should_yield = False
                        filtered_count += 1
                        logger.debug("Record filtered by processor: %s", processor.__class__.__name__)
                        break
                        
                except Exception as e:
                    logger.error(
                        "Error in processor %s for record: %s",
                        processor.__class__.__name__,
                        e,
                        exc_info=True
                    )
                    should_yield = False
                    filtered_count += 1
                    break

            if should_yield:
                yield current_record

        logger.info(
            "Processing complete: total=%d, yielded=%d, filtered=%d",
            count,
            count - filtered_count,
            filtered_count,
        )