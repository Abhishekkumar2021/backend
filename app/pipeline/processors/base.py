"""Base class for data processors."""

from typing import Optional, Iterator
from app.connectors.base import Record


class BaseProcessor:
    """Base class for all data processors.
    
    Processors are responsible for transforming, filtering, or validating
    records as they move from source to destination.
    
    The architecture relies on Iterator chaining.
    """

    def process(self, records: Iterator[Record]) -> Iterator[Record]:
        """
        Process a stream of records.
        
        Args:
            records: Iterator of input records
            
        Yields:
            Transformed records.
        """
        for record in records:
            result = self.transform_record(record)
            if result is not None:
                yield result

    def transform_record(self, record: Record) -> Optional[Record]:
        """
        Process a single record. Override this for simple row-level transformations.
        
        Args:
            record: The input record to process
            
        Returns:
            The transformed record, or None if the record should be dropped.
        """
        return record
