"""Data Processors for Pipeline Engine
This module contains various data processing utilities that can be applied
between the source extraction and destination loading phases of a pipeline.
"""

from collections.abc import Iterator
from app.connectors.base import Record


class BaseProcessor:
    """Base class for all data processors."""

    def process(self, records: Iterator[Record]) -> Iterator[Record]:
        """
        Processes an iterator of records and yields transformed records.
        """
        for record in records:
            yield self.transform_record(record)

    def transform_record(self, record: Record) -> Record:
        """
        Applies transformations to a single record.
        This method should be overridden by concrete processor implementations.
        """
        return record
