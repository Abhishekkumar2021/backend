from app.pipeline.processors.base import BaseProcessor
from app.connectors.base import Record

class NoOpProcessor(BaseProcessor):
    """A processor that does nothing, effectively passing records through."""

    def transform_record(self, record: Record) -> Record:
        return record