from app.pipeline.processors.base import BaseProcessor
from app.connectors.base import Record

class NoOpProcessor(BaseProcessor):
    """Pass-through processor. Does nothing."""
    def transform_record(self, record: Record):
        return record