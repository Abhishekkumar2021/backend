from app.pipeline.processors.base import BaseProcessor
from app.connectors.base import Record
from datetime import datetime, timezone

class ExampleTransformerProcessor(BaseProcessor):
    """
    An example processor that adds a timestamp to each record's data.
    """

    def transform_record(self, record: Record) -> Record:
        record.data["processed_at"] = datetime.now(timezone.utc).isoformat()
        return record
