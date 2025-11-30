from app.pipeline.processors.base import BaseProcessor
from app.connectors.base import Record
import datetime

class ExampleTransformerProcessor(BaseProcessor):
    """
    An example processor that adds a timestamp to each record's data.
    """

    def transform_record(self, record: Record) -> Record:
        record.data["processed_at"] = datetime.utcnow().isoformat()
        return record
