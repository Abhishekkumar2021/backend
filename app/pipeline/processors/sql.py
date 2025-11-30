from app.pipeline.processors.base import BaseProcessor
from app.connectors.base import Record
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any
import duckdb # Import duckdb
import pandas as pd # DuckDB works well with Pandas

from app.connectors.base import Record

@dataclass
class DuckDBProcessor(BaseProcessor):
    """
    A processor that applies SQL transformations using DuckDB.
    Expects a 'sql_query' in its configuration.
    The SQL query should select from a table named 'input_data'.
    """
    sql_query: str
    batch_size: int = 1000

    def process(self, records: Iterator[Record]) -> Iterator[Record]:
        """
        Processes records in batches using DuckDB for SQL transformations.
        """
        batch = []
        for record in records:
            batch.append(record.data) # Collect raw data dicts
            if len(batch) >= self.batch_size:
                yield from self._process_batch(batch, record.stream) # Pass stream for new records
                batch = []
        if batch:
            yield from self._process_batch(batch, record.stream) # Process any remaining records

    def _process_batch(self, data_batch: list[dict[str, Any]], stream_name: str) -> Iterator[Record]:
        """
        Loads a batch into DuckDB, executes the query, and yields results.
        """
        if not data_batch:
            return

        # Create an in-memory DuckDB connection
        con = duckdb.connect(database=':memory:', read_only=False)

        try:
            # Create a Pandas DataFrame from the batch
            df = pd.DataFrame(data_batch)

            # Register the DataFrame as a DuckDB view
            con.create_view("input_data", df)

            # Execute the SQL query
            result_df = con.execute(self.sql_query).fetchdf()

            # Convert result DataFrame back to Records
            for _, row in result_df.iterrows():
                yield Record(stream=stream_name, data=row.to_dict())

        finally:
            con.close()