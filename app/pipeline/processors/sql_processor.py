"""
DuckDB SQL Transformer
----------------------

Allows applying arbitrary SQL transformations to the incoming record stream.

This is extremely powerful:
✓ Aggregations
✓ Joins
✓ Complex expressions
✓ Window functions
"""

import duckdb
from typing import Iterator, List
from app.pipeline.processors.base import BaseProcessor
from app.connectors.base import Record


class DuckDBProcessor(BaseProcessor):
    """Transform records using a DuckDB SQL query."""

    def __init__(self, sql_query: str):
        self.sql_query = sql_query
        self._conn = None

    def setup(self):
        self._conn = duckdb.connect()

    def teardown(self):
        if self._conn:
            self._conn.close()

    def process(self, records: Iterator[Record]) -> Iterator[Record]:
        # Convert iterator → list of dicts
        data = [rec.data for rec in records]
        if not data:
            return iter([])

        df = self._conn.execute(self.sql_query).fetchdf()

        for row in df.to_dict(orient="records"):
            yield Record(data=row)
