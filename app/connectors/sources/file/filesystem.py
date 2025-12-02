"""
FileSystem Source Connector — Universal ETL Compatible
------------------------------------------------------
Supports:
✓ Parquet
✓ CSV
✓ JSON
✓ JSONL
✓ Directory globbing
✓ Multi-file batching
Does NOT support:
✗ streams
✗ SQL queries
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator, Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from app.connectors.base import (
    Record,
    Schema,
    Table,
    Column,
    DataType,
    SourceConnector,
    ConnectionTestResult,
)
from app.schemas.connector_configs import FileSystemConfig
from app.core.logging import get_logger

logger = get_logger(__name__)


class FileSystemSource(SourceConnector):

    SUPPORTED_FORMATS = ["parquet", "json", "jsonl", "csv"]

    # ------------------------------------------------------------------
    # INIT
    # ------------------------------------------------------------------
    def __init__(self, config: FileSystemConfig):
        super().__init__(config)

        self.file_path: Path = Path(config.file_path)
        self.format: str = config.format.lower()
        self.glob_pattern: str = config.glob_pattern or "*"

        if self.format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported filesystem format: {self.format}")

        logger.debug(
            "filesystem_source_initialized",
            file_path=str(self.file_path),
            format=self.format,
        )

    # ------------------------------------------------------------------
    # UNIVERSAL CONNECTOR API
    # ------------------------------------------------------------------
    def supports_streams(self) -> bool:
        return False  # Files don't have table/collection concept

    def supports_query(self) -> bool:
        return False  # No SQL-like queries

    def get_stream_identifier(self, config: dict) -> str | None:
        """Filesystem always has a single logical stream."""
        return config.get("stream") or self.file_path.stem or "filesystem"

    def get_query(self, config: dict) -> None:
        """Filesystem does not support query-based extraction."""
        return None

    # ------------------------------------------------------------------
    # TEST CONNECTION
    # ------------------------------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        try:
            if not self.file_path.exists():
                return ConnectionTestResult(
                    success=False,
                    message=f"Path does not exist: {self.file_path}",
                )

            # Check read access
            if not self.file_path.is_file() and not self.file_path.is_dir():
                return ConnectionTestResult(
                    success=False,
                    message=f"Invalid path: {self.file_path}",
                )

            # Count files
            files = self._get_files()
            return ConnectionTestResult(
                success=True,
                message="Read access OK",
                metadata={
                    "files_found": len(files),
                    "format": self.format,
                    "path": str(self.file_path),
                },
            )

        except Exception as e:
            return ConnectionTestResult(success=False, message=str(e))

    # ------------------------------------------------------------------
    # FILE DISCOVERY
    # ------------------------------------------------------------------
    def _get_files(self) -> list[Path]:
        """Returns all matching files."""
        if self.file_path.is_file():
            return [self.file_path]

        if self.file_path.is_dir():
            return sorted(self.file_path.glob(self.glob_pattern))

        return []

    # ------------------------------------------------------------------
    # SCHEMA DISCOVERY
    # ------------------------------------------------------------------
    def discover_schema(self) -> Schema:
        files = self._get_files()
        if not files:
            return Schema(tables=[])

        first = files[0]
        columns = []

        try:
            if self.format == "parquet":
                table = pq.read_table(first)
                for field in table.schema:
                    columns.append(
                        Column(
                            name=field.name,
                            data_type=self._map_type(field.type),
                        )
                    )

            elif self.format == "csv":
                df = pd.read_csv(first, nrows=1)
                for col in df.columns:
                    columns.append(Column(name=col, data_type=DataType.STRING))

            elif self.format in ["json", "jsonl"]:
                columns.append(Column(name="data", data_type=DataType.JSON))

        except Exception as e:
            logger.error("filesystem_schema_discovery_failed", file=str(first), error=str(e))
            raise

        return Schema(
            tables=[Table(name="filesystem", columns=columns, row_count=0)]
        )

    # ------------------------------------------------------------------
    # TYPE MAPPING
    # ------------------------------------------------------------------
    @staticmethod
    def _map_type(pa_type) -> DataType:
        if pa.types.is_integer(pa_type):
            return DataType.INTEGER
        if pa.types.is_floating(pa_type):
            return DataType.FLOAT
        if pa.types.is_boolean(pa_type):
            return DataType.BOOLEAN
        if pa.types.is_timestamp(pa_type) or pa.types.is_date(pa_type):
            return DataType.DATETIME
        return DataType.STRING

    # ------------------------------------------------------------------
    # READ
    # ------------------------------------------------------------------
    def read(
        self,
        stream: str,
        state: Any = None,   # state not used for filesystem
        query: str = None,   # not used
    ) -> Iterator[Record]:

        files = self._get_files()

        for file_path in files:
            if self.format == "parquet":
                yield from self._read_parquet(file_path, stream)
            elif self.format == "csv":
                yield from self._read_csv(file_path, stream)
            elif self.format == "json":
                yield from self._read_json(file_path, stream)
            elif self.format == "jsonl":
                yield from self._read_jsonl(file_path, stream)

    # ------------------------------------------------------------------
    # FORMAT READERS
    # ------------------------------------------------------------------
    def _read_parquet(self, file: Path, stream: str):
        table = pq.read_table(file)
        df = table.to_pandas()
        for _, row in df.iterrows():
            yield Record(stream=stream, data=row.to_dict())

    def _read_csv(self, file: Path, stream: str):
        df = pd.read_csv(file)
        for _, row in df.iterrows():
            yield Record(stream=stream, data=row.to_dict())

    def _read_json(self, file: Path, stream: str):
        with open(file, encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            for item in data:
                yield Record(stream=stream, data=item)
        else:
            yield Record(stream=stream, data=data)

    def _read_jsonl(self, file: Path, stream: str):
        with open(file, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield Record(stream=stream, data=json.loads(line))
