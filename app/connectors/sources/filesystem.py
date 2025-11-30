"""File System Source Connector
"""

import json
import logging
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from app.connectors.base import Column, ConnectionTestResult, DataType, Record, Schema, SourceConnector

logger = logging.getLogger(__name__)


class FileSystemSource(SourceConnector):
    """File System source connector
    Reads data from local files: Parquet, JSON, CSV, JSON Lines
    """

    SUPPORTED_FORMATS = ["parquet", "json", "csv", "jsonl"]

    def __init__(self, config: dict[str, Any]):
        """Initialize FileSystem source connector

        Config format:
        {
            "file_path": "/path/to/file.parquet",  # or directory
            "format": "parquet",  # Auto-detect if not specified
            "glob_pattern": "*.parquet"  # Optional: for directory reading
        }
        """
        super().__init__(config)
        self.file_path = Path(config["file_path"])
        self.format = config.get("format", self._detect_format()).lower()
        self.glob_pattern = config.get("glob_pattern", "*")

        if self.format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {self.format}")

    def _detect_format(self) -> str:
        """Auto-detect file format from extension"""
        if self.file_path.is_file():
            extension = self.file_path.suffix.lower().lstrip(".")
            if extension in self.SUPPORTED_FORMATS:
                return extension
        return "parquet"  # Default

    def test_connection(self) -> ConnectionTestResult:
        """Test file system read access"""
        try:
            if not self.file_path.exists():
                return ConnectionTestResult(success=False, message=f"Path does not exist: {self.file_path}")

            if not os.access(self.file_path, os.R_OK):
                return ConnectionTestResult(success=False, message=f"No read permission for: {self.file_path}")

            # Count files
            if self.file_path.is_dir():
                files = list(self.file_path.glob(self.glob_pattern))
                file_count = len(files)
            else:
                file_count = 1

            return ConnectionTestResult(
                success=True,
                message="Read access confirmed",
                metadata={"path": str(self.file_path), "format": self.format, "file_count": file_count},
            )

        except Exception as e:
            return ConnectionTestResult(success=False, message=f"Test failed: {e!s}")

    def discover_schema(self) -> Schema:
        """Discover schema from the file(s)
        """
        files = self._get_files()
        if not files:
            return Schema(tables=[])

        first_file = files[0]
        columns = []

        try:
            if self.format == "parquet":
                table = pq.read_table(first_file)
                for field in table.schema:
                    # Basic type mapping
                    col_type = DataType.STRING
                    if pa.types.is_integer(field.type):
                        col_type = DataType.INTEGER
                    elif pa.types.is_floating(field.type):
                        col_type = DataType.FLOAT
                    elif pa.types.is_boolean(field.type):
                        col_type = DataType.BOOLEAN
                    elif pa.types.is_timestamp(field.type) or pa.types.is_date(field.type):
                        col_type = DataType.DATETIME

                    columns.append(Column(name=field.name, data_type=col_type))

            elif self.format == "csv":
                df = pd.read_csv(first_file, nrows=1)
                for col in df.columns:
                    # In CSV everything is string initially, but we could infer
                    # For now, let's default to string unless obvious
                    columns.append(Column(name=col, data_type=DataType.STRING))

            # JSON/JSONL logic is similar (read first record)
            elif self.format in ["json", "jsonl"]:
                # Simplified discovery for JSON
                columns.append(Column(name="data", data_type=DataType.JSON))

        except Exception as e:
            logger.error(f"Failed to discover schema: {e}")
            # Return empty schema on failure or just the stream name

        # We treat the file path/name as the "table"
        table_name = self.file_path.stem

        return Schema(tables=[{"name": table_name, "columns": columns}])

    def get_record_count(self, stream: str) -> int:
        """Get total rows for progress tracking
        """
        # This can be expensive for large CSV/JSON files
        # For parquet it's cheap
        files = self._get_files()
        total = 0
        for f in files:
            if self.format == "parquet":
                try:
                    meta = pq.read_metadata(f)
                    total += meta.num_rows
                except:
                    pass
            # For others, maybe too expensive to count? Return -1 or estimation?
            # Let's just return 0 for now to be safe if unknown
        return total

    def read(self, stream: str, state: dict[str, Any] = None, query: str = None) -> Iterator[Record]:
        """Read data from file(s)

        Args:
            stream: File name or pattern
            state: Incremental state (unused for files usually)
            query: Optional query filter (unused for files)

        Yields:
            Record objects

        """
        files = self._get_files()

        for file_path in files:
            logger.info(f"Reading file: {file_path}")

            if self.format == "parquet":
                yield from self._read_parquet(file_path, stream)
            elif self.format == "json":
                yield from self._read_json(file_path, stream)
            elif self.format == "jsonl":
                yield from self._read_jsonl(file_path, stream)
            elif self.format == "csv":
                yield from self._read_csv(file_path, stream)

    def _get_files(self) -> list[Path]:
        """Get list of files to read"""
        if self.file_path.is_file():
            return [self.file_path]
        if self.file_path.is_dir():
            return sorted(self.file_path.glob(self.glob_pattern))
        return []

    def _read_parquet(self, file_path: Path, stream: str) -> Iterator[Record]:
        """Read Parquet file"""
        table = pq.read_table(file_path)
        df = table.to_pandas()

        for _, row in df.iterrows():
            yield Record(stream=stream, data=row.to_dict())

    def _read_json(self, file_path: Path, stream: str) -> Iterator[Record]:
        """Read JSON file (array of objects)"""
        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            for item in data:
                yield Record(stream=stream, data=item)
        else:
            yield Record(stream=stream, data=data)

    def _read_jsonl(self, file_path: Path, stream: str) -> Iterator[Record]:
        """Read JSON Lines file"""
        with open(file_path, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    data = json.loads(line)
                    yield Record(stream=stream, data=data)

    def _read_csv(self, file_path: Path, stream: str) -> Iterator[Record]:
        """Read CSV file"""
        df = pd.read_csv(file_path)

        for _, row in df.iterrows():
            yield Record(stream=stream, data=row.to_dict())
