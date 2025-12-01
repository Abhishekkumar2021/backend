"""File System Source Connector
"""

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from app.connectors.base import (
    Column, ConnectionTestResult, DataType,
    Record, Schema, SourceConnector, Table
)
from app.core.logging import get_logger

logger = get_logger(__name__)


class FileSystemSource(SourceConnector):
    """File System source connector
    Reads data from local files: Parquet, JSON, CSV, JSON Lines
    """

    SUPPORTED_FORMATS = ["parquet", "json", "csv", "jsonl"]

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)

        self.file_path = Path(config["file_path"])
        self.format = config.get("format", self._detect_format()).lower()
        self.glob_pattern = config.get("glob_pattern", "*")

        if self.format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {self.format}")

        logger.debug(
            "filesystem_source_initialized",
            file_path=str(self.file_path),
            format=self.format,
            glob_pattern=self.glob_pattern,
        )

    # ===================================================================
    # Format Detection
    # ===================================================================
    def _detect_format(self) -> str:
        """Auto-detect file format from extension."""
        if self.file_path.is_file():
            ext = self.file_path.suffix.lower().lstrip(".")
            if ext in self.SUPPORTED_FORMATS:
                return ext
        return "parquet"

    # ===================================================================
    # Test Connection
    # ===================================================================
    def test_connection(self) -> ConnectionTestResult:
        logger.info(
            "filesystem_test_connection_requested",
            path=str(self.file_path),
        )

        try:
            if not self.file_path.exists():
                logger.warning(
                    "filesystem_test_path_not_found",
                    path=str(self.file_path),
                )
                return ConnectionTestResult(
                    success=False,
                    message=f"Path does not exist: {self.file_path}",
                )

            if not os.access(self.file_path, os.R_OK):
                logger.warning(
                    "filesystem_test_no_read_access",
                    path=str(self.file_path),
                )
                return ConnectionTestResult(
                    success=False,
                    message=f"No read permission for: {self.file_path}",
                )

            # Count files
            if self.file_path.is_dir():
                files = list(self.file_path.glob(self.glob_pattern))
                file_count = len(files)
            else:
                file_count = 1

            logger.info(
                "filesystem_test_connection_success",
                path=str(self.file_path),
                file_count=file_count,
            )

            return ConnectionTestResult(
                success=True,
                message="Read access confirmed",
                metadata={
                    "path": str(self.file_path),
                    "format": self.format,
                    "file_count": file_count,
                },
            )

        except Exception as e:
            logger.error(
                "filesystem_test_connection_failed",
                path=str(self.file_path),
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(success=False, message=f"Test failed: {e}")

    # ===================================================================
    # Schema Discovery
    # ===================================================================
    def discover_schema(self) -> Schema:
        logger.info(
            "filesystem_schema_discovery_requested",
            path=str(self.file_path),
            format=self.format,
        )

        files = self._get_files()

        if not files:
            logger.info(
                "filesystem_schema_no_files_found",
                path=str(self.file_path),
            )
            return Schema(tables=[])

        first_file = files[0]
        columns = []

        try:
            if self.format == "parquet":
                table = pq.read_table(first_file)
                for field in table.schema:
                    if pa.types.is_integer(field.type):
                        col_type = DataType.INTEGER
                    elif pa.types.is_floating(field.type):
                        col_type = DataType.FLOAT
                    elif pa.types.is_boolean(field.type):
                        col_type = DataType.BOOLEAN
                    elif pa.types.is_timestamp(field.type) or pa.types.is_date(field.type):
                        col_type = DataType.DATETIME
                    else:
                        col_type = DataType.STRING

                    columns.append(Column(name=field.name, data_type=col_type))

            elif self.format == "csv":
                df = pd.read_csv(first_file, nrows=1)
                for col in df.columns:
                    columns.append(Column(name=col, data_type=DataType.STRING))

            elif self.format in ["json", "jsonl"]:
                # Simplified JSON schema: a single "data" column
                columns.append(Column(name="data", data_type=DataType.JSON))

            logger.info(
                "filesystem_schema_discovery_completed",
                file=str(first_file),
                column_count=len(columns),
            )

        except Exception as e:
            logger.error(
                "filesystem_schema_discovery_failed",
                file=str(first_file),
                error=str(e),
                exc_info=True,
            )

        table_name = self.file_path.stem
        return Schema(
            tables=[
                Table(
                    name=table_name,
                    columns=columns,
                    row_count=0,
                )
            ]
        )

    # ===================================================================
    # Record Count
    # ===================================================================
    def get_record_count(self, stream: str) -> int:
        logger.info(
            "filesystem_record_count_requested",
            path=str(self.file_path),
            format=self.format,
        )

        files = self._get_files()
        total = 0

        for f in files:
            if self.format == "parquet":
                try:
                    meta = pq.read_metadata(f)
                    total += meta.num_rows
                except Exception:
                    logger.warning(
                        "filesystem_record_count_metadata_failed",
                        file=str(f),
                        exc_info=True,
                    )
                    pass

        logger.info(
            "filesystem_record_count_completed",
            record_count=total,
        )

        return total

    # ===================================================================
    # Read Data
    # ===================================================================
    def read(
        self,
        stream: str,
        state: dict[str, Any] = None,
        query: str = None,
    ) -> Iterator[Record]:
        logger.info(
            "filesystem_read_requested",
            path=str(self.file_path),
            format=self.format,
            stream=stream,
        )

        files = self._get_files()

        for file_path in files:
            logger.info(
                "filesystem_file_read_requested",
                file=str(file_path),
            )

            if self.format == "parquet":
                yield from self._read_parquet(file_path, stream)
            elif self.format == "json":
                yield from self._read_json(file_path, stream)
            elif self.format == "jsonl":
                yield from self._read_jsonl(file_path, stream)
            elif self.format == "csv":
                yield from self._read_csv(file_path, stream)

    # ===================================================================
    # File Discovery
    # ===================================================================
    def _get_files(self) -> list[Path]:
        if self.file_path.is_file():
            logger.debug(
                "filesystem_get_files_single_file",
                file=str(self.file_path),
            )
            return [self.file_path]

        if self.file_path.is_dir():
            files = sorted(self.file_path.glob(self.glob_pattern))
            logger.debug(
                "filesystem_get_files_directory",
                directory=str(self.file_path),
                file_count=len(files),
            )
            return files

        logger.warning(
            "filesystem_get_files_invalid_path",
            path=str(self.file_path),
        )
        return []

    # ===================================================================
    # Read Formats
    # ===================================================================
    def _read_parquet(self, file_path: Path, stream: str) -> Iterator[Record]:
        logger.debug(
            "filesystem_read_parquet_requested",
            file=str(file_path),
        )

        table = pq.read_table(file_path)
        df = table.to_pandas()

        for _, row in df.iterrows():
            yield Record(stream=stream, data=row.to_dict())

    def _read_json(self, file_path: Path, stream: str) -> Iterator[Record]:
        logger.debug(
            "filesystem_read_json_requested",
            file=str(file_path),
        )

        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            for item in data:
                yield Record(stream=stream, data=item)
        else:
            yield Record(stream=stream, data=data)

    def _read_jsonl(self, file_path: Path, stream: str) -> Iterator[Record]:
        logger.debug(
            "filesystem_read_jsonl_requested",
            file=str(file_path),
        )

        with open(file_path, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    data = json.loads(line)
                    yield Record(stream=stream, data=data)

    def _read_csv(self, file_path: Path, stream: str) -> Iterator[Record]:
        logger.debug(
            "filesystem_read_csv_requested",
            file=str(file_path),
        )

        df = pd.read_csv(file_path)
        for _, row in df.iterrows():
            yield Record(stream=stream, data=row.to_dict())
