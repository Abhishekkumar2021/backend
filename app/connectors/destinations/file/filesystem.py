"""
File System Destination Connector — Universal ETL Compatible
------------------------------------------------------------
Supports:
✓ JSON
✓ JSONL
✓ CSV
✓ Parquet
✓ Avro
✓ Multi-stream writing
✓ Batching for large datasets
"""

from __future__ import annotations

import csv
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import avro.schema
import avro.datafile
import avro.io

from app.connectors.base import (
    DestinationConnector,
    ConnectionTestResult,
    Record,
    Column,
)
from app.core.logging import get_logger
from app.schemas.connector_configs import FileSystemConfig
from app.connectors.base import DataType

logger = get_logger(__name__)


class FileSystemDestination(DestinationConnector):

    SUPPORTED_FORMATS = {"json", "jsonl", "csv", "parquet", "avro"}

    def __init__(self, config: FileSystemConfig):
        super().__init__(config)

        self.base_path = Path(config.file_path)
        self.format = config.format.lower()
        self.overwrite = getattr(config, "overwrite", False)
        self.partition_by = getattr(config, "partition_by", None)
        self.compression = getattr(config, "compression", "snappy")
        self.batch_size = config.batch_size

        if self.format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported filesystem format: {self.format}")

        self.base_path.mkdir(parents=True, exist_ok=True)

        logger.debug(
            "filesystem_destination_initialized",
            base_path=str(self.base_path),
            format=self.format,
            partition_by=self.partition_by,
            overwrite=self.overwrite,
            compression=self.compression,
        )

    # ------------------------------------------------------------------
    # Universal Connector API
    # ------------------------------------------------------------------
    def supports_streams(self) -> bool:
        return True

    def supports_query(self) -> bool:
        return False

    def get_stream_identifier(self, config: dict) -> str:
        return config.get("stream") or "filesystem"

    # ------------------------------------------------------------------
    # Test Connection
    # ------------------------------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        try:
            if not self.base_path.exists():
                return ConnectionTestResult(False, f"Path does not exist: {self.base_path}")

            if not self.base_path.is_dir():
                return ConnectionTestResult(False, f"Not a directory: {self.base_path}")

            test_file = self.base_path / ".fs_test"
            test_file.write_text("ok")
            test_file.unlink()

            return ConnectionTestResult(True, "Write access OK")

        except Exception as e:
            return ConnectionTestResult(False, str(e))

    # ------------------------------------------------------------------
    # Create Stream (Directory)
    # ------------------------------------------------------------------
    def create_stream(self, stream: str, schema: list[Column]) -> None:
        stream_dir = self.base_path / stream
        stream_dir.mkdir(parents=True, exist_ok=True)

        logger.info("filesystem_stream_prepared", path=str(stream_dir), stream=stream)

    # ------------------------------------------------------------------
    # Main Write Entry
    # ------------------------------------------------------------------
    def write(self, records: Iterator[Record]) -> int:
        """
        Group by stream and write file per batch.
        Avoid loading all rows into memory.
        """
        buffer: dict[str, list[dict]] = {}
        total_written = 0

        for rec in records:
            buffer.setdefault(rec.stream, []).append(rec.data)

            if len(buffer[rec.stream]) >= self.batch_size:
                total_written += self._flush_stream(rec.stream, buffer[rec.stream])
                buffer[rec.stream] = []

        # final flush
        for stream, rows in buffer.items():
            if rows:
                total_written += self._flush_stream(stream, rows)

        logger.info("filesystem_write_completed", total_written=total_written)
        return total_written

    # ------------------------------------------------------------------
    # Flush One Stream
    # ------------------------------------------------------------------
    def _flush_stream(self, stream: str, rows: list[dict]) -> int:
        if not rows:
            return 0

        stream_dir = (self.base_path / stream).resolve()
        stream_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        unique = uuid.uuid4().hex[:8]
        ext = self._ext()
        filename = f"{stream}_{timestamp}_{unique}.{ext}"

        file_path = stream_dir / filename

        logger.debug("filesystem_writing_file", stream=stream, file=str(file_path), count=len(rows))

        if self.format == "json":
            return self._write_json(file_path, rows)
        if self.format == "jsonl":
            return self._write_jsonl(file_path, rows)
        if self.format == "csv":
            return self._write_csv(file_path, rows)
        if self.format == "parquet":
            return self._write_parquet(file_path, rows)
        if self.format == "avro":
            return self._write_avro(file_path, rows)

        raise ValueError(f"Unsupported format: {self.format}")

    # ------------------------------------------------------------------
    # Format Writers
    # ------------------------------------------------------------------
    def _ext(self) -> str:
        return {
            "json": "json",
            "jsonl": "jsonl",
            "csv": "csv",
            "parquet": "parquet",
            "avro": "avro",
        }[self.format]

    def _write_json(self, path: Path, rows: list[dict]) -> int:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2, default=str)
        return len(rows)

    def _write_jsonl(self, path: Path, rows: list[dict]) -> int:
        with open(path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, default=str) + "\n")
        return len(rows)

    def _write_csv(self, path: Path, rows: list[dict]) -> int:
        fieldnames = sorted({key for row in rows for key in row})
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return len(rows)

    def _write_parquet(self, path: Path, rows: list[dict]) -> int:
        df = pd.DataFrame(rows)
        table = pa.Table.from_pandas(df)
        pq.write_table(
            table,
            path,
            compression=None if self.compression == "none" else self.compression,
        )
        return len(rows)

    # ------------------------------------------------------------------
    # Avro Writer
    # ------------------------------------------------------------------
    def _write_avro(self, path: Path, rows: list[dict]) -> int:
        schema = self._generate_avro_schema(rows[0])
        schema_obj = avro.schema.parse(json.dumps(schema))

        with open(path, "wb") as f:
            writer = avro.datafile.DataFileWriter(f, avro.io.DatumWriter(), schema_obj)
            for row in rows:
                writer.append(self._prepare_avro_row(row))
            writer.close()

        return len(rows)

    def _generate_avro_schema(self, sample: dict[str, Any]) -> dict:
        fields = []
        for key, val in sample.items():
            fields.append({"name": key, "type": self._infer_avro_type(val)})
        return {"type": "record", "name": "Record", "fields": fields}

    def _infer_avro_type(self, value: Any) -> Any:
        if value is None:
            return ["null", "string"]
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "long"
        if isinstance(value, float):
            return "double"
        if isinstance(value, (str, datetime)):
            return "string"
        if isinstance(value, (list, dict)):
            return "string"
        if isinstance(value, bytes):
            return "bytes"
        return "string"

    def _prepare_avro_row(self, row: dict[str, Any]) -> dict[str, Any]:
        out = {}
        for k, v in row.items():
            if isinstance(v, datetime):
                out[k] = v.isoformat()
            elif isinstance(v, (list, dict)):
                out[k] = json.dumps(v)
            else:
                out[k] = v
        return out
