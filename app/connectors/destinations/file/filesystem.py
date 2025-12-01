"""File System Destination Connector (with Structured Logging)"""

import csv
import json
from datetime import datetime
import os
from pathlib import Path
from typing import Any
from collections.abc import Iterator

import avro.datafile
import avro.io
import avro.schema
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from app.connectors.base import Column, ConnectionTestResult, DataType, DestinationConnector, Record
from app.core.logging import get_logger
from app.schemas.connector_configs import FileSystemConfig

logger = get_logger(__name__)


class FileSystemDestination(DestinationConnector):
    """File System destination connector
    Supports Parquet, JSON, CSV, JSONL, Avro formats
    """

    SUPPORTED_FORMATS = ["parquet", "json", "csv", "jsonl", "avro"]

    AVRO_TYPE_MAPPING = {
        DataType.INTEGER: "long",
        DataType.FLOAT: "double",
        DataType.STRING: "string",
        DataType.BOOLEAN: "boolean",
        DataType.DATE: "string",
        DataType.DATETIME: "string",
        DataType.JSON: "string",
        DataType.BINARY: "bytes",
        DataType.NULL: ["null", "string"],
    }

    def __init__(self, config: FileSystemConfig):
        super().__init__(config)
        self.base_path = Path(config.file_path)
        self.format = config.format.lower()
        
        # Extra fields
        self.partition_by = getattr(config, "partition_by", None)
        self.compression = getattr(config, "compression", "snappy")
        self.overwrite = getattr(config, "overwrite", False)

        logger.debug(
            "fs_destination_initialized",
            base_path=str(self.base_path),
            format=self.format,
            compression=self.compression,
            overwrite=self.overwrite,
            partition_by=self.partition_by,
        )

        if self.format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {self.format}")

        self.base_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Test Connection
    # ------------------------------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        logger.info(
            "fs_test_connection_started",
            base_path=str(self.base_path)
        )
        try:
            if not self.base_path.exists():
                return ConnectionTestResult(
                    success=False,
                    message=f"Base path does not exist: {self.base_path}"
                )

            if not os.access(self.base_path, os.W_OK):
                return ConnectionTestResult(
                    success=False,
                    message=f"No write permission: {self.base_path}"
                )

            test_file = self.base_path / ".test_write"
            try:
                test_file.write_text("test")
                test_file.unlink()
            except Exception as e:
                logger.error(
                    "fs_test_write_failed",
                    error=str(e),
                    exc_info=True
                )
                return ConnectionTestResult(
                    success=False,
                    message=f"Cannot write to directory: {e}"
                )

            logger.info("fs_test_connection_success", base_path=str(self.base_path))

            return ConnectionTestResult(
                success=True,
                message="File system write access confirmed",
                metadata={
                    "base_path": str(self.base_path),
                    "format": self.format,
                    "compression": self.compression,
                },
            )

        except Exception as e:
            logger.error("fs_test_connection_failed", error=str(e), exc_info=True)
            return ConnectionTestResult(success=False, message=str(e))

    # ------------------------------------------------------------------
    # Create Stream
    # ------------------------------------------------------------------
    def create_stream(self, stream: str, schema: list[Column]) -> None:
        stream_path = self.base_path / stream
        stream_path.mkdir(parents=True, exist_ok=True)

        logger.info(
            "fs_stream_created",
            stream=stream,
            path=str(stream_path)
        )

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------
    def write(self, records: Iterator[Record]) -> int:
        logger.info("fs_write_started")

        total_written = 0
        grouped: dict[str, list[dict[str, Any]]] = {}

        for record in records:
            grouped.setdefault(record.stream, []).append(record.data)

        for stream, data in grouped.items():
            if not data:
                continue

            count = self._write_stream(stream, data)
            total_written += count

            logger.info(
                "fs_stream_write_completed",
                stream=stream,
                count=count
            )

        logger.info("fs_write_completed", total_written=total_written)
        return total_written

    # ------------------------------------------------------------------
    # Write Stream
    # ------------------------------------------------------------------
    def _write_stream(self, stream: str, data: list[dict[str, Any]]) -> int:
        stream_dir = self.base_path / stream
        stream_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        ext = self._get_file_extension()
        file_path = stream_dir / f"{stream}_{timestamp}.{ext}"

        if file_path.exists() and not self.overwrite:
            counter = 1
            while file_path.exists():
                file_path = stream_dir / f"{stream}_{timestamp}_{counter}.{ext}"
                counter += 1

        logger.debug(
            "fs_stream_writing",
            path=str(file_path),
            stream=stream,
            records=len(data),
            format=self.format
        )

        try:
            if self.format == "parquet":
                return self._write_parquet(file_path, data)
            elif self.format == "json":
                return self._write_json(file_path, data)
            elif self.format == "jsonl":
                return self._write_jsonl(file_path, data)
            elif self.format == "csv":
                return self._write_csv(file_path, data)
            elif self.format == "avro":
                return self._write_avro(file_path, data)
        except Exception as e:
            logger.error(
                "fs_stream_write_failed",
                stream=stream,
                error=str(e),
                exc_info=True
            )
            raise

        raise ValueError(f"Unsupported format: {self.format}")

    # ------------------------------------------------------------------
    # Format Handlers
    # ------------------------------------------------------------------
    def _get_file_extension(self) -> str:
        return {
            "parquet": "parquet",
            "json": "json",
            "jsonl": "jsonl",
            "csv": "csv",
            "avro": "avro",
        }.get(self.format, "dat")

    def _write_parquet(self, file_path: Path, data: list[dict[str, Any]]) -> int:
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path, compression=None if self.compression == "none" else self.compression)

        logger.info("fs_parquet_written", path=str(file_path), records=len(data))
        return len(data)

    def _write_json(self, file_path: Path, data: list[dict[str, Any]]) -> int:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)

        logger.info("fs_json_written", path=str(file_path), records=len(data))
        return len(data)

    def _write_jsonl(self, file_path: Path, data: list[dict[str, Any]]) -> int:
        with open(file_path, "w", encoding="utf-8") as f:
            for row in data:
                json.dump(row, f, default=str)
                f.write("\n")

        logger.info("fs_jsonl_written", path=str(file_path), records=len(data))
        return len(data)

    def _write_csv(self, file_path: Path, data: list[dict[str, Any]]) -> int:
        if not data:
            return 0

        fieldnames = sorted({key for row in data for key in row.keys()})

        with open(file_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        logger.info("fs_csv_written", path=str(file_path), records=len(data))
        return len(data)

    def _write_avro(self, file_path: Path, data: list[dict[str, Any]]) -> int:
        if not data:
            return 0

        avro_schema = self._infer_avro_schema(data[0])
        schema_obj = avro.schema.parse(json.dumps(avro_schema))

        with open(file_path, "wb") as f:
            writer = avro.datafile.DataFileWriter(f, avro.io.DatumWriter(), schema_obj)
            for row in data:
                writer.append(self._convert_to_avro(row))
            writer.close()

        logger.info("fs_avro_written", path=str(file_path), records=len(data))
        return len(data)

    # ------------------------------------------------------------------
    # Avro Helpers
    # ------------------------------------------------------------------
    def _infer_avro_schema(self, sample: dict[str, Any]) -> dict[str, Any]:
        fields = []
        for key, value in sample.items():
            avro_type = self._python_to_avro_type(value)
            fields.append({"name": key, "type": ["null", avro_type] if value is None else avro_type})
        return {"type": "record", "name": "Record", "fields": fields}

    def _python_to_avro_type(self, value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "long"
        if isinstance(value, float):
            return "double"
        if isinstance(value, (str, datetime)):
            return "string"
        if isinstance(value, bytes):
            return "bytes"
        if isinstance(value, (dict, list)):
            return "string"
        return "string"

    def _convert_to_avro(self, record: dict[str, Any]) -> dict[str, Any]:
        converted = {}
        for key, value in record.items():
            if value is None:
                converted[key] = None
            elif isinstance(value, datetime):
                converted[key] = value.isoformat()
            elif isinstance(value, (list, dict)):
                converted[key] = json.dumps(value, default=str)
            else:
                converted[key] = value
        return converted
