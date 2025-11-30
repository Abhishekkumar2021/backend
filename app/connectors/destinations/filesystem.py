"""File System Destination Connector
"""

import csv
import json
import logging
import os
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import avro.datafile
import avro.io
import avro.schema
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from app.connectors.base import Column, ConnectionTestResult, DataType, DestinationConnector, Record

logger = logging.getLogger(__name__)


class FileSystemDestination(DestinationConnector):
    """File System destination connector
    Supports Parquet, JSON, CSV, Avro formats
    """

    SUPPORTED_FORMATS = ["parquet", "json", "csv", "jsonl", "avro"]

    # DataType to Avro type mapping
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

    def __init__(self, config: dict[str, Any]):
        """Initialize FileSystem destination connector

        Config format:
        {
            "base_path": "/path/to/output/directory",
            "format": "parquet",  # Options: parquet, json, csv, jsonl, avro
            "partition_by": None,  # Optional: column name to partition by
            "compression": "snappy",  # For parquet: snappy, gzip, none
            "overwrite": false  # If true, overwrite existing files
        }
        """
        super().__init__(config)
        self.base_path = Path(config["base_path"])
        self.format = config.get("format", "parquet").lower()
        self.partition_by = config.get("partition_by")
        self.compression = config.get("compression", "snappy")
        self.overwrite = config.get("overwrite", False)

        if self.format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {self.format}. Must be one of {self.SUPPORTED_FORMATS}")

        # Ensure base path exists
        self.base_path.mkdir(parents=True, exist_ok=True)

    def test_connection(self) -> ConnectionTestResult:
        """Test file system write permissions"""
        try:
            # Check if base path exists and is writable
            if not self.base_path.exists():
                return ConnectionTestResult(success=False, message=f"Base path does not exist: {self.base_path}")

            if not os.access(self.base_path, os.W_OK):
                return ConnectionTestResult(success=False, message=f"No write permission for: {self.base_path}")

            # Try creating a test file
            test_file = self.base_path / ".test_write"
            try:
                test_file.write_text("test")
                test_file.unlink()
            except Exception as e:
                return ConnectionTestResult(success=False, message=f"Cannot write to directory: {e!s}")

            return ConnectionTestResult(
                success=True,
                message="Write access confirmed",
                metadata={"base_path": str(self.base_path), "format": self.format, "compression": self.compression},
            )

        except Exception as e:
            return ConnectionTestResult(success=False, message=f"Test failed: {e!s}")

    def create_stream(self, stream: str, schema: list[Column]) -> None:
        """Create directory for stream (table)

        Args:
            stream: Table/stream name
            schema: List of Column objects

        """
        stream_path = self.base_path / stream
        stream_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory for stream: {stream_path}")

    def write(self, records: Iterator[Record]) -> int:
        """Write records to file system

        Args:
            records: Iterator of Record objects

        Returns:
            Number of records written

        """
        total_written = 0

        # Group records by stream
        stream_records: dict[str, list[dict[str, Any]]] = {}

        for record in records:
            if record.stream not in stream_records:
                stream_records[record.stream] = []
            stream_records[record.stream].append(record.data)

        # Write each stream
        for stream, data in stream_records.items():
            if not data:
                continue

            count = self._write_stream(stream, data)
            total_written += count
            logger.info(f"Wrote {count} records to {stream}")

        return total_written

    def _write_stream(self, stream: str, data: list[dict[str, Any]]) -> int:
        """Write data for a single stream"""
        stream_path = self.base_path / stream
        stream_path.mkdir(parents=True, exist_ok=True)

        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{stream}_{timestamp}.{self._get_file_extension()}"
        file_path = stream_path / filename

        # Check if file exists and overwrite is disabled
        if file_path.exists() and not self.overwrite:
            # Append counter to filename
            counter = 1
            while file_path.exists():
                filename = f"{stream}_{timestamp}_{counter}.{self._get_file_extension()}"
                file_path = stream_path / filename
                counter += 1

        # Write data based on format
        if self.format == "parquet":
            return self._write_parquet(file_path, data)
        if self.format == "json":
            return self._write_json(file_path, data)
        if self.format == "jsonl":
            return self._write_jsonl(file_path, data)
        if self.format == "csv":
            return self._write_csv(file_path, data)
        if self.format == "avro":
            return self._write_avro(file_path, data)
        raise ValueError(f"Unsupported format: {self.format}")

    def _get_file_extension(self) -> str:
        """Get file extension for format"""
        extensions = {"parquet": "parquet", "json": "json", "jsonl": "jsonl", "csv": "csv", "avro": "avro"}
        return extensions.get(self.format, "dat")

    def _write_parquet(self, file_path: Path, data: list[dict[str, Any]]) -> int:
        """Write data to Parquet format"""
        df = pd.DataFrame(data)

        # Convert to PyArrow Table for better control
        table = pa.Table.from_pandas(df)

        # Write with compression
        pq.write_table(table, file_path, compression=self.compression if self.compression != "none" else None)

        logger.info(f"Wrote Parquet file: {file_path} ({len(data)} records)")
        return len(data)

    def _write_json(self, file_path: Path, data: list[dict[str, Any]]) -> int:
        """Write data to JSON format (array of objects)"""
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)

        logger.info(f"Wrote JSON file: {file_path} ({len(data)} records)")
        return len(data)

    def _write_jsonl(self, file_path: Path, data: list[dict[str, Any]]) -> int:
        """Write data to JSON Lines format (one JSON object per line)"""
        with open(file_path, "w", encoding="utf-8") as f:
            for record in data:
                json.dump(record, f, default=str)
                f.write("\n")

        logger.info(f"Wrote JSONL file: {file_path} ({len(data)} records)")
        return len(data)

    def _write_csv(self, file_path: Path, data: list[dict[str, Any]]) -> int:
        """Write data to CSV format"""
        if not data:
            return 0

        # Get all unique keys across all records
        fieldnames = set()
        for record in data:
            fieldnames.update(record.keys())
        fieldnames = sorted(fieldnames)

        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        logger.info(f"Wrote CSV file: {file_path} ({len(data)} records)")
        return len(data)

    def _write_avro(self, file_path: Path, data: list[dict[str, Any]]) -> int:
        """Write data to Avro format"""
        if not data:
            return 0

        # Infer schema from first record
        avro_schema = self._infer_avro_schema(data[0])
        schema = avro.schema.parse(json.dumps(avro_schema))

        with open(file_path, "wb") as f:
            writer = avro.datafile.DataFileWriter(f, avro.io.DatumWriter(), schema)

            for record in data:
                # Convert values to Avro-compatible types
                avro_record = self._convert_to_avro(record)
                writer.append(avro_record)

            writer.close()

        logger.info(f"Wrote Avro file: {file_path} ({len(data)} records)")
        return len(data)

    def _infer_avro_schema(self, sample_record: dict[str, Any]) -> dict[str, Any]:
        """Infer Avro schema from a sample record"""
        fields = []

        for key, value in sample_record.items():
            avro_type = self._python_to_avro_type(value)
            fields.append({"name": key, "type": ["null", avro_type] if value is None else avro_type})

        return {"type": "record", "name": "Record", "fields": fields}

    def _python_to_avro_type(self, value: Any) -> str:
        """Convert Python value to Avro type"""
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
        if isinstance(value, (list, dict)):
            return "string"  # Serialize complex types as JSON strings
        return "string"

    def _convert_to_avro(self, record: dict[str, Any]) -> dict[str, Any]:
        """Convert record to Avro-compatible format"""
        avro_record = {}

        for key, value in record.items():
            if value is None:
                avro_record[key] = None
            elif isinstance(value, datetime):
                avro_record[key] = value.isoformat()
            elif isinstance(value, (list, dict)):
                avro_record[key] = json.dumps(value, default=str)
            else:
                avro_record[key] = value

        return avro_record
