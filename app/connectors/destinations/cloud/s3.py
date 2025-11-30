"""AWS S3 Destination Connector
"""

import io
import json
import logging
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from app.connectors.base import Column, ConnectionTestResult, DestinationConnector, Record

logger = logging.getLogger(__name__)


class S3Destination(DestinationConnector):
    """S3 destination connector
    Writes data to AWS S3 in Parquet, JSON, CSV formats
    """

    SUPPORTED_FORMATS = ["parquet", "json", "csv", "jsonl"]

    def __init__(self, config: dict[str, Any]):
        """Initialize S3 destination connector

        Config format:
        {
            "bucket": "my-bucket",
            "prefix": "data/",  # Optional: S3 key prefix
            "format": "parquet",
            "aws_access_key_id": "...",
            "aws_secret_access_key": "...",
            "region": "us-east-1",
            "compression": "snappy"  # For parquet
        }
        """
        super().__init__(config)
        self.bucket = config["bucket"]
        self.prefix = config.get("prefix", "").rstrip("/")
        self.format = config.get("format", "parquet").lower()
        self.compression = config.get("compression", "snappy")

        if self.format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {self.format}")

        # Initialize S3 client
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=config.get("aws_access_key_id"),
            aws_secret_access_key=config.get("aws_secret_access_key"),
            region_name=config.get("region", "us-east-1"),
        )

    def test_connection(self) -> ConnectionTestResult:
        """Test S3 connection and write permissions"""
        try:
            # Check if bucket exists
            try:
                self.s3_client.head_bucket(Bucket=self.bucket)
            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code == "404":
                    return ConnectionTestResult(success=False, message=f"Bucket '{self.bucket}' does not exist")
                if error_code == "403":
                    return ConnectionTestResult(success=False, message=f"No access to bucket '{self.bucket}'")
                raise

            # Test write permission with a small test object
            test_key = f"{self.prefix}/.test_write" if self.prefix else ".test_write"
            self.s3_client.put_object(Bucket=self.bucket, Key=test_key, Body=b"test")

            # Clean up test object
            self.s3_client.delete_object(Bucket=self.bucket, Key=test_key)

            return ConnectionTestResult(
                success=True,
                message="S3 access confirmed",
                metadata={"bucket": self.bucket, "prefix": self.prefix, "format": self.format},
            )

        except Exception as e:
            return ConnectionTestResult(success=False, message=f"S3 connection failed: {e!s}")

    def create_stream(self, stream: str, schema: list[Column]) -> None:
        """No need to create "stream" in S3 (just a key prefix)
        """
        logger.info(f"Stream '{stream}' will be created as S3 key prefix")

    def write(self, records: Iterator[Record]) -> int:
        """Write records to S3

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
            logger.info(f"Wrote {count} records to s3://{self.bucket}/{self.prefix}/{stream}")

        return total_written

    def _write_stream(self, stream: str, data: list[dict[str, Any]]) -> int:
        """Write data for a single stream to S3"""
        # Generate S3 key with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        extension = self._get_file_extension()

        if self.prefix:
            s3_key = f"{self.prefix}/{stream}/{stream}_{timestamp}.{extension}"
        else:
            s3_key = f"{stream}/{stream}_{timestamp}.{extension}"

        # Write data based on format
        if self.format == "parquet":
            return self._write_parquet(s3_key, data)
        if self.format == "json":
            return self._write_json(s3_key, data)
        if self.format == "jsonl":
            return self._write_jsonl(s3_key, data)
        if self.format == "csv":
            return self._write_csv(s3_key, data)
        raise ValueError(f"Unsupported format: {self.format}")

    def _get_file_extension(self) -> str:
        """Get file extension for format"""
        return {"parquet": "parquet", "json": "json", "jsonl": "jsonl", "csv": "csv"}.get(self.format, "dat")

    def _write_parquet(self, s3_key: str, data: list[dict[str, Any]]) -> int:
        """Write data to S3 in Parquet format"""
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df)

        # Write to buffer
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression=self.compression if self.compression != "none" else None)

        # Upload to S3
        buffer.seek(0)
        self.s3_client.put_object(Bucket=self.bucket, Key=s3_key, Body=buffer.getvalue())

        logger.info(f"Wrote Parquet to s3://{self.bucket}/{s3_key} ({len(data)} records)")
        return len(data)

    def _write_json(self, s3_key: str, data: list[dict[str, Any]]) -> int:
        """Write data to S3 in JSON format"""
        json_data = json.dumps(data, indent=2, default=str)

        self.s3_client.put_object(Bucket=self.bucket, Key=s3_key, Body=json_data.encode("utf-8"))

        logger.info(f"Wrote JSON to s3://{self.bucket}/{s3_key} ({len(data)} records)")
        return len(data)

    def _write_jsonl(self, s3_key: str, data: list[dict[str, Any]]) -> int:
        """Write data to S3 in JSON Lines format"""
        jsonl_data = "\n".join(json.dumps(record, default=str) for record in data)

        self.s3_client.put_object(Bucket=self.bucket, Key=s3_key, Body=jsonl_data.encode("utf-8"))

        logger.info(f"Wrote JSONL to s3://{self.bucket}/{s3_key} ({len(data)} records)")
        return len(data)

    def _write_csv(self, s3_key: str, data: list[dict[str, Any]]) -> int:
        """Write data to S3 in CSV format"""
        if not data:
            return 0

        df = pd.DataFrame(data)
        csv_data = df.to_csv(index=False)

        self.s3_client.put_object(Bucket=self.bucket, Key=s3_key, Body=csv_data.encode("utf-8"))

        logger.info(f"Wrote CSV to s3://{self.bucket}/{s3_key} ({len(data)} records)")
        return len(data)
