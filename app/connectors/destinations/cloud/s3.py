"""
AWS S3 Destination Connector (Improved, Production-Ready)
"""

import io
import json
from datetime import datetime, timezone
from collections.abc import Iterator
from typing import Any, List, Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from app.connectors.base import Column, ConnectionTestResult, DestinationConnector, Record
from app.core.logging import get_logger
from app.schemas.connector_configs import S3Config

logger = get_logger(__name__)


class S3Destination(DestinationConnector):
    """
    AWS S3 Destination Connector
    Supports Parquet, JSON, JSONL, CSV
    """

    SUPPORTED_FORMATS = ["parquet", "json", "csv", "jsonl"]

    def __init__(self, config: S3Config):
        super().__init__(config)

        self.bucket = config.bucket
        self.prefix = config.prefix.rstrip("/") if config.prefix else ""
        self.format = config.format.lower()

        # Optional (aligning with FS connector)
        self.partition_by: Optional[List[str]] = getattr(config, "partition_by", None)
        self.compression = getattr(config, "compression", "snappy")

        if self.format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported S3 format: {self.format}")

        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=(
                config.aws_secret_access_key.get_secret_value()
                if config.aws_secret_access_key
                else None
            ),
            region_name=config.region,
        )

        logger.debug(
            "s3_destination_initialized",
            bucket=self.bucket,
            prefix=self.prefix,
            format=self.format,
            compression=self.compression,
            partition_by=self.partition_by,
        )

    # ==========================================================================
    # Test Connection
    # ==========================================================================
    def test_connection(self) -> ConnectionTestResult:
        logger.info("s3_test_connection_requested", bucket=self.bucket, prefix=self.prefix)

        try:
            # Verify bucket exists + access
            self.s3.head_bucket(Bucket=self.bucket)

            # Verify write permission
            test_key = f"{self.prefix}/.write_test" if self.prefix else ".write_test"
            self.s3.put_object(Bucket=self.bucket, Key=test_key, Body=b"ok")
            self.s3.delete_object(Bucket=self.bucket, Key=test_key)

            logger.info("s3_test_connection_success", bucket=self.bucket)

            return ConnectionTestResult(
                success=True,
                message="S3 access confirmed",
                metadata={"bucket": self.bucket, "prefix": self.prefix, "format": self.format},
            )

        except ClientError as e:
            code = e.response["Error"].get("Code")
            if code in ("403", "AccessDenied"):
                msg = f"No access to bucket '{self.bucket}'"
            elif code in ("404", "NoSuchBucket"):
                msg = f"Bucket '{self.bucket}' does not exist"
            else:
                msg = f"S3 connection error: {e}"
            return ConnectionTestResult(success=False, message=msg)

        except Exception as e:
            logger.error("s3_test_connection_failed", error=str(e), exc_info=True)
            return ConnectionTestResult(success=False, message=str(e))

    # ==========================================================================
    # Stream creation (NOOP)
    # ==========================================================================
    def create_stream(self, stream: str, schema: list[Column]) -> None:
        logger.debug("s3_create_stream_noop", stream=stream)

    # ==========================================================================
    # Write API
    # ==========================================================================
    def write(self, records: Iterator[Record]) -> int:
        logger.info(
            "s3_write_requested",
            bucket=self.bucket,
            prefix=self.prefix,
            format=self.format,
        )

        grouped: dict[str, List[dict[str, Any]]] = {}

        for record in records:
            grouped.setdefault(record.stream, []).append(record.data)

        total_written = 0

        for stream, data in grouped.items():
            if not data:
                continue

            logger.debug(
                "s3_write_stream_started",
                stream=stream,
                record_count=len(data),
            )

            count = self._write_stream(stream, data)
            total_written += count

            logger.info(
                "s3_write_stream_completed",
                stream=stream,
                written=count,
            )

        logger.info("s3_write_completed", total=total_written)
        return total_written

    # ==========================================================================
    # Internal: Write per stream
    # ==========================================================================
    def _write_stream(self, stream: str, records: List[dict[str, Any]]) -> int:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        # Build key path with optional partitioning
        partition_path = ""
        if self.partition_by:
            first = records[0]
            parts = []
            for field in self.partition_by:
                val = first.get(field)
                val = str(val).replace("/", "_").replace(" ", "_")
                parts.append(f"{field}={val}")
            partition_path = "/".join(parts)

        extension = self._get_extension()

        if self.prefix and partition_path:
            key = f"{self.prefix}/{stream}/{partition_path}/{stream}_{timestamp}.{extension}"
        elif self.prefix:
            key = f"{self.prefix}/{stream}/{stream}_{timestamp}.{extension}"
        elif partition_path:
            key = f"{stream}/{partition_path}/{stream}_{timestamp}.{extension}"
        else:
            key = f"{stream}/{stream}_{timestamp}.{extension}"

        logger.debug("s3_stream_write_key", key=key)

        if self.format == "parquet":
            return self._write_parquet(key, records)
        elif self.format == "json":
            return self._write_json(key, records)
        elif self.format == "jsonl":
            return self._write_jsonl(key, records)
        elif self.format == "csv":
            return self._write_csv(key, records)

        raise ValueError(f"Unsupported format: {self.format}")

    # ==========================================================================
    # Helpers
    # ==========================================================================
    def _get_extension(self) -> str:
        return {
            "parquet": "parquet",
            "json": "json",
            "jsonl": "jsonl",
            "csv": "csv",
        }[self.format]

    # ----------------------------------------------------------------------
    # Parquet writer
    # ----------------------------------------------------------------------
    def _write_parquet(self, key: str, data: List[dict[str, Any]]) -> int:
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df)

        buf = io.BytesIO()
        pq.write_table(
            table,
            buf,
            compression=None if self.compression == "none" else self.compression,
        )

        buf.seek(0)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=buf.getvalue())

        logger.info("s3_parquet_written", key=key, count=len(data))
        return len(data)

    # ----------------------------------------------------------------------
    # JSON writer
    # ----------------------------------------------------------------------
    def _write_json(self, key: str, data: List[dict[str, Any]]) -> int:
        body = json.dumps(data, indent=2, default=str)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body.encode("utf-8"))

        logger.info("s3_json_written", key=key, count=len(data))
        return len(data)

    # ----------------------------------------------------------------------
    # JSONL writer
    # ----------------------------------------------------------------------
    def _write_jsonl(self, key: str, data: List[dict[str, Any]]) -> int:
        lines = [json.dumps(r, default=str) for r in data]
        body = "\n".join(lines)

        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body.encode("utf-8"))

        logger.info("s3_jsonl_written", key=key, count=len(data))
        return len(data)

    # ----------------------------------------------------------------------
    # CSV writer
    # ----------------------------------------------------------------------
    def _write_csv(self, key: str, data: List[dict[str, Any]]) -> int:
        df = pd.DataFrame(data)
        body = df.to_csv(index=False)

        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body.encode("utf-8"))

        logger.info("s3_csv_written", key=key, count=len(data))
        return len(data)
