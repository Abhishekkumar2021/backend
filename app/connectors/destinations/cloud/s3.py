"""AWS S3 Destination Connector
"""

import io
import json
from collections.abc import Iterator
from datetime import datetime
from typing import Any

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
    """S3 destination connector
    Writes data to AWS S3 in Parquet, JSON, CSV formats
    """

    SUPPORTED_FORMATS = ["parquet", "json", "csv", "jsonl"]

    def __init__(self, config: S3Config):
        super().__init__(config)

        self.bucket = config.bucket
        self.prefix = config.prefix.rstrip("/") if config.prefix else ""
        self.format = config.format.lower()
        self.compression = getattr(config, "compression", "snappy")

        if self.format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {self.format}")

        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key.get_secret_value() if config.aws_secret_access_key else None,
            region_name=config.region,
        )

        logger.debug(
            "s3_destination_initialized",
            bucket=self.bucket,
            prefix=self.prefix,
            format=self.format,
        )

    # ===================================================================
    # Connection Test
    # ===================================================================
    def test_connection(self) -> ConnectionTestResult:
        logger.info(
            "s3_test_connection_requested",
            bucket=self.bucket,
            prefix=self.prefix,
        )

        try:
            # Check bucket existence
            try:
                self.s3_client.head_bucket(Bucket=self.bucket)
            except ClientError as e:
                code = e.response["Error"].get("Code")

                if code == "404":
                    logger.warning(
                        "s3_test_bucket_not_found",
                        bucket=self.bucket,
                    )
                    return ConnectionTestResult(
                        success=False,
                        message=f"Bucket '{self.bucket}' does not exist",
                    )

                if code == "403":
                    logger.warning(
                        "s3_test_permission_denied",
                        bucket=self.bucket,
                    )
                    return ConnectionTestResult(
                        success=False,
                        message=f"No access to bucket '{self.bucket}'",
                    )

                logger.error(
                    "s3_test_unexpected_error",
                    error=str(e),
                    exc_info=True,
                )
                raise

            # Test write permission
            test_key = f"{self.prefix}/.test_write" if self.prefix else ".test_write"

            self.s3_client.put_object(Bucket=self.bucket, Key=test_key, Body=b"test")
            self.s3_client.delete_object(Bucket=self.bucket, Key=test_key)

            logger.info(
                "s3_test_connection_success",
                bucket=self.bucket,
            )

            return ConnectionTestResult(
                success=True,
                message="S3 access confirmed",
                metadata={"bucket": self.bucket, "prefix": self.prefix, "format": self.format},
            )

        except Exception as e:
            logger.error(
                "s3_test_connection_failed",
                bucket=self.bucket,
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(
                success=False,
                message=f"S3 connection failed: {e}",
            )

    # ===================================================================
    # Create Stream (noop)
    # ===================================================================
    def create_stream(self, stream: str, schema: list[Column]) -> None:
        logger.info(
            "s3_create_stream",
            stream=stream,
        )

    # ===================================================================
    # Write
    # ===================================================================
    def write(self, records: Iterator[Record]) -> int:
        logger.info(
            "s3_write_requested",
            bucket=self.bucket,
            prefix=self.prefix,
            format=self.format,
        )

        total_written = 0
        stream_records: dict[str, list[dict[str, Any]]] = {}

        # Group records by stream
        for record in records:
            stream_records.setdefault(record.stream, []).append(record.data)

        # Write data per stream
        for stream, data in stream_records.items():
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
                written_count=count,
            )

        logger.info(
            "s3_write_completed",
            total_written=total_written,
        )

        return total_written

    # ===================================================================
    # Internal Stream Writer
    # ===================================================================
    def _write_stream(self, stream: str, data: list[dict[str, Any]]) -> int:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        extension = self._get_file_extension()

        if self.prefix:
            s3_key = f"{self.prefix}/{stream}/{stream}_{timestamp}.{extension}"
        else:
            s3_key = f"{stream}/{stream}_{timestamp}.{extension}"

        logger.debug(
            "s3_write_stream_file_prepared",
            stream=stream,
            s3_key=s3_key,
        )

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
        return {
            "parquet": "parquet",
            "json": "json",
            "jsonl": "jsonl",
            "csv": "csv",
        }.get(self.format, "dat")

    # ===================================================================
    # Parquet Write
    # ===================================================================
    def _write_parquet(self, s3_key: str, data: list[dict[str, Any]]) -> int:
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df)

        buffer = io.BytesIO()
        pq.write_table(
            table,
            buffer,
            compression=self.compression if self.compression != "none" else None,
        )

        buffer.seek(0)
        self.s3_client.put_object(Bucket=self.bucket, Key=s3_key, Body=buffer.getvalue())

        logger.info(
            "s3_write_parquet",
            s3_key=s3_key,
            record_count=len(data),
        )
        return len(data)

    # ===================================================================
    # JSON Write
    # ===================================================================
    def _write_json(self, s3_key: str, data: list[dict[str, Any]]) -> int:
        json_data = json.dumps(data, indent=2, default=str)
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=json_data.encode("utf-8"),
        )

        logger.info(
            "s3_write_json",
            s3_key=s3_key,
            record_count=len(data),
        )
        return len(data)

    # ===================================================================
    # JSONL Write
    # ===================================================================
    def _write_jsonl(self, s3_key: str, data: list[dict[str, Any]]) -> int:
        jsonl_data = "\n".join(json.dumps(rec, default=str) for rec in data)
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=jsonl_data.encode("utf-8"),
        )

        logger.info(
            "s3_write_jsonl",
            s3_key=s3_key,
            record_count=len(data),
        )
        return len(data)

    # ===================================================================
    # CSV Write
    # ===================================================================
    def _write_csv(self, s3_key: str, data: list[dict[str, Any]]) -> int:
        df = pd.DataFrame(data)
        csv_data = df.to_csv(index=False)

        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=csv_data.encode("utf-8"),
        )

        logger.info(
            "s3_write_csv",
            s3_key=s3_key,
            record_count=len(data),
        )
        return len(data)
