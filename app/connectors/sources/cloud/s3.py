"""AWS S3 Source Connector
"""

import io
import json
from collections.abc import Iterator
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from app.connectors.base import (
    Column, ConnectionTestResult, DataType,
    Record, Schema, SourceConnector, State, Table
)
from app.core.logging import get_logger
from app.schemas.connector_configs import S3Config

logger = get_logger(__name__)


class S3Source(SourceConnector):
    """S3 source connector
    Reads data from S3 in Parquet, JSON, CSV formats
    """

    SUPPORTED_FORMATS = ["parquet", "json", "csv", "jsonl"]

    def __init__(self, config: S3Config):
        super().__init__(config)

        self.bucket = config.bucket
        self.prefix = config.prefix.rstrip("/") if config.prefix else ""
        self.format = config.format.lower()

        if self.format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {self.format}")

        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key.get_secret_value() if config.aws_secret_access_key else None,
            region_name=config.region,
        )

        logger.debug(
            "s3_source_initialized",
            bucket=self.bucket,
            prefix=self.prefix,
            format=self.format,
        )

    # ===================================================================
    # Test Connection
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
                error_code = e.response["Error"]["Code"]

                logger.warning(
                    "s3_test_bucket_head_failed",
                    bucket=self.bucket,
                    error_code=error_code,
                )

                if error_code == "404":
                    return ConnectionTestResult(success=False, message=f"Bucket '{self.bucket}' does not exist")
                if error_code == "403":
                    return ConnectionTestResult(success=False, message=f"No access to bucket '{self.bucket}'")
                raise

            # List small prefix
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=self.prefix,
                MaxKeys=10,
            )

            file_count = response.get("KeyCount", 0)

            logger.info(
                "s3_test_connection_success",
                bucket=self.bucket,
                prefix=self.prefix,
                file_count=file_count,
            )

            return ConnectionTestResult(
                success=True,
                message="S3 access confirmed",
                metadata={"bucket": self.bucket, "prefix": self.prefix, "file_count": file_count},
            )

        except Exception as e:
            logger.error(
                "s3_test_connection_failed",
                bucket=self.bucket,
                prefix=self.prefix,
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(success=False, message=f"S3 connection failed: {e!s}")

    # ===================================================================
    # Schema Discovery
    # ===================================================================
    def discover_schema(self) -> Schema:
        logger.info(
            "s3_schema_discovery_requested",
            bucket=self.bucket,
            prefix=self.prefix,
            format=self.format,
        )

        stream_name = self.prefix.split("/")[-1] if self.prefix else "root"

        try:
            objects = self._list_objects(stream_name)

            if not objects:
                logger.info(
                    "s3_schema_no_objects_found",
                    bucket=self.bucket,
                    prefix=self.prefix,
                )
                return Schema(tables=[])

            first_key = objects[0]

            logger.debug(
                "s3_schema_reading_sample_object",
                key=first_key,
            )

            response = self.s3_client.get_object(Bucket=self.bucket, Key=first_key)
            content = response["Body"].read()

            columns = []

            if self.format == "parquet":
                buffer = io.BytesIO(content)
                table = pq.read_table(buffer)

                for field in table.schema:
                    if pa.types.is_integer(field.type):
                        col_type = DataType.INTEGER
                    elif pa.types.is_floating(field.type):
                        col_type = DataType.FLOAT
                    elif pa.types.is_boolean(field.type):
                        col_type = DataType.BOOLEAN
                    elif pa.types.is_timestamp(field.type):
                        col_type = DataType.DATETIME
                    else:
                        col_type = DataType.STRING

                    columns.append(Column(name=field.name, data_type=col_type))

            elif self.format == "csv":
                buffer = io.BytesIO(content)
                df = pd.read_csv(buffer, nrows=1)
                for col in df.columns:
                    columns.append(Column(name=col, data_type=DataType.STRING))

            elif self.format in ["json", "jsonl"]:
                columns.append(Column(name="data", data_type=DataType.JSON))

            logger.info(
                "s3_schema_discovery_completed",
                bucket=self.bucket,
                prefix=self.prefix,
                format=self.format,
                column_count=len(columns),
            )

            return Schema(
                tables=[
                    Table(
                        name=stream_name,
                        columns=columns,
                        row_count=0,
                    )
                ]
            )

        except Exception as e:
            logger.error(
                "s3_schema_discovery_failed",
                bucket=self.bucket,
                prefix=self.prefix,
                error=str(e),
                exc_info=True,
            )
            return Schema(tables=[])

    # ===================================================================
    # Read Data
    # ===================================================================
    def read(
        self,
        stream: str,
        state: State | None = None,
        query: str | None = None,
    ) -> Iterator[Record]:
        logger.info(
            "s3_read_requested",
            bucket=self.bucket,
            prefix=self.prefix,
            stream=stream,
            format=self.format,
        )

        objects = self._list_objects(stream)

        for obj_key in objects:
            logger.info(
                "s3_read_object_requested",
                bucket=self.bucket,
                key=obj_key,
            )

            yield from self._read_object(obj_key, stream)

    # ===================================================================
    # List Objects
    # ===================================================================
    def _list_objects(self, stream: str) -> list[str]:
        search_prefix = f"{self.prefix}/{stream}" if self.prefix else stream

        if stream == self.prefix.split("/")[-1]:
            search_prefix = self.prefix

        logger.debug(
            "s3_list_objects_requested",
            bucket=self.bucket,
            search_prefix=search_prefix,
        )

        paginator = self.s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket, Prefix=search_prefix)

        objects = []

        try:
            for page in pages:
                for obj in page.get("Contents", []):
                    if not obj["Key"].endswith("/"):
                        objects.append(obj["Key"])

            logger.info(
                "s3_list_objects_completed",
                bucket=self.bucket,
                prefix=search_prefix,
                object_count=len(objects),
            )

            return objects

        except Exception as e:
            logger.error(
                "s3_list_objects_failed",
                bucket=self.bucket,
                prefix=search_prefix,
                error=str(e),
                exc_info=True,
            )
            return []

    # ===================================================================
    # Read Individual Objects
    # ===================================================================
    def _read_object(self, s3_key: str, stream: str) -> Iterator[Record]:
        logger.debug(
            "s3_object_read_requested",
            key=s3_key,
        )

        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=s3_key)
            content = response["Body"].read()

            if self.format == "parquet":
                yield from self._read_parquet(content, stream)
            elif self.format == "json":
                yield from self._read_json(content, stream)
            elif self.format == "jsonl":
                yield from self._read_jsonl(content, stream)
            elif self.format == "csv":
                yield from self._read_csv(content, stream)

        except Exception as e:
            logger.error(
                "s3_object_read_failed",
                key=s3_key,
                error=str(e),
                exc_info=True,
            )
            raise

    # ===================================================================
    # Read Format Implementations
    # ===================================================================
    def _read_parquet(self, content: bytes, stream: str) -> Iterator[Record]:
        logger.debug(
            "s3_read_parquet_requested",
            stream=stream,
        )

        table = pq.read_table(io.BytesIO(content))
        df = table.to_pandas()

        for _, row in df.iterrows():
            yield Record(stream=stream, data=row.to_dict())

    def _read_json(self, content: bytes, stream: str) -> Iterator[Record]:
        logger.debug(
            "s3_read_json_requested",
            stream=stream,
        )

        data = json.loads(content.decode("utf-8"))
        if isinstance(data, list):
            for item in data:
                yield Record(stream=stream, data=item)
        else:
            yield Record(stream=stream, data=data)

    def _read_jsonl(self, content: bytes, stream: str) -> Iterator[Record]:
        logger.debug(
            "s3_read_jsonl_requested",
            stream=stream,
        )

        lines = content.decode("utf-8").split("\n")
        for line in lines:
            if line.strip():
                data = json.loads(line)
                yield Record(stream=stream, data=data)

    def _read_csv(self, content: bytes, stream: str) -> Iterator[Record]:
        logger.debug(
            "s3_read_csv_requested",
            stream=stream,
        )

        df = pd.read_csv(io.BytesIO(content))
        for _, row in df.iterrows():
            yield Record(stream=stream, data=row.to_dict())

    # ===================================================================
    # Count Records
    # ===================================================================
    def get_record_count(self, stream: str) -> int:
        logger.warning(
            "s3_record_count_requested_unoptimized",
            bucket=self.bucket,
            stream=stream,
        )

        count = sum(1 for _ in self.read(stream))

        logger.info(
            "s3_record_count_completed",
            stream=stream,
            record_count=count,
        )

        return count
