"""AWS S3 Source Connector
"""

import io
import json
import logging
from collections.abc import Iterator
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from app.connectors.base import Column, ConnectionTestResult, DataType, Record, Schema, SourceConnector, State, Table

logger = logging.getLogger(__name__)


class S3Source(SourceConnector):
    """S3 source connector
    Reads data from S3 in Parquet, JSON, CSV formats
    """

    SUPPORTED_FORMATS = ["parquet", "json", "csv", "jsonl"]

    def __init__(self, config: dict[str, Any]):
        """Initialize S3 source connector

        Config format:
        {
            "bucket": "my-bucket",
            "prefix": "data/",  # Optional: S3 key prefix or specific file
            "format": "parquet",
            "aws_access_key_id": "...",
            "aws_secret_access_key": "...",
            "region": "us-east-1"
        }
        """
        super().__init__(config)
        self.bucket = config["bucket"]
        self.prefix = config.get("prefix", "").rstrip("/")
        self.format = config.get("format", "parquet").lower()

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
        """Test S3 connection and read permissions"""
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

            # List objects in prefix
            response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix, MaxKeys=10)

            file_count = response.get("KeyCount", 0)

            return ConnectionTestResult(
                success=True,
                message="S3 access confirmed",
                metadata={"bucket": self.bucket, "prefix": self.prefix, "file_count": file_count},
            )

        except Exception as e:
            return ConnectionTestResult(success=False, message=f"S3 connection failed: {e!s}")

    def discover_schema(self) -> Schema:
        """Discover schema from S3 objects (infers from first object found)
        """
        # Try to list files using the prefix (which is treated as the 'stream' roughly,
        # or usually discovery is for the whole bucket/prefix root)

        # In S3, "schema discovery" is vague. We'll assume the prefix contains
        # one "table" of data.
        stream_name = self.prefix.split("/")[-1] if self.prefix else "root"

        try:
            objects = self._list_objects(stream_name)
            if not objects:
                return Schema(tables=[])

            # Read first object to infer schema
            first_key = objects[0]
            response = self.s3_client.get_object(Bucket=self.bucket, Key=first_key)
            content = response["Body"].read()

            columns = []

            if self.format == "parquet":
                buffer = io.BytesIO(content)
                table = pq.read_table(buffer)
                for field in table.schema:
                    # Map types
                    col_type = DataType.STRING
                    if pa.types.is_integer(field.type):
                        col_type = DataType.INTEGER
                    elif pa.types.is_floating(field.type):
                        col_type = DataType.FLOAT
                    elif pa.types.is_boolean(field.type):
                        col_type = DataType.BOOLEAN
                    elif pa.types.is_timestamp(field.type):
                        col_type = DataType.DATETIME

                    columns.append(Column(name=field.name, data_type=col_type))

            elif self.format == "csv":
                buffer = io.BytesIO(content)
                df = pd.read_csv(buffer, nrows=1)
                for col in df.columns:
                    columns.append(Column(name=col, data_type=DataType.STRING))

            elif self.format in ["json", "jsonl"]:
                columns.append(Column(name="data", data_type=DataType.JSON))

            return Schema(
                tables=[
                    Table(
                        name=stream_name,
                        columns=columns,
                        row_count=0,  # Unknown without listing all
                    )
                ]
            )

        except Exception as e:
            logger.error(f"Failed to discover schema from S3: {e}")
            return Schema(tables=[])

    def read(self, stream: str, state: State | None = None, query: str | None = None) -> Iterator[Record]:
        """Read data from S3

        Args:
            stream: S3 key pattern or specific file
            state: Not used for S3
            query: Not used for S3

        Yields:
            Record objects

        """
        # List all objects matching the prefix/stream
        objects = self._list_objects(stream)

        for obj_key in objects:
            logger.info(f"Reading s3://{self.bucket}/{obj_key}")
            yield from self._read_object(obj_key, stream)

    def _list_objects(self, stream: str) -> list[str]:
        """List S3 objects matching the stream pattern"""
        search_prefix = f"{self.prefix}/{stream}" if self.prefix else stream

        # If stream is empty or same as prefix, just use prefix
        if stream == self.prefix.split("/")[-1]:
            search_prefix = self.prefix

        paginator = self.s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket, Prefix=search_prefix)

        objects = []
        for page in pages:
            for obj in page.get("Contents", []):
                # Filter out directories
                if not obj["Key"].endswith("/"):
                    objects.append(obj["Key"])

        return objects

    def _read_object(self, s3_key: str, stream: str) -> Iterator[Record]:
        """Read a single S3 object"""
        # Download object
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

    def _read_parquet(self, content: bytes, stream: str) -> Iterator[Record]:
        """Read Parquet content"""
        buffer = io.BytesIO(content)
        table = pq.read_table(buffer)
        df = table.to_pandas()

        for _, row in df.iterrows():
            yield Record(stream=stream, data=row.to_dict())

    def _read_json(self, content: bytes, stream: str) -> Iterator[Record]:
        """Read JSON content"""
        data = json.loads(content.decode("utf-8"))

        if isinstance(data, list):
            for item in data:
                yield Record(stream=stream, data=item)
        else:
            yield Record(stream=stream, data=data)

    def _read_jsonl(self, content: bytes, stream: str) -> Iterator[Record]:
        """Read JSON Lines content"""
        lines = content.decode("utf-8").split("\n")

        for line in lines:
            if line.strip():
                data = json.loads(line)
                yield Record(stream=stream, data=data)

    def _read_csv(self, content: bytes, stream: str) -> Iterator[Record]:
        """Read CSV content"""
        buffer = io.BytesIO(content)
        df = pd.read_csv(buffer)

        for _, row in df.iterrows():
            yield Record(stream=stream, data=row.to_dict())

    def get_record_count(self, stream: str) -> int:
        """Get record count (not efficiently implemented for S3)"""
        logger.warning("get_record_count is not optimized for S3 - reading all data")
        count = sum(1 for _ in self.read(stream))
        return count
