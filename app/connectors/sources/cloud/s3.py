"""
S3 Source Connector — improved, universal-friendly version

Features
- boto3.Session based client
- Supports parquet, csv, json, jsonl
- Batch streaming controlled by config.batch_size
- Schema discovery from a sample object
- Optional incremental sync using state.cursor_field (best-effort)
- Consistent connect/disconnect/test APIs and structured logging
"""

from __future__ import annotations
import io
import json
from collections.abc import Iterator
from typing import Any, Optional, List

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError, BotoCoreError

from app.connectors.base import (
    Column,
    ConnectionTestResult,
    DataType,
    Record,
    Schema,
    SourceConnector,
    State,
    Table,
)
from app.core.logging import get_logger
from app.schemas.connector_configs import S3Config

logger = get_logger(__name__)


def _pyarrow_type_to_datatype(field_type: pa.DataType) -> DataType:
    if pa.types.is_integer(field_type):
        return DataType.INTEGER
    if pa.types.is_floating(field_type):
        return DataType.FLOAT
    if pa.types.is_boolean(field_type):
        return DataType.BOOLEAN
    if pa.types.is_timestamp(field_type) or pa.types.is_date(field_type):
        return DataType.DATETIME
    if pa.types.is_binary(field_type):
        return DataType.BINARY
    # fallback
    return DataType.STRING


class S3Source(SourceConnector):
    """S3 source connector.

    Config fields expected (S3Config):
      - bucket
      - prefix (optional)
      - format ("parquet" | "csv" | "json" | "jsonl")
      - region (optional)
      - aws_access_key_id / aws_secret_access_key (optional)
      - batch_size (int)
      - glob_pattern (optional)
    """

    SUPPORTED_FORMATS = {"parquet", "csv", "json", "jsonl"}

    def __init__(self, config: S3Config):
        super().__init__(config)

        self.bucket: str = config.bucket
        self.prefix: str = (config.prefix or "").rstrip("/")
        self._format: str = (config.format or "parquet").lower()
        self.region: Optional[str] = getattr(config, "region", None)
        self.glob_pattern: str = getattr(config, "glob_pattern", "*")
        self.batch_size: int = int(getattr(config, "batch_size", 1000))

        if self._format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {self._format}")

        # boto3 session/client will be created on connect()
        self._session: Optional[boto3.Session] = None
        self._s3_client = None

        logger.debug(
            "s3_source_initialized",
            bucket=self.bucket,
            prefix=self.prefix,
            format=self._format,
            region=self.region,
            batch_size=self.batch_size,
        )

    # -------------------------
    # Connection management
    # -------------------------
    def connect(self) -> None:
        if self._s3_client is not None:
            return

        logger.info("s3_connect_requested", bucket=self.bucket, prefix=self.prefix)

        try:
            # Create session so credentials may come from config or environment/instance profile
            session_kwargs = {}
            # prefer explicit credentials from config if provided
            if getattr(self.config, "aws_access_key_id", None):
                session_kwargs["aws_access_key_id"] = self.config.aws_access_key_id
            if getattr(self.config, "aws_secret_access_key", None):
                session_kwargs["aws_secret_access_key"] = (
                    self.config.aws_secret_access_key.get_secret_value()
                    if hasattr(self.config.aws_secret_access_key, "get_secret_value")
                    else self.config.aws_secret_access_key
                )
            if self.region:
                session_kwargs["region_name"] = self.region

            self._session = boto3.Session(**session_kwargs) if session_kwargs else boto3.Session()
            self._s3_client = self._session.client("s3")
            logger.info("s3_connect_success", bucket=self.bucket)

        except (BotoCoreError, ClientError) as e:
            logger.error("s3_connect_failed", bucket=self.bucket, error=str(e), exc_info=True)
            raise

    def disconnect(self) -> None:
        # boto3 clients don't require explicit close, but we clear references
        if self._s3_client:
            logger.debug("s3_disconnect", bucket=self.bucket)
        self._s3_client = None
        self._session = None

    # -------------------------
    # Test connection
    # -------------------------
    def test_connection(self) -> ConnectionTestResult:
        logger.info("s3_test_connection_requested", bucket=self.bucket, prefix=self.prefix)
        try:
            self.connect()
            # quick head_bucket to verify existence / permissions
            try:
                self._s3_client.head_bucket(Bucket=self.bucket)
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                logger.warning("s3_head_bucket_failed", bucket=self.bucket, error_code=code)
                if code in ("404", "NotFound"):
                    return ConnectionTestResult(False, f"Bucket '{self.bucket}' not found")
                if code in ("403", "AccessDenied"):
                    return ConnectionTestResult(False, f"No access to bucket '{self.bucket}'")
                return ConnectionTestResult(False, f"S3 head_bucket failed: {e}")

            # list a few objects
            resp = self._s3_client.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix or "", MaxKeys=5)
            count = resp.get("KeyCount", 0)
            logger.info("s3_test_connection_success", bucket=self.bucket, sample_count=count)
            return ConnectionTestResult(True, "S3 access OK", metadata={"bucket": self.bucket, "prefix": self.prefix, "sample_count": count})

        except Exception as e:
            logger.error("s3_test_connection_failed", bucket=self.bucket, error=str(e), exc_info=True)
            return ConnectionTestResult(False, f"S3 connection failed: {e}")

        finally:
            self.disconnect()

    # -------------------------
    # Schema discovery
    # -------------------------
    def discover_schema(self) -> Schema:
        logger.info("s3_schema_discovery_requested", bucket=self.bucket, prefix=self.prefix, format=self._format)
        try:
            objects = self._list_objects(limit=50)
            if not objects:
                logger.info("s3_schema_no_objects", bucket=self.bucket, prefix=self.prefix)
                return Schema(tables=[])

            sample_key = objects[0]
            logger.debug("s3_schema_using_sample_object", key=sample_key)

            # fetch sample object bytes
            self.connect()
            resp = self._s3_client.get_object(Bucket=self.bucket, Key=sample_key)
            body = resp["Body"].read()

            columns: list[Column] = []

            if self._format == "parquet":
                # read as pyarrow table and inspect schema
                tbl = pq.read_table(io.BytesIO(body))
                for field in tbl.schema:
                    columns.append(Column(name=field.name, data_type=_pyarrow_type_to_datatype(field.type)))
            elif self._format == "csv":
                df = pd.read_csv(io.BytesIO(body), nrows=1)
                for c in df.columns:
                    columns.append(Column(name=c, data_type=DataType.STRING))
            elif self._format in ("json", "jsonl"):
                # Simplified — JSON discovery returns single JSON column OR keys from first element if array/object
                try:
                    parsed = json.loads(body.decode("utf-8"))
                    if isinstance(parsed, list) and parsed:
                        first = parsed[0]
                        if isinstance(first, dict):
                            for k, v in first.items():
                                # best guess: strings for unknowns
                                columns.append(Column(name=k, data_type=DataType.STRING))
                        else:
                            columns.append(Column(name="value", data_type=DataType.JSON))
                    elif isinstance(parsed, dict):
                        for k, v in parsed.items():
                            columns.append(Column(name=k, data_type=DataType.STRING))
                    else:
                        columns.append(Column(name="value", data_type=DataType.JSON))
                except Exception:
                    columns.append(Column(name="data", data_type=DataType.JSON))
            else:
                columns.append(Column(name="data", data_type=DataType.JSON))

            table_name = (self.prefix.split("/")[-1] if self.prefix else "s3_root")
            logger.info("s3_schema_discovery_completed", bucket=self.bucket, column_count=len(columns))
            return Schema(tables=[Table(name=table_name, columns=columns, row_count=0)])

        except Exception as e:
            logger.error("s3_schema_discovery_failed", bucket=self.bucket, error=str(e), exc_info=True)
            return Schema(tables=[])

        finally:
            self.disconnect()

    # -------------------------
    # Read (stream records)
    # -------------------------
    def read(self, stream: str, state: Optional[State] = None, query: Optional[str] = None) -> Iterator[Record]:
        """
        stream: logical name (maps to prefix segment). If None, uses self.prefix.
        state: optional State(cursor_field, cursor_value) for incremental (best-effort)
        query: not used for S3 but accepted for API compatibility
        """
        logger.info("s3_read_requested", bucket=self.bucket, prefix=self.prefix, stream=stream, format=self._format)
        self.connect()
        try:
            # determine object-key prefix to list
            prefix_to_list = self._effective_prefix(stream)

            object_keys = self._list_objects(prefix_to_list)
            if not object_keys:
                logger.info("s3_read_no_objects", bucket=self.bucket, prefix=prefix_to_list)
                return

            # iterate objects and stream records
            for key in object_keys:
                logger.debug("s3_processing_object", key=key)
                try:
                    yield from self._read_object(key, stream, state)
                except Exception as obj_err:
                    logger.error("s3_object_processing_failed", key=key, error=str(obj_err), exc_info=True)
                    # continue with next object (resilient)
                    continue

        finally:
            self.disconnect()

    # -------------------------
    # Helpers
    # -------------------------
    def _effective_prefix(self, stream: Optional[str]) -> str:
        if stream:
            # if prefix provided plus stream -> prefix/stream
            if self.prefix:
                return f"{self.prefix.rstrip('/')}/{stream.lstrip('/')}"
            return stream
        return self.prefix or ""

    def _list_objects(self, prefix: Optional[str] = None, limit: Optional[int] = None) -> List[str]:
        """List object keys under prefix. Returns list of keys (may be empty)."""
        self.connect()
        prefix = prefix or (self.prefix or "")
        logger.debug("s3_list_objects_start", bucket=self.bucket, prefix=prefix)

        paginator = self._s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

        keys: list[str] = []
        try:
            for page in page_iterator:
                contents = page.get("Contents", []) or []
                for obj in contents:
                    key = obj.get("Key")
                    if not key or key.endswith("/"):
                        continue
                    keys.append(key)
                    if limit and len(keys) >= limit:
                        logger.debug("s3_list_objects_limit_reached", count=len(keys))
                        return keys
            logger.debug("s3_list_objects_completed", found=len(keys))
            return keys
        except Exception as e:
            logger.error("s3_list_objects_failed", bucket=self.bucket, prefix=prefix, error=str(e), exc_info=True)
            return []

    def _read_object(self, key: str, stream: str, state: Optional[State]) -> Iterator[Record]:
        """Read a single object and yield Record items, applying optional incremental filter."""
        resp = self._s3_client.get_object(Bucket=self.bucket, Key=key)
        body = resp["Body"].read()

        # parquet -> use pyarrow table and yield batches for memory safety
        if self._format == "parquet":
            tbl = pq.read_table(io.BytesIO(body))
            # Iterate pyarrow record batches, convert to python dicts in batches
            for batch in tbl.to_batches():
                df = batch.to_pandas()
                for _, row in df.iterrows():
                    rec = row.to_dict()
                    if self._passes_state_filter(rec, state):
                        yield Record(stream=stream or key, data=rec)

        elif self._format == "csv":
            # use pandas to stream in chunks
            try:
                for chunk in pd.read_csv(io.BytesIO(body), chunksize=self.batch_size):
                    for _, row in chunk.iterrows():
                        rec = row.to_dict()
                        if self._passes_state_filter(rec, state):
                            yield Record(stream=stream or key, data=rec)
            except Exception:
                # fallback to read entire csv if chunked read fails
                df = pd.read_csv(io.BytesIO(body))
                for _, row in df.iterrows():
                    rec = row.to_dict()
                    if self._passes_state_filter(rec, state):
                        yield Record(stream=stream or key, data=rec)

        elif self._format in ("json", "jsonl"):
            text = body.decode("utf-8")
            if self._format == "jsonl":
                for line in text.splitlines():
                    if not line.strip():
                        continue
                    obj = json.loads(line)
                    if self._passes_state_filter(obj, state):
                        yield Record(stream=stream or key, data=obj)
            else:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    for obj in parsed:
                        if self._passes_state_filter(obj, state):
                            yield Record(stream=stream or key, data=obj)
                elif isinstance(parsed, dict):
                    # yield single dict as one record
                    if self._passes_state_filter(parsed, state):
                        yield Record(stream=stream or key, data=parsed)
                else:
                    # unknown type -> wrap as value
                    obj = {"value": parsed}
                    if self._passes_state_filter(obj, state):
                        yield Record(stream=stream or key, data=obj)
        else:
            # unknown format — yield raw bytes as single record
            yield Record(stream=stream or key, data={"raw": body})

    def _passes_state_filter(self, record: dict[str, Any], state: Optional[State]) -> bool:
        """Best-effort incremental filtering: if state present and cursor_field in record, compare."""
        if not state or not state.cursor_field:
            return True
        key = state.cursor_field
        last = state.cursor_value
        # if cursor not present in record, we can't make a decision — include it
        if key not in record or last is None:
            return True
        try:
            # simple comparable filter, assumes values are comparable (strings, nums)
            return record.get(key) > last
        except Exception:
            # if comparison fails, include record to be safe
            return True

    # -------------------------
    # Record count (expensive for object store)
    # -------------------------
    def get_record_count(self, stream: str) -> int:
        logger.warning("s3_get_record_count_unoptimized", bucket=self.bucket, stream=stream)
        # naive approach — iterate through read() and count
        count = 0
        for _ in self.read(stream=stream):
            count += 1
        logger.info("s3_record_count", bucket=self.bucket, stream=stream, count=count)
        return count
