"""
Improved BigQuery Destination Connector
Fully aligned with your other structured connectors.
Safe overwrite, truncate, append modes.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, List

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from app.connectors.base import (
    Column,
    ConnectionTestResult,
    DestinationConnector,
    Record,
    DataType,
)
from app.core.logging import get_logger
from app.schemas.connector_configs import BigQueryConfig

logger = get_logger(__name__)


class BigQueryDestination(DestinationConnector):
    """
    BigQuery Destination Connector.
    Loads data into Google BigQuery tables using streaming inserts.
    """

    MAX_BQ_BATCH = 10000  # safety limit

    def __init__(self, config: BigQueryConfig):
        super().__init__(config)

        self.project_id = config.project_id
        self.dataset_id = config.dataset_id
        self.table_id = getattr(config, "table_id", None)

        # append | overwrite | truncate
        self.write_mode = getattr(config, "write_mode", "append")
        self._batch_size = min(config.batch_size, self.MAX_BQ_BATCH)

        self._client: bigquery.Client | None = None

        logger.debug(
            "bigquery_destination_initialized",
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            write_mode=self.write_mode,
            batch_size=self._batch_size,
        )

    # ----------------------------------------------------------------------
    # Connection Handling
    # ----------------------------------------------------------------------
    def connect(self) -> None:
        if self._client:
            return

        logger.info("bigquery_connect_requested", project_id=self.project_id)

        try:
            self._client = bigquery.Client(project=self.project_id)
            # basic permission check
            list(self._client.list_datasets(project=self.project_id, max_results=1))

            logger.info("bigquery_connect_success", project_id=self.project_id)

        except Exception as e:
            logger.error(
                "bigquery_connect_failed",
                project_id=self.project_id,
                error=str(e),
                exc_info=True,
            )
            raise

    def disconnect(self) -> None:
        logger.debug("bigquery_disconnect_requested")
        self._client = None  # BigQuery client is lightweight

    # ----------------------------------------------------------------------
    # Test Connection
    # ----------------------------------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        logger.info("bigquery_test_connection_requested", project=self.project_id)

        try:
            with self:
                pass
            return ConnectionTestResult(
                success=True,
                message="Connected to BigQuery",
            )
        except Exception as e:
            logger.error("bigquery_test_connection_failed", error=str(e))
            return ConnectionTestResult(success=False, message=str(e))

    # ----------------------------------------------------------------------
    # Create Table / Stream
    # ----------------------------------------------------------------------
    def create_stream(self, stream: str, schema: List[Column]) -> None:
        logger.info(
            "bigquery_create_stream_requested",
            stream=stream,
            dataset=self.dataset_id,
        )

        if not schema:
            # Prevent BadRequest: BigQuery requires at least 1 field
            logger.debug("bigquery_create_stream_empty_schema_noop", stream=stream)
            return

        with self:
            table_ref = self._client.dataset(self.dataset_id).table(stream)

            try:
                self._client.get_table(table_ref)  # exists
                logger.debug("bigquery_stream_already_exists", stream=stream)
                return

            except GoogleAPIError:
                # does not exist → create
                pass

            # build schema fields
            fields = [
                bigquery.SchemaField(
                    col.name,
                    self._bq_type(col.data_type),
                    mode="NULLABLE" if col.nullable else "REQUIRED",
                )
                for col in schema
            ]

            table = bigquery.Table(table_ref, schema=fields)
            self._client.create_table(table)

            logger.info(
                "bigquery_stream_created",
                stream=stream,
                column_count=len(schema),
            )

    # ----------------------------------------------------------------------
    # Write data
    # ----------------------------------------------------------------------
    def write(self, records: Iterator[Record]) -> int:
        if not self.table_id:
            raise ValueError("BigQuery table_id must be provided")

        logger.info(
            "bigquery_write_requested",
            dataset=self.dataset_id,
            table=self.table_id,
            write_mode=self.write_mode,
        )

        with self:
            table_ref = self._client.dataset(self.dataset_id).table(self.table_id)
            inserted = 0

            # handle overwrite/truncate
            if self.write_mode == "overwrite":
                logger.warning("bigquery_write_mode_overwrite", table=self.table_id)
                self._client.delete_table(table_ref, not_found_ok=True)

            if self.write_mode in {"overwrite", "truncate"}:
                if self.write_mode == "truncate":
                    logger.warning("bigquery_write_mode_truncate", table=self.table_id)
                    self._client.query(
                        f"TRUNCATE TABLE `{self.project_id}.{self.dataset_id}.{self.table_id}`"
                    ).result()

                # table may have been dropped → recreate schema from first batch later
                pass

            # get existing table object (if exists)
            try:
                table = self._client.get_table(table_ref)
            except GoogleAPIError:
                table = None

            buffer: list[dict[str, Any]] = []

            for record in records:
                buffer.append(record.data)

                if len(buffer) >= self._batch_size:
                    inserted += self._insert_batch(table, table_ref, buffer)
                    buffer = []

            if buffer:
                inserted += self._insert_batch(table, table_ref, buffer)

            logger.info("bigquery_write_completed", inserted=inserted)
            return inserted

    # ----------------------------------------------------------------------
    # Insert Batch
    # ----------------------------------------------------------------------
    def _insert_batch(
        self,
        table: bigquery.Table | None,
        table_ref,
        rows: list[dict[str, Any]],
    ) -> int:
        if not rows:
            return 0

        # Lazy table creation (for overwrite)
        if not table:
            logger.info(
                "bigquery_lazy_table_create_from_batch",
                column_count=len(rows[0]),
            )
            inferred_schema = [
                Column(name=k, data_type=self._infer_data_type(v))
                for k, v in rows[0].items()
            ]
            self.create_stream(self.table_id, inferred_schema)
            table = self._client.get_table(table_ref)

        errors = self._client.insert_rows_json(table, rows)

        if errors:
            logger.error("bigquery_write_batch_errors", errors=str(errors))
            return 0

        logger.debug("bigquery_write_batch_success", count=len(rows))
        return len(rows)

    # ----------------------------------------------------------------------
    # Type Mapping
    # ----------------------------------------------------------------------
    def _bq_type(self, dt: DataType) -> str:
        mapping = {
            DataType.STRING: "STRING",
            DataType.INTEGER: "INT64",
            DataType.FLOAT: "FLOAT64",
            DataType.BOOLEAN: "BOOL",
            DataType.DATE: "DATE",
            DataType.DATETIME: "TIMESTAMP",
            DataType.BINARY: "BYTES",
            DataType.JSON: "JSON",  # modern BQ supports JSON
        }
        return mapping.get(dt, "STRING")

    def _infer_data_type(self, value: Any) -> DataType:
        if value is None:
            return DataType.NULL
        if isinstance(value, bool):
            return DataType.BOOLEAN
        if isinstance(value, int):
            return DataType.INTEGER
        if isinstance(value, float):
            return DataType.FLOAT
        if isinstance(value, (dict, list)):
            return DataType.JSON
        return DataType.STRING
