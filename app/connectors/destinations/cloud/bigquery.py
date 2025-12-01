from collections.abc import Iterator
from typing import Any, List

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from app.connectors.base import (
    Column, ConnectionTestResult, DestinationConnector, Record, DataType
)
from app.core.logging import get_logger

logger = get_logger(__name__)


class BigQueryDestination(DestinationConnector):
    """
    BigQuery Destination Connector.
    Loads data into Google BigQuery tables.
    """

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)

        self.project_id = config.get("project_id")
        self.dataset_id = config.get("dataset_id")
        self.table_id = config.get("table_id")
        self.write_mode = config.get("write_mode", "append")
        self._batch_size = config.get("batch_size", 1000)

        self._client = None

        logger.debug(
            "bigquery_destination_initialized",
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            write_mode=self.write_mode,
            batch_size=self._batch_size,
        )

    # ===================================================================
    # Connection Handling
    # ===================================================================
    def connect(self) -> None:
        logger.info(
            "bigquery_connect_requested",
            project_id=self.project_id,
            dataset_id=self.dataset_id,
        )

        try:
            self._client = bigquery.Client(project=self.project_id)

            # Test list datasets to check permissions
            list(self._client.list_datasets(project=self.project_id, max_results=1))

            logger.info(
                "bigquery_connect_success",
                project_id=self.project_id,
            )

        except GoogleAPIError as e:
            logger.error(
                "bigquery_connect_failed_google_api",
                project_id=self.project_id,
                error=str(e),
                exc_info=True,
            )
            raise Exception(f"Failed to connect to BigQuery: {e}")

        except Exception as e:
            logger.error(
                "bigquery_connect_failed_unexpected",
                project_id=self.project_id,
                error=str(e),
                exc_info=True,
            )
            raise Exception(f"An unexpected error occurred during BigQuery connection: {e}")

    def disconnect(self) -> None:
        logger.debug("bigquery_disconnect_requested")

        if self._client:
            try:
                del self._client
                logger.debug("bigquery_disconnect_success")
            except Exception as e:
                logger.warning(
                    "bigquery_disconnect_failed",
                    error=str(e),
                    exc_info=True,
                )
            finally:
                self._client = None

    # ===================================================================
    # Test Connection
    # ===================================================================
    def test_connection(self) -> ConnectionTestResult:
        logger.info(
            "bigquery_test_connection_requested",
            project_id=self.project_id,
        )

        try:
            with self:
                return ConnectionTestResult(
                    success=True,
                    message="Successfully connected to Google BigQuery.",
                )
        except Exception as e:
            logger.error(
                "bigquery_test_connection_failed",
                project_id=self.project_id,
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(
                success=False,
                message=f"Failed to connect to Google BigQuery: {e}",
            )

    # ===================================================================
    # Create / Ensure Table Exists
    # ===================================================================
    def create_stream(self, stream: str, schema: List[Column]) -> None:
        logger.info(
            "bigquery_create_stream_requested",
            stream=stream,
            dataset=self.dataset_id,
        )

        if not self.project_id or not self.dataset_id or not stream:
            raise ValueError("Project ID, Dataset ID, and stream name are required.")

        with self:
            table_ref = self._client.dataset(self.dataset_id).table(stream)

            try:
                self._client.get_table(table_ref)

                logger.debug(
                    "bigquery_create_stream_exists",
                    stream=stream,
                )

            except GoogleAPIError as e:
                if e.code == 404:
                    logger.info(
                        "bigquery_create_stream_missing_creating",
                        stream=stream,
                    )

                    bq_schema = []
                    for col in schema:
                        bq_type = self._map_data_type_to_bigquery_type(col.data_type)
                        bq_schema.append(
                            bigquery.SchemaField(
                                col.name,
                                bq_type,
                                mode="NULLABLE" if col.nullable else "REQUIRED",
                            )
                        )

                    table = bigquery.Table(table_ref, schema=bq_schema)
                    self._client.create_table(table)

                    logger.info(
                        "bigquery_table_created",
                        stream=stream,
                        column_count=len(schema),
                    )
                else:
                    logger.error(
                        "bigquery_table_check_failed",
                        stream=stream,
                        error=str(e),
                        exc_info=True,
                    )
                    raise Exception(f"Failed to check/create BigQuery table {stream}: {e}")

    # ===================================================================
    # Write Data
    # ===================================================================
    def write(self, records: Iterator[Record]) -> int:
        logger.info(
            "bigquery_write_requested",
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            write_mode=self.write_mode,
        )

        if not self.project_id or not self.dataset_id or not self.table_id:
            raise ValueError("Project ID, Dataset ID, and table ID must be provided in config.")

        with self:
            table_ref = self._client.dataset(self.dataset_id).table(self.table_id)
            table = self._client.get_table(table_ref)

            rows_to_insert = []
            inserted_count = 0

            # Handle write modes
            if self.write_mode == "overwrite":
                logger.warning(
                    "bigquery_write_overwrite_mode",
                    table_id=self.table_id,
                )

                self._client.delete_table(table_ref, not_found_ok=True)
                self.create_stream(self.table_id, [])

            elif self.write_mode == "truncate":
                logger.warning(
                    "bigquery_write_truncate_mode",
                    table_id=self.table_id,
                )

                query = f"TRUNCATE TABLE `{self.project_id}.{self.dataset_id}.{self.table_id}`"
                self._client.query(query).result()

            # Insert in batches
            for record in records:
                rows_to_insert.append(record.data)

                if len(rows_to_insert) >= self._batch_size:
                    errors = self._client.insert_rows_json(table, rows_to_insert)

                    if errors:
                        logger.error(
                            "bigquery_write_batch_errors",
                            error=str(errors),
                        )
                    else:
                        inserted_count += len(rows_to_insert)
                        logger.debug(
                            "bigquery_write_batch_success",
                            batch_size=len(rows_to_insert),
                        )

                    rows_to_insert = []

            # Insert remaining rows
            if rows_to_insert:
                errors = self._client.insert_rows_json(table, rows_to_insert)

                if errors:
                    logger.error(
                        "bigquery_write_final_batch_errors",
                        error=str(errors),
                    )
                else:
                    inserted_count += len(rows_to_insert)
                    logger.debug(
                        "bigquery_write_final_batch_success",
                        batch_size=len(rows_to_insert),
                    )

            logger.info(
                "bigquery_write_completed",
                inserted_count=inserted_count,
            )

            return inserted_count

    # ===================================================================
    # Type Mapping Utilities
    # ===================================================================
    def _map_data_type_to_bigquery_type(self, data_type: Column) -> str:
        if data_type == DataType.STRING:
            return "STRING"
        if data_type == DataType.INTEGER:
            return "INT64"
        if data_type == DataType.FLOAT:
            return "FLOAT64"
        if data_type == DataType.BOOLEAN:
            return "BOOL"
        if data_type == DataType.DATE:
            return "DATE"
        if data_type == DataType.DATETIME:
            return "TIMESTAMP"
        if data_type == DataType.JSON:
            return "JSON"
        if data_type == DataType.BINARY:
            return "BYTES"

        return "STRING"
