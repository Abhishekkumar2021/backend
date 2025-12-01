from collections.abc import Iterator
from typing import Any, List

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from app.connectors.base import (
    Column, ConnectionTestResult, DataType,
    Record, Schema, SourceConnector, Table
)
from app.connectors.utils import map_bigquery_type_to_data_type
from app.core.logging import get_logger
from app.schemas.connector_configs import BigQueryConfig

logger = get_logger(__name__)


class BigQuerySource(SourceConnector):
    """
    BigQuery Source Connector.
    Connects to Google BigQuery and extracts data.
    """

    def __init__(self, config: BigQueryConfig):
        super().__init__(config)
        self.project_id = config.project_id
        self.dataset_id = config.dataset_id
        self._client = None

        logger.debug(
            "bigquery_source_initialized",
            project_id=self.project_id,
            dataset_id=self.dataset_id,
        )

    # ===================================================================
    # Connection Handling
    # ===================================================================
    def connect(self) -> None:
        """Establish connection to BigQuery."""
        logger.info(
            "bigquery_connect_requested",
            project_id=self.project_id,
        )

        try:
            self._client = bigquery.Client(project=self.project_id)
            list(self._client.list_datasets(project=self.project_id, max_results=1))

            logger.info(
                "bigquery_connect_success",
                project_id=self.project_id,
            )

        except GoogleAPIError as e:
            logger.error(
                "bigquery_connect_failed_google_error",
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
        """Disconnect the BigQuery client."""
        if self._client:
            logger.debug(
                "bigquery_disconnect_requested",
                project_id=self.project_id,
            )
            self._client = None
            logger.debug(
                "bigquery_disconnected",
                project_id=self.project_id,
            )

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
                logger.info(
                    "bigquery_test_connection_success",
                    project_id=self.project_id,
                )
                return ConnectionTestResult(success=True, message="Successfully connected to Google BigQuery.")
        except Exception as e:
            logger.warning(
                "bigquery_test_connection_failed",
                project_id=self.project_id,
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(success=False, message=f"Failed to connect to Google BigQuery: {e}")

    # ===================================================================
    # Schema Discovery
    # ===================================================================
    def discover_schema(self) -> Schema:
        logger.info(
            "bigquery_schema_discovery_requested",
            project_id=self.project_id,
            dataset_id=self.dataset_id,
        )

        if not self.project_id or not self.dataset_id:
            logger.error(
                "bigquery_schema_discovery_invalid_config",
                project_id=self.project_id,
                dataset_id=self.dataset_id,
            )
            raise ValueError("Project ID and Dataset ID must be provided in config to discover schema.")

        with self:
            tables = []
            dataset_ref = self._client.dataset(self.dataset_id, project=self.project_id)

            try:
                for table_entry in self._client.list_tables(dataset_ref):
                    table_id = table_entry.table_id
                    table_full_id = f"{self.project_id}.{self.dataset_id}.{table_id}"

                    logger.debug(
                        "bigquery_schema_table_processing",
                        table_id=table_id,
                        table_full_id=table_full_id,
                    )

                    table = self._client.get_table(table_full_id)
                    columns = []

                    for field in table.schema:
                        data_type = map_bigquery_type_to_data_type(field.field_type)

                        columns.append(
                            Column(
                                name=field.name,
                                data_type=data_type,
                                nullable=field.is_nullable,
                            )
                        )

                    tables.append(
                        Table(
                            name=table_id,
                            schema=self.dataset_id,
                            columns=columns,
                            row_count=table.num_rows,
                        )
                    )

                logger.info(
                    "bigquery_schema_discovery_completed",
                    table_count=len(tables),
                )

                return Schema(tables=tables)

            except Exception as e:
                logger.error(
                    "bigquery_schema_discovery_failed",
                    error=str(e),
                    exc_info=True,
                )
                raise

    # ===================================================================
    # Data Reading
    # ===================================================================
    def read(
        self,
        stream: str,
        state: dict | None = None,
        query: str | None = None
    ) -> Iterator[Record]:
        logger.info(
            "bigquery_read_requested",
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            stream=stream,
            custom_query_provided=bool(query),
        )

        if not self.project_id or not self.dataset_id or not stream:
            logger.error(
                "bigquery_read_invalid_config",
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                stream=stream,
            )
            raise ValueError("Project ID, Dataset ID, and Table name (stream) must be provided.")

        with self:
            if query:
                final_query = query
            else:
                final_query = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{stream}`"

            # Incremental sync
            if state and self.config.get("replication_key"):
                replication_key = self.config["replication_key"]
                last_replicated_value = state.get("cursor_value")

                logger.debug(
                    "bigquery_read_incremental_applied",
                    replication_key=replication_key,
                    last_replicated_value=last_replicated_value,
                )

                if last_replicated_value:
                    if "WHERE" in final_query.upper():
                        final_query += f" AND {replication_key} > '{last_replicated_value}'"
                    else:
                        final_query += f" WHERE {replication_key} > '{last_replicated_value}'"

            logger.info(
                "bigquery_read_query_executing",
                stream=stream,
                query_preview=final_query[:250],
            )

            try:
                query_job = self._client.query(final_query)
                for row in query_job.result():
                    data = {key: row[key] for key in row.keys()}

                    logger.debug(
                        "bigquery_read_row_fetched",
                        stream=stream,
                    )

                    yield Record(stream=stream, data=data)

            except Exception as e:
                logger.error(
                    "bigquery_read_failed",
                    stream=stream,
                    error=str(e),
                    exc_info=True,
                )
                raise

    # ===================================================================
    # Record Count
    # ===================================================================
    def get_record_count(self, stream: str) -> int:
        logger.info(
            "bigquery_record_count_requested",
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            stream=stream,
        )

        if not self.project_id or not self.dataset_id or not stream:
            logger.error(
                "bigquery_record_count_invalid_config",
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                stream=stream,
            )
            raise ValueError("Project ID, Dataset ID, and Table name (stream) must be provided.")

        with self:
            table_full_id = f"{self.project_id}.{self.dataset_id}.{stream}"

            try:
                table = self._client.get_table(table_full_id)

                logger.info(
                    "bigquery_record_count_retrieved",
                    stream=stream,
                    row_count=table.num_rows,
                )

                return table.num_rows

            except Exception as e:
                logger.error(
                    "bigquery_record_count_failed",
                    stream=stream,
                    error=str(e),
                    exc_info=True,
                )
                raise
