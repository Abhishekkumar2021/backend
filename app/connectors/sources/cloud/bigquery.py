"""
BigQuery Source Connector — Consistent Universal ETL Version
"""

from __future__ import annotations
from collections.abc import Iterator
from typing import Any, Optional

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError, NotFound

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
from app.connectors.utils import map_bigquery_type_to_data_type
from app.core.logging import get_logger
from app.schemas.connector_configs import BigQueryConfig

logger = get_logger(__name__)


class BigQuerySource(SourceConnector):
    """
    BigQuery Source Connector.

    Supports:
    - direct table reads
    - custom SQL queries
    - incremental sync via replication_key
    - schema discovery
    """

    def __init__(self, config: BigQueryConfig):
        super().__init__(config)

        self.project_id = config.project_id
        self.dataset_id = config.dataset_id
        self.credentials_path = getattr(config, "credentials_path", None)

        self._client: Optional[bigquery.Client] = None

        logger.debug(
            "bigquery_source_initialized",
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            credentials_path=self.credentials_path,
        )

    # ===================================================================
    # Connection Handling
    # ===================================================================
    def connect(self) -> None:
        """Establish GCP BigQuery client."""
        if self._client is not None:
            return

        logger.info(
            "bigquery_connect_requested",
            project_id=self.project_id,
            credentials_path=self.credentials_path,
        )

        try:
            if self.credentials_path:
                self._client = bigquery.Client.from_service_account_json(
                    self.credentials_path,
                    project=self.project_id,
                )
            else:
                # Default Credentials (ADC or GCP runtime)
                self._client = bigquery.Client(project=self.project_id)

            # Smoke test
            list(self._client.list_datasets(max_results=1))

            logger.info(
                "bigquery_connect_success",
                project_id=self.project_id,
            )

        except Exception as e:
            logger.error(
                "bigquery_connect_failed",
                project_id=self.project_id,
                error=str(e),
                exc_info=True,
            )
            raise Exception(f"BigQuery connection failed: {e}")

    def disconnect(self) -> None:
        """BigQuery client uses no persistent connection → safe to clear."""
        if self._client:
            logger.debug("bigquery_disconnect_requested", project_id=self.project_id)
        self._client = None

    # ===================================================================
    # Test Connection
    # ===================================================================
    def test_connection(self) -> ConnectionTestResult:
        logger.info("bigquery_test_connection_requested", project_id=self.project_id)

        try:
            self.connect()
            # simple query
            self._client.query("SELECT 1").result()

            logger.info("bigquery_test_connection_success", project_id=self.project_id)
            return ConnectionTestResult(True, "Connected to BigQuery")

        except Exception as e:
            logger.warning(
                "bigquery_test_connection_failed",
                project_id=self.project_id,
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(False, f"BigQuery test failed: {e}")

        finally:
            self.disconnect()

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
            raise ValueError("Project ID and Dataset ID required")

        self.connect()
        tables: list[Table] = []

        try:
            dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)

            for table_item in self._client.list_tables(dataset_ref):
                table_id = table_item.table_id
                fq_table = f"{self.project_id}.{self.dataset_id}.{table_id}"

                logger.debug("bigquery_schema_table_processing", table=fq_table)

                try:
                    table_obj = self._client.get_table(fq_table)
                except NotFound:
                    logger.warning("bigquery_table_not_found", table=fq_table)
                    continue

                columns: list[Column] = []
                for field in table_obj.schema:
                    columns.append(
                        Column(
                            name=field.name,
                            data_type=map_bigquery_type_to_data_type(field.field_type),
                            nullable=field.is_nullable,
                        )
                    )

                tables.append(
                    Table(
                        name=table_id,
                        schema=self.dataset_id,
                        columns=columns,
                        row_count=table_obj.num_rows,
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
            raise e

        finally:
            self.disconnect()

    # ===================================================================
    # READ DATA
    # ===================================================================
    def read(
        self,
        stream: str,
        state: Optional[State] = None,
        query: Optional[str] = None,
    ) -> Iterator[Record]:

        logger.info(
            "bigquery_read_requested",
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            stream=stream,
            has_state=bool(state),
            has_query=bool(query),
        )

        if not self.project_id or not self.dataset_id or not stream:
            raise ValueError("Project, Dataset, and Table name required")

        self.connect()

        # CASE 1 — Custom SQL
        if query:
            sql = query
        else:
            sql = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{stream}`"

        # Incremental Sync
        if state and state.cursor_field:
            cursor_field = state.cursor_field
            cursor_value = state.cursor_value

            logger.debug(
                "bigquery_incremental_filter_applied",
                cursor_field=cursor_field,
                cursor_value=str(cursor_value),
            )

            if cursor_value is not None:
                if "WHERE" in sql.upper():
                    sql += f" AND {cursor_field} > '{cursor_value}'"
                else:
                    sql += f" WHERE {cursor_field} > '{cursor_value}'"

        logger.info("bigquery_read_query_executing", query_preview=sql[:200])

        try:
            query_job = self._client.query(sql)

            for row in query_job.result():
                record = {col: row[col] for col in row.keys()}

                logger.debug("bigquery_row_fetched", stream=stream)
                yield Record(stream=stream, data=record)

        except Exception as e:
            logger.error(
                "bigquery_read_failed",
                error=str(e),
                query_preview=sql[:200],
                exc_info=True,
            )
            raise

        finally:
            self.disconnect()

    # ===================================================================
    # Record Count
    # ===================================================================
    def get_record_count(self, stream: str) -> int:
        logger.info(
            "bigquery_record_count_requested",
            stream=stream,
            dataset_id=self.dataset_id,
        )

        if not self.project_id or not self.dataset_id or not stream:
            raise ValueError("Project, Dataset, and Table name required")

        self.connect()

        try:
            table_ref = f"{self.project_id}.{self.dataset_id}.{stream}"
            table = self._client.get_table(table_ref)

            logger.info(
                "bigquery_record_count_retrieved",
                row_count=table.num_rows,
            )

            return table.num_rows

        except Exception as e:
            logger.error(
                "bigquery_record_count_failed",
                error=str(e),
                exc_info=True,
            )
            raise

        finally:
            self.disconnect()
