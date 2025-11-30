from collections.abc import Iterator
from typing import Any, List

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from app.connectors.base import Column, ConnectionTestResult, DataType, Record, Schema, SourceConnector, Table
from app.connectors.utils import map_bigquery_type_to_data_type


class BigQuerySource(SourceConnector):
    """
    BigQuery Source Connector.
    Connects to Google BigQuery and extracts data.
    """

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.project_id = config.get("project_id")
        self.dataset_id = config.get("dataset_id")
        # For simplicity, we assume application default credentials or
        # credentials configured via GOOGLE_APPLICATION_CREDENTIALS env var.
        # In a real app, explicit key file path might be provided.
        self._client = None

    def connect(self) -> None:
        """Establish connection to BigQuery."""
        try:
            self._client = bigquery.Client(project=self.project_id)
            # Test connection by listing datasets
            list(self._client.list_datasets(project=self.project_id, max_results=1))
        except GoogleAPIError as e:
            raise Exception(f"Failed to connect to BigQuery: {e}")
        except Exception as e:
            raise Exception(f"An unexpected error occurred during BigQuery connection: {e}")

    def disconnect(self) -> None:
        """BigQuery client typically manages its own connections."""
        if self._client:
            del self._client
            self._client = None

    def test_connection(self) -> ConnectionTestResult:
        """Test if connection is valid and accessible."""
        try:
            with self: # Use context manager for connection test
                return ConnectionTestResult(success=True, message="Successfully connected to Google BigQuery.")
        except Exception as e:
            return ConnectionTestResult(success=False, message=f"Failed to connect to Google BigQuery: {e}")

    def discover_schema(self) -> Schema:
        """Discover and return schema metadata."""
        if not self.project_id or not self.dataset_id:
            raise ValueError("Project ID and Dataset ID must be provided in config to discover schema.")

        with self:
            tables = []
            dataset_ref = self._client.dataset(self.dataset_id, project=self.project_id)

            for table_entry in self._client.list_tables(dataset_ref):
                table_id = table_entry.table_id
                table_full_id = f"{self.project_id}.{self.dataset_id}.{table_id}"
                
                table = self._client.get_table(table_full_id)
                columns = []

                for field in table.schema:
                    data_type = map_bigquery_type_to_data_type(field.field_type)
                    columns.append(Column(name=field.name, data_type=data_type, nullable=field.is_nullable))
                
                tables.append(Table(name=table_id, schema=self.dataset_id, columns=columns, row_count=table.num_rows))
            
            return Schema(tables=tables)

    def read(self, stream: str, state: dict | None = None, query: str | None = None) -> Iterator[Record]:
        """Read data from BigQuery table."""
        if not self.project_id or not self.dataset_id or not stream:
            raise ValueError("Project ID, Dataset ID, and Table name (stream) must be provided in config to read data.")

        with self:
            if query:
                # If a custom query is provided, use it directly
                final_query = query
            else:
                # Otherwise, construct a default SELECT query
                final_query = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{stream}`"

            # Apply state for incremental sync
            if state and self.config.get("replication_key"):
                replication_key = self.config["replication_key"]
                last_replicated_value = state.get("cursor_value")
                if last_replicated_value:
                    if "WHERE" in final_query.upper():
                        final_query += f" AND {replication_key} > '{last_replicated_value}'"
                    else:
                        final_query += f" WHERE {replication_key} > '{last_replicated_value}'"
            
            query_job = self._client.query(final_query)
            for row in query_job.result():
                # Convert Row to dict
                data = {key: row[key] for key in row.keys()}
                yield Record(stream=stream, data=data)

    def get_record_count(self, stream: str) -> int:
        """Get total record count for a stream."""
        if not self.project_id or not self.dataset_id or not stream:
            raise ValueError("Project ID, Dataset ID, and Table name (stream) must be provided.")
        with self:
            table_full_id = f"{self.project_id}.{self.dataset_id}.{stream}"
            table = self._client.get_table(table_full_id)
            return table.num_rows
