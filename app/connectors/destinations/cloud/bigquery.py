from collections.abc import Iterator
from typing import Any, List

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from app.connectors.base import Column, ConnectionTestResult, DestinationConnector, Record


class BigQueryDestination(DestinationConnector):
    """
    BigQuery Destination Connector.
    Connects to Google BigQuery and loads data into tables.
    """

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.project_id = config.get("project_id")
        self.dataset_id = config.get("dataset_id")
        self.table_id = config.get("table_id")
        self.write_mode = config.get("write_mode", "append") # "append", "overwrite", "truncate"
        self._client = None
        self._batch_size = config.get("batch_size", 1000)

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

    def create_stream(self, stream: str, schema: List[Column]) -> None:
        """Create BigQuery table if it doesn't exist."""
        if not self.project_id or not self.dataset_id or not stream:
            raise ValueError("Project ID, Dataset ID, and Table name (stream) must be provided.")
        
        with self:
            table_ref = self._client.dataset(self.dataset_id).table(stream)
            try:
                self._client.get_table(table_ref) # Check if table exists
            except GoogleAPIError as e:
                if e.code == 404: # Table not found, create it
                    bq_schema = []
                    for col in schema:
                        # Map DataType enum to BigQuery Standard SQL types
                        bq_type = self._map_data_type_to_bigquery_type(col.data_type)
                        bq_schema.append(bigquery.SchemaField(col.name, bq_type, mode="NULLABLE" if col.nullable else "REQUIRED"))
                    
                    table = bigquery.Table(table_ref, schema=bq_schema)
                    self._client.create_table(table)
                else:
                    raise Exception(f"Failed to check/create BigQuery table {stream}: {e}")

    def write(self, records: Iterator[Record]) -> int:
        """Write records to BigQuery table."""
        if not self.project_id or not self.dataset_id or not self.table_id:
            raise ValueError("Project ID, Dataset ID, and Table ID must be provided in config to write data.")

        with self:
            table_ref = self._client.dataset(self.dataset_id).table(self.table_id)
            table = self._client.get_table(table_ref) # Get table to ensure it exists and for schema validation

            rows_to_insert: List[dict] = []
            inserted_count = 0

            # Handle write mode
            if self.write_mode == "overwrite":
                self._client.delete_table(table_ref, not_found_ok=True)
                self.create_stream(self.table_id, []) # Recreate table (schema will be inferred on first insert)
            elif self.write_mode == "truncate":
                query = f"TRUNCATE TABLE `{self.project_id}.{self.dataset_id}.{self.table_id}`"
                self._client.query(query).result() # Run truncate query

            for record in records:
                rows_to_insert.append(record.data)
                if len(rows_to_insert) >= self._batch_size:
                    errors = self._client.insert_rows_json(table, rows_to_insert)
                    if errors:
                        print(f"Encountered errors while inserting rows: {errors}")
                    else:
                        inserted_count += len(rows_to_insert)
                    rows_to_insert = []

            if rows_to_insert:
                errors = self._client.insert_rows_json(table, rows_to_insert)
                if errors:
                    print(f"Encountered errors while inserting final batch: {errors}")
                else:
                    inserted_count += len(rows_to_insert)

            return inserted_count

    def _map_data_type_to_bigquery_type(self, data_type: Column) -> str:
        """
        Maps a DataType enum to a BigQuery Standard SQL type string.
        """
        if data_type == DataType.STRING:
            return "STRING"
        elif data_type == DataType.INTEGER:
            return "INT64"
        elif data_type == DataType.FLOAT:
            return "FLOAT64"
        elif data_type == DataType.BOOLEAN:
            return "BOOL"
        elif data_type == DataType.DATE:
            return "DATE"
        elif data_type == DataType.DATETIME:
            return "TIMESTAMP"
        elif data_type == DataType.JSON:
            return "JSON"
        elif data_type == DataType.BINARY:
            return "BYTES"
        else:
            return "STRING" # Default for unhandled types