from collections.abc import Iterator
from typing import Any, List

import snowflake.connector
from app.connectors.base import Column, ConnectionTestResult, DestinationConnector, Record
from app.connectors.utils import map_data_type_to_sql_type


class SnowflakeDestination(DestinationConnector):
    """
    Snowflake Destination Connector.
    Connects to Snowflake and loads data into tables.
    """

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.account = config.get("account")
        self.username = config.get("username")
        self.password = config.get("password")
        self.role = config.get("role")
        self.warehouse = config.get("warehouse")
        self.database = config.get("database")
        self.schema = config.get("schema")
        self.table = config.get("table") # Target table name
        self.write_mode = config.get("write_mode", "append") # "append", "overwrite"
        self._batch_size = config.get("batch_size", 1000)

    def connect(self) -> None:
        """Establish connection to Snowflake."""
        try:
            self._connection = snowflake.connector.connect(
                user=self.username,
                password=self.password,
                account=self.account,
                role=self.role,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
            )
        except Exception as e:
            raise Exception(f"Failed to connect to Snowflake: {e}")

    def disconnect(self) -> None:
        """Close connection to Snowflake."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def test_connection(self) -> ConnectionTestResult:
        """Test if connection is valid and accessible."""
        try:
            with self:
                # Attempt to execute a simple query
                self._connection.cursor().execute("SELECT 1").close()
                return ConnectionTestResult(success=True, message="Successfully connected to Snowflake.")
        except Exception as e:
            return ConnectionTestResult(success=False, message=f"Failed to connect to Snowflake: {e}")

    def create_stream(self, stream: str, schema: List[Column]) -> None:
        """Create table if it doesn't exist."""
        if not self.database or not self.schema or not stream:
            raise ValueError("Database, Schema, and Table name (stream) must be provided.")

        with self:
            cursor = self._connection.cursor()
            try:
                # Construct CREATE TABLE statement
                columns_sql = []
                for col in schema:
                    sql_type = map_data_type_to_sql_type(col.data_type)
                    nullable = "NULL" if col.nullable else "NOT NULL"
                    columns_sql.append(f"{col.name} {sql_type} {nullable}")
                
                create_table_sql = f"CREATE TABLE IF NOT EXISTS {self.database}.{self.schema}.{stream} (\n  " \
                                   + ",\n  ".join(columns_sql) + "\n)"
                cursor.execute(create_table_sql)
            finally:
                cursor.close()

    def write(self, records: Iterator[Record]) -> int:
        """Write records to Snowflake table."""
        if not self.database or not self.schema or not self.table:
            raise ValueError("Database, Schema, and Table name must be provided in config to write data.")

        with self:
            cursor = self._connection.cursor()
            inserted_count = 0
            batch: List[dict[str, Any]] = []

            for record in records:
                batch.append(record.data)
                if len(batch) >= self._batch_size:
                    inserted_count += self._insert_batch(cursor, self.table, batch)
                    batch = []

            if batch: # Insert any remaining records
                inserted_count += self._insert_batch(cursor, self.table, batch)

            self._connection.commit()
            return inserted_count
    
    def _insert_batch(self, cursor: Any, table_name: str, batch: List[dict[str, Any]]) -> int:
        """Helper to insert a batch of records into Snowflake."""
        if not batch:
            return 0
        
        # Get column names from the first record (assuming all records in batch have same schema)
        columns = list(batch[0].keys())
        columns_sql = ", ".join([f'"{c}"' for c in columns]) # Quote column names for Snowflake

        # Construct VALUES placeholders and data
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {self.database}.{self.schema}.{table_name} ({columns_sql}) VALUES ({placeholders})"

        data_to_insert = [[item.get(col) for col in columns] for item in batch]

        try:
            # Execute batch insert
            cursor.executemany(insert_sql, data_to_insert)
            return len(batch)
        except Exception as e:
            print(f"Error inserting batch into Snowflake: {e}")
            # Depending on error handling strategy, might re-raise, log, or return 0
            return 0
