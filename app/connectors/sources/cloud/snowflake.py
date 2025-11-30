from collections.abc import Iterator
from typing import Any, List

import snowflake.connector
from app.connectors.base import Column, ConnectionTestResult, DataType, Record, Schema, SourceConnector, Table
from app.connectors.utils import map_sql_type_to_data_type


class SnowflakeSource(SourceConnector):
    """
    Snowflake Source Connector.
    Connects to Snowflake and extracts data.
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

    def discover_schema(self) -> Schema:
        """Discover and return schema metadata."""
        if not self.database or not self.schema:
            raise ValueError("Database and Schema must be provided in config to discover schema.")

        with self:
            cursor = self._connection.cursor()
            tables_meta = []

            # Get tables
            cursor.execute(f"SHOW TABLES IN {self.database}.{self.schema}")
            for row in cursor:
                table_name = row[1]
                table_comment = row[7] if len(row) > 7 else None # COMMENT column
                tables_meta.append({'name': table_name, 'comment': table_comment})

            tables = []
            for table_meta in tables_meta:
                table_name = table_meta['name']
                columns = []
                
                # Get columns for each table
                cursor.execute(f"DESCRIBE TABLE {self.database}.{self.schema}.{table_name}")
                for col_row in cursor:
                    col_name = col_row[0]
                    col_type = col_row[1]
                    col_nullable = col_row[3] == 'Y'

                    data_type = map_sql_type_to_data_type(col_type)
                    columns.append(Column(name=col_name, data_type=data_type, nullable=col_nullable))
                tables.append(Table(name=table_name, schema=self.schema, columns=columns, description=table_meta['comment']))
            
            return Schema(tables=tables)

    def read(self, stream: str, state: dict | None = None, query: str | None = None) -> Iterator[Record]:
        """Read data from Snowflake table."""
        if not self.database or not self.schema or not stream:
            raise ValueError("Database, Schema, and Table name (stream) must be provided in config to read data.")

        with self:
            cursor = self._connection.cursor()
            
            # Use query if provided, otherwise select all from stream
            if query:
                full_query = query
            else:
                full_query = f"SELECT * FROM {self.database}.{self.schema}.{stream}"

            # Apply state for incremental sync
            if state and self.config.get("replication_key"):
                replication_key = self.config["replication_key"]
                last_replicated_value = state.get("cursor_value")
                if last_replicated_value:
                    if "WHERE" in full_query.upper():
                        full_query += f" AND {replication_key} > '{last_replicated_value}'"
                    else:
                        full_query += f" WHERE {replication_key} > '{last_replicated_value}'"
            
            cursor.execute(full_query)
            
            column_names = [col[0] for col in cursor.description]

            for row in cursor:
                data = dict(zip(column_names, row))
                yield Record(stream=stream, data=data)

    def get_record_count(self, stream: str) -> int:
        """Get total record count for a stream."""
        if not self.database or not self.schema or not stream:
            raise ValueError("Database, Schema, and Table name (stream) must be provided.")
        with self:
            cursor = self._connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {self.database}.{self.schema}.{stream}")
            return cursor.fetchone()[0]