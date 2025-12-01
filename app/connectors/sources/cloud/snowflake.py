from collections.abc import Iterator
from typing import Any, List

import snowflake.connector

from app.connectors.base import (
    Column, ConnectionTestResult, DataType,
    Record, Schema, SourceConnector, Table
)
from app.connectors.utils import map_sql_type_to_data_type
from app.core.logging import get_logger
from app.schemas.connector_configs import SnowflakeConfig

logger = get_logger(__name__)


class SnowflakeSource(SourceConnector):
    """
    Snowflake Source Connector.
    Connects to Snowflake and extracts data.
    """

    def __init__(self, config: SnowflakeConfig):
        super().__init__(config)

        self.account = config.account
        self.username = config.username
        self.password = config.password.get_secret_value()
        self.role = config.role
        self.warehouse = config.warehouse
        self.database = config.database
        self.schema = config.schema_

        self._connection = None

        logger.debug(
            "snowflake_source_initialized",
            account=self.account,
            database=self.database,
            schema=self.schema,
            warehouse=self.warehouse,
            role=self.role,
        )

    # ===================================================================
    # Connection Handling
    # ===================================================================
    def connect(self) -> None:
        """Establish connection to Snowflake."""
        logger.info(
            "snowflake_connect_requested",
            account=self.account,
            database=self.database,
            schema=self.schema,
        )

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

            logger.info(
                "snowflake_connect_success",
                account=self.account,
                database=self.database,
                schema=self.schema,
            )

        except Exception as e:
            logger.error(
                "snowflake_connect_failed",
                account=self.account,
                database=self.database,
                schema=self.schema,
                error=str(e),
                exc_info=True,
            )
            raise Exception(f"Failed to connect to Snowflake: {e}")

    def disconnect(self) -> None:
        """Close connection to Snowflake."""
        if self._connection:
            logger.debug(
                "snowflake_disconnect_requested",
                account=self.account,
            )
            self._connection.close()
            self._connection = None
            logger.debug(
                "snowflake_disconnected",
                account=self.account,
            )

    # ===================================================================
    # Test Connection
    # ===================================================================
    def test_connection(self) -> ConnectionTestResult:
        logger.info(
            "snowflake_test_connection_requested",
            account=self.account,
            database=self.database,
        )

        try:
            with self:
                self._connection.cursor().execute("SELECT 1").close()
            logger.info(
                "snowflake_test_connection_success",
                account=self.account,
            )
            return ConnectionTestResult(success=True, message="Successfully connected to Snowflake.")

        except Exception as e:
            logger.warning(
                "snowflake_test_connection_failed",
                account=self.account,
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(success=False, message=f"Failed to connect to Snowflake: {e}")

    # ===================================================================
    # Schema Discovery
    # ===================================================================
    def discover_schema(self) -> Schema:
        logger.info(
            "snowflake_schema_discovery_requested",
            database=self.database,
            schema=self.schema,
        )

        if not self.database or not self.schema:
            logger.error(
                "snowflake_schema_discovery_invalid_config",
                database=self.database,
                schema=self.schema,
            )
            raise ValueError("Database and Schema must be provided in config to discover schema.")

        with self:
            try:
                cursor = self._connection.cursor()

                tables_meta = []
                cursor.execute(f"SHOW TABLES IN {self.database}.{self.schema}")

                for row in cursor:
                    table_name = row[1]
                    table_comment = row[7] if len(row) > 7 else None
                    tables_meta.append({
                        "name": table_name,
                        "comment": table_comment,
                    })

                logger.debug(
                    "snowflake_schema_table_list_fetched",
                    table_count=len(tables_meta),
                )

                tables = []
                for meta in tables_meta:
                    table_name = meta["name"]

                    logger.debug(
                        "snowflake_schema_table_columns_request",
                        table_name=table_name,
                    )

                    cursor.execute(f"DESCRIBE TABLE {self.database}.{self.schema}.{table_name}")

                    columns = []
                    for col_row in cursor:
                        col_name = col_row[0]
                        col_type = col_row[1]
                        col_nullable = col_row[3] == "Y"
                        mapped = map_sql_type_to_data_type(col_type)

                        columns.append(
                            Column(
                                name=col_name,
                                data_type=mapped,
                                nullable=col_nullable,
                            )
                        )

                    tables.append(
                        Table(
                            name=table_name,
                            schema=self.schema,
                            columns=columns,
                            description=meta["comment"],
                        )
                    )

                logger.info(
                    "snowflake_schema_discovery_completed",
                    table_count=len(tables),
                )

                return Schema(tables=tables)

            except Exception as e:
                logger.error(
                    "snowflake_schema_discovery_failed",
                    error=str(e),
                    exc_info=True,
                )
                return Schema(tables=[])

    # ===================================================================
    # Read Data
    # ===================================================================
    def read(
        self,
        stream: str,
        state: dict | None = None,
        query: str | None = None
    ) -> Iterator[Record]:
        logger.info(
            "snowflake_read_requested",
            database=self.database,
            schema=self.schema,
            stream=stream,
            custom_query_provided=bool(query),
        )

        if not self.database or not self.schema or not stream:
            logger.error(
                "snowflake_read_invalid_config",
                database=self.database,
                schema=self.schema,
                stream=stream,
            )
            raise ValueError("Database, Schema, and Table name (stream) must be provided.")

        with self:
            try:
                cursor = self._connection.cursor()

                if query:
                    final_query = query
                else:
                    final_query = f"SELECT * FROM {self.database}.{self.schema}.{stream}"

                if state and self.config.get("replication_key"):
                    replication_key = self.config["replication_key"]
                    last_value = state.get("cursor_value")

                    logger.debug(
                        "snowflake_read_incremental_state_applied",
                        replication_key=replication_key,
                        last_value=last_value,
                    )

                    if last_value:
                        if "WHERE" in final_query.upper():
                            final_query += f" AND {replication_key} > '{last_value}'"
                        else:
                            final_query += f" WHERE {replication_key} > '{last_value}'"

                logger.info(
                    "snowflake_read_query_executing",
                    query_preview=final_query[:250],
                )

                cursor.execute(final_query)
                column_names = [col[0] for col in cursor.description]

                for row in cursor:
                    logger.debug(
                        "snowflake_row_fetched",
                        stream=stream,
                    )
                    yield Record(stream=stream, data=dict(zip(column_names, row)))

            except Exception as e:
                logger.error(
                    "snowflake_read_failed",
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
            "snowflake_record_count_requested",
            database=self.database,
            schema=self.schema,
            stream=stream,
        )

        if not self.database or not self.schema or not stream:
            logger.error(
                "snowflake_record_count_invalid_config",
                database=self.database,
                schema=self.schema,
                stream=stream,
            )
            raise ValueError("Database, Schema, and Table name (stream) must be provided.")

        with self:
            try:
                cursor = self._connection.cursor()
                cursor.execute(
                    f"SELECT COUNT(*) FROM {self.database}.{self.schema}.{stream}"
                )

                count = cursor.fetchone()[0]

                logger.info(
                    "snowflake_record_count_retrieved",
                    stream=stream,
                    record_count=count,
                )

                return count

            except Exception as e:
                logger.error(
                    "snowflake_record_count_failed",
                    stream=stream,
                    error=str(e),
                    exc_info=True,
                )
                raise
