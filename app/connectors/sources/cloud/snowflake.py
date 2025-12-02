"""
Universal ETL — Snowflake Source Connector
------------------------------------------
✓ Supports table (stream) OR custom SQL query
✓ Uses incremental sync if state.cursor_field is provided
✓ Streams results with batch fetching
✓ Discovers schema using SHOW TABLES + DESC TABLE
✓ Matches PostgreSQL/MySQL/MSSQL/Oracle connector patterns
"""

from __future__ import annotations
from collections.abc import Iterator
from typing import Any, Optional

import snowflake.connector

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
from app.connectors.utils import map_sql_type_to_data_type
from app.core.logging import get_logger
from app.schemas.connector_configs import SnowflakeConfig

logger = get_logger(__name__)


class SnowflakeSource(SourceConnector):
    """Universal ETL compliant Snowflake Source Connector."""

    # ------------------------------------------
    # Optional universal metadata helpers
    # ------------------------------------------
    def supports_streams(self) -> bool:
        return True

    def supports_query(self) -> bool:
        return True

    def get_stream_identifier(self, config: dict) -> Optional[str]:
        # Universal key resolution
        return (
            config.get("stream")
            or config.get("table")
            or config.get("name")
        )

    # ------------------------------------------
    # Initialization
    # ------------------------------------------
    def __init__(self, config: SnowflakeConfig):
        super().__init__(config)

        self.account = config.account
        self.username = config.username
        self.password = config.password.get_secret_value()
        self.role = config.role
        self.warehouse = config.warehouse
        self.database = config.database
        self.schema = config.schema_

        self.batch_size = config.batch_size
        self._connection = None

        logger.debug(
            "snowflake_source_initialized",
            account=self.account,
            database=self.database,
            schema=self.schema,
            warehouse=self.warehouse,
            role=self.role,
            batch_size=self.batch_size,
        )

    # ------------------------------------------
    # Connection Management
    # ------------------------------------------
    def connect(self) -> None:
        if self._connection:
            return

        logger.info("snowflake_connecting", account=self.account)

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
            logger.info("snowflake_connected", account=self.account)

        except Exception as e:
            logger.error("snowflake_connect_failed", error=str(e), exc_info=True)
            raise

    def disconnect(self) -> None:
        if self._connection:
            try:
                self._connection.close()
                logger.debug("snowflake_disconnected")
            except Exception as e:
                logger.warning("snowflake_disconnect_failed", error=str(e))
            finally:
                self._connection = None

    # ------------------------------------------
    # Test Connection
    # ------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        logger.info("snowflake_test_connection_started", account=self.account)

        try:
            with self:
                cur = self._connection.cursor()
                cur.execute("SELECT CURRENT_VERSION()")
                version = cur.fetchone()[0]
                cur.close()

            return ConnectionTestResult(
                success=True,
                message="Connected successfully",
                metadata={"version": version},
            )

        except Exception as e:
            logger.error("snowflake_test_connection_failed", error=str(e))
            return ConnectionTestResult(False, str(e))

    # ------------------------------------------
    # Schema Discovery
    # ------------------------------------------
    def discover_schema(self) -> Schema:
        logger.info(
            "snowflake_schema_discovery_started",
            database=self.database,
            schema=self.schema,
        )

        if not self.database or not self.schema:
            raise ValueError("Both database and schema must be set.")

        with self:
            try:
                cursor = self._connection.cursor()

                # -----------------------------
                # Fetch tables
                # -----------------------------
                cursor.execute(f"SHOW TABLES IN {self.database}.{self.schema}")
                table_rows = cursor.fetchall()

                logger.debug(
                    "snowflake_tables_listed",
                    table_count=len(table_rows),
                )

                tables = []

                for row in table_rows:
                    table_name = row[1]
                    comment = row[7] if len(row) > 7 else None

                    tables.append(
                        self._describe_table(cursor, table_name, comment)
                    )

                logger.info("snowflake_schema_discovery_completed")

                return Schema(tables=tables)

            except Exception as e:
                logger.error("snowflake_schema_discovery_failed", error=str(e))
                return Schema(tables=[])

    def _describe_table(self, cursor, table_name: str, comment: Optional[str]) -> Table:
        cursor.execute(f"DESCRIBE TABLE {self.database}.{self.schema}.{table_name}")

        columns = []
        for col_row in cursor:
            col_name = col_row[0]
            col_type = col_row[1]
            nullable = col_row[3] == "Y"

            mapped = map_sql_type_to_data_type(col_type)

            columns.append(
                Column(
                    name=col_name,
                    data_type=mapped,
                    nullable=nullable,
                )
            )

        return Table(
            name=table_name,
            schema=self.schema,
            columns=columns,
            description=comment,
        )

    # ------------------------------------------
    # Read Data (Streaming)
    # ------------------------------------------
    def read(
        self,
        stream: str,
        state: State | None = None,
        query: str | None = None,
    ) -> Iterator[Record]:

        if not stream and not query:
            raise ValueError("SnowflakeSource requires stream or query.")

        logger.info(
            "snowflake_read_started",
            stream=stream,
            query=bool(query),
            has_state=bool(state),
        )

        with self:
            try:
                cursor = self._connection.cursor()

                # -------------------------
                # Build SQL
                # -------------------------
                if query:
                    sql = query
                else:
                    sql = f"SELECT * FROM {self.database}.{self.schema}.{stream}"

                # -------------------------
                # Incremental sync support
                # -------------------------
                if state and state.cursor_field:
                    key = state.cursor_field
                    val = state.cursor_value

                    if val is not None:
                        clause = f"{key} > '{val}'"
                        if "WHERE" in sql.upper():
                            sql += f" AND {clause}"
                        else:
                            sql += f" WHERE {clause}"

                        logger.debug(
                            "snowflake_incremental_applied",
                            cursor_field=key,
                            cursor_value=str(val),
                        )

                logger.debug("snowflake_sql_prepared", sql_preview=sql[:250])

                cursor.execute(sql)

                col_names = [col[0] for col in cursor.description]

                # -------------------------
                # Stream records
                # -------------------------
                while True:
                    rows = cursor.fetchmany(self.batch_size)
                    if not rows:
                        break

                    logger.debug("snowflake_batch_fetched", batch_size=len(rows))

                    for row in rows:
                        yield Record(stream=stream, data=dict(zip(col_names, row)))

            except Exception as e:
                logger.error("snowflake_read_failed", error=str(e), exc_info=True)
                raise

    # ------------------------------------------
    # Record Count
    # ------------------------------------------
    def get_record_count(self, stream: str) -> int:
        logger.info("snowflake_record_count_requested", stream=stream)

        if not stream:
            raise ValueError("stream must be provided")

        with self:
            try:
                cursor = self._connection.cursor()
                cursor.execute(
                    f"SELECT COUNT(*) FROM {self.database}.{self.schema}.{stream}"
                )
                count = cursor.fetchone()[0]

                logger.info("snowflake_record_count_retrieved", count=count)
                return count

            except Exception as e:
                logger.error("snowflake_record_count_failed", error=str(e))
                raise
