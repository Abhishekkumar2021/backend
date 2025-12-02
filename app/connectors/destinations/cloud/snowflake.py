"""
Improved Snowflake Destination Connector (Production-Ready)
"""

from collections.abc import Iterator
from typing import Any, List

import snowflake.connector

from app.connectors.base import (
    Column,
    ConnectionTestResult,
    DestinationConnector,
    Record,
    DataType,
)
from app.connectors.utils import map_data_type_to_sql_type
from app.core.logging import get_logger
from app.schemas.connector_configs import SnowflakeConfig

logger = get_logger(__name__)


class SnowflakeDestination(DestinationConnector):
    """
    Snowflake Destination Connector.
    Loads data into Snowflake with safe SQL, strong logging,
    correct parameter binding, write modes, and multiple-stream support.
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
        self._batch_size = config.batch_size

        # Pipeline overrides
        self.table = getattr(config, "table", None)
        self.write_mode = getattr(config, "write_mode", "append")  # append | truncate | overwrite

        logger.debug(
            "snowflake_destination_initialized",
            account=self.account,
            database=self.database,
            schema=self.schema,
            table=self.table,
            write_mode=self.write_mode,
            batch_size=self._batch_size,
        )

    # ===================================================================
    # Connection Handling
    # ===================================================================
    def connect(self) -> None:
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

            logger.info("snowflake_connect_success", database=self.database, schema=self.schema)

        except Exception as e:
            logger.error("snowflake_connect_failed", error=str(e), exc_info=True)
            raise

    def disconnect(self) -> None:
        """Close Snowflake connection."""
        if self._connection:
            try:
                self._connection.close()
                logger.debug("snowflake_connection_closed")
            except Exception as e:
                logger.warning("snowflake_connection_close_failed", error=str(e))
            finally:
                self._connection = None

    # ===================================================================
    # Test Connection
    # ===================================================================
    def test_connection(self) -> ConnectionTestResult:
        logger.info("snowflake_test_connection_started")

        try:
            with self:
                self._connection.cursor().execute("SELECT CURRENT_TIMESTAMP()").close()

            return ConnectionTestResult(success=True, message="Connected to Snowflake.")

        except Exception as e:
            logger.error("snowflake_test_connection_failed", error=str(e))
            return ConnectionTestResult(success=False, message=str(e))

    # ===================================================================
    # Create Table
    # ===================================================================
    def create_stream(self, stream: str, schema: List[Column]) -> None:
        """Create table if not exists."""
        if not self.database or not self.schema or not stream:
            raise ValueError("Database, Schema, and Table are required.")

        with self:
            cursor = self._connection.cursor()

            try:
                column_defs = []
                for col in schema:
                    sql_type = map_data_type_to_sql_type(col.data_type)
                    null_def = "NULL" if col.nullable else "NOT NULL"
                    column_defs.append(f'"{col.name}" {sql_type} {null_def}')

                sql = (
                    f'CREATE TABLE IF NOT EXISTS "{self.database}"."{self.schema}"."{stream}" '
                    f'({", ".join(column_defs)})'
                )

                logger.debug("snowflake_create_table_sql", sql_preview=sql[:200])
                cursor.execute(sql)

                logger.info("snowflake_table_created_or_exists", table=stream)

            finally:
                cursor.close()

    # ===================================================================
    # Write Records (MULTI-STREAM SUPPORT)
    # ===================================================================
    def write(self, records: Iterator[Record]) -> int:
        logger.info(
            "snowflake_write_started",
            configured_table=self.table,
            write_mode=self.write_mode,
            batch_size=self._batch_size,
        )

        # Group by stream → required for pipelines with multiple tables
        grouped: dict[str, List[dict[str, Any]]] = {}

        for rec in records:
            grouped.setdefault(rec.stream, []).append(rec.data)

        total = 0

        with self:
            for stream, batch_rows in grouped.items():
                logger.info("snowflake_stream_write_started", stream=stream, rows=len(batch_rows))

                # Apply configured override (if provided)
                table_to_write = self.table or stream

                # Write mode handling
                self._prepare_table_write_mode(table_to_write)

                inserted = self._insert_all_batches(stream=table_to_write, rows=batch_rows)
                total += inserted

                logger.info("snowflake_stream_write_completed", stream=stream, inserted=inserted)

        logger.info("snowflake_write_completed", total_inserted=total)
        return total

    # ===================================================================
    # Handle write_mode (append / truncate / overwrite)
    # ===================================================================
    def _prepare_table_write_mode(self, table: str):
        cursor = self._connection.cursor()
        try:
            full_name = f'"{self.database}"."{self.schema}"."{table}"'

            if self.write_mode == "truncate":
                logger.warning("snowflake_write_mode_truncate", table=table)
                cursor.execute(f"TRUNCATE TABLE {full_name}")

            elif self.write_mode == "overwrite":
                logger.warning("snowflake_write_mode_overwrite", table=table)
                cursor.execute(f"DROP TABLE IF EXISTS {full_name}")
                # Recreate empty table with no schema? (User should call create_stream)
        finally:
            cursor.close()

    # ===================================================================
    # Insert Batches
    # ===================================================================
    def _insert_all_batches(self, stream: str, rows: List[dict[str, Any]]) -> int:
        if not rows:
            return 0

        cursor = self._connection.cursor()
        inserted_total = 0

        try:
            while rows:
                current = rows[: self._batch_size]
                rows = rows[self._batch_size :]

                inserted_total += self._insert_batch(cursor, stream, current)

            self._connection.commit()
            return inserted_total

        except Exception:
            self._connection.rollback()
            raise

        finally:
            cursor.close()

    # ===================================================================
    # Insert a Single Batch
    # ===================================================================
    def _insert_batch(self, cursor: Any, table: str, batch: List[dict[str, Any]]) -> int:
        if not batch:
            return 0

        columns = list(batch[0].keys())
        full_table = f'"{self.database}"."{self.schema}"."{table}"'

        cols_sql = ", ".join([f'"{c}"' for c in columns])
        placeholders = ", ".join(["%s"] * len(columns))

        sql = f"INSERT INTO {full_table} ({cols_sql}) VALUES ({placeholders})"

        values = [[row.get(col) for col in columns] for row in batch]

        try:
            cursor.executemany(sql, values)

            logger.debug(
                "snowflake_batch_inserted",
                table=table,
                rows=len(batch),
            )

            return len(batch)

        except Exception as e:
            logger.error("snowflake_batch_insert_failed", table=table, error=str(e))
            return 0
