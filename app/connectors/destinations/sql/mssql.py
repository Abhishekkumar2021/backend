"""
MSSQL Destination Connector — Unified ETL Framework Edition
Fully aligned with PostgreSQL / Oracle / MySQL / SQLite destination connectors.
"""

from __future__ import annotations
from collections.abc import Iterator
from typing import Any, List

import pymssql

from app.connectors.base import (
    Column,
    ConnectionTestResult,
    DestinationConnector,
    Record,
    DataType,
)
from app.core.logging import get_logger
from app.schemas.connector_configs import MSSQLConfig

logger = get_logger(__name__)


class MSSQLDestination(DestinationConnector):
    """Microsoft SQL Server destination connector."""

    TYPE_MAPPING = {
        DataType.INTEGER: "BIGINT",
        DataType.FLOAT: "FLOAT",
        DataType.STRING: "NVARCHAR(MAX)",
        DataType.BOOLEAN: "BIT",
        DataType.DATE: "DATE",
        DataType.DATETIME: "DATETIME2",
        DataType.JSON: "NVARCHAR(MAX)",
        DataType.BINARY: "VARBINARY(MAX)",
        DataType.NULL: "NVARCHAR(MAX)",
    }

    def __init__(self, config: MSSQLConfig):
        super().__init__(config)

        self.host = config.host
        self.port = config.port
        self.user = config.user
        self.password = config.password.get_secret_value()
        self.database = config.database
        self._batch_size = config.batch_size

        self._connection = None
        self.write_mode = getattr(config, "write_mode", "insert")

        logger.debug(
            "mssql_destination_initialized",
            host=self.host,
            port=self.port,
            database=self.database,
            write_mode=self.write_mode,
            batch_size=self._batch_size,
        )

    # =========================================================================
    # Connection
    # =========================================================================
    def connect(self) -> None:
        if self._connection:
            return

        server = f"{self.host}:{self.port}" if self.port else self.host

        logger.info(
            "mssql_destination_connecting",
            server=server,
            database=self.database,
        )

        try:
            self._connection = pymssql.connect(
                server=server,
                user=self.user,
                password=self.password,
                database=self.database,
                autocommit=False,
                as_dict=False,  # better for performance + consistent with other connectors
            )

            logger.info("mssql_destination_connected", server=server)

        except Exception as e:
            logger.error(
                "mssql_destination_connect_failed",
                server=server,
                error=str(e),
                exc_info=True,
            )
            raise

    def disconnect(self) -> None:
        if not self._connection:
            return

        try:
            self._connection.commit()
            logger.debug("mssql_destination_commit_success")
        except Exception as e:
            logger.warning(
                "mssql_destination_commit_failed",
                error=str(e),
                exc_info=True,
            )

        try:
            self._connection.close()
            logger.info("mssql_destination_disconnected")
        except Exception as e:
            logger.warning(
                "mssql_destination_disconnect_failed",
                error=str(e),
                exc_info=True,
            )

        finally:
            self._connection = None

    # =========================================================================
    # Test Connection
    # =========================================================================
    def test_connection(self) -> ConnectionTestResult:
        logger.info("mssql_destination_test_requested")

        try:
            self.connect()
            cur = self._connection.cursor()
            cur.execute("SELECT @@VERSION")
            version = cur.fetchone()[0]
            cur.close()

            logger.info("mssql_destination_test_success", version=version)

            return ConnectionTestResult(True, "Connected", {"version": version})

        except Exception as e:
            logger.error(
                "mssql_destination_test_failed",
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(False, str(e))

        finally:
            self.disconnect()

    # =========================================================================
    # Create Stream / Table
    # =========================================================================
    def create_stream(self, stream: str, schema: List[Column]) -> None:
        self.connect()

        logger.info("mssql_destination_create_stream", stream=stream)

        try:
            cur = self._connection.cursor()

            # Parse schema.table
            if "." in stream:
                schema_name, table_name = stream.split(".", 1)
            else:
                schema_name, table_name = "dbo", stream

            full_name = f"[{schema_name}].[{table_name}]"

            col_defs = []
            for col in schema:
                sql_type = self.TYPE_MAPPING.get(col.data_type, "NVARCHAR(MAX)")
                nullable = "NULL" if col.nullable else "NOT NULL"
                col_defs.append(f"[{col.name}] {sql_type} {nullable}")

            pk_cols = [f"[{c.name}]" for c in schema if c.primary_key]
            if pk_cols:
                col_defs.append(f"PRIMARY KEY ({', '.join(pk_cols)})")

            sql = f"""
            IF NOT EXISTS (
                SELECT 1
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{schema_name}'
                  AND TABLE_NAME = '{table_name}'
            )
            CREATE TABLE {full_name} (
                {", ".join(col_defs)}
            )
            """

            logger.debug(
                "mssql_destination_create_sql",
                sql_preview=sql[:250],
            )

            cur.execute(sql)
            self._connection.commit()

            logger.info("mssql_destination_table_created", table=full_name)

        except Exception as e:
            self._connection.rollback()
            logger.error(
                "mssql_destination_create_stream_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            raise

        finally:
            self.disconnect()

    # =========================================================================
    # Write Records
    # =========================================================================
    def write(self, records: Iterator[Record]) -> int:
        logger.info(
            "mssql_destination_write_started",
            batch_size=self._batch_size,
            write_mode=self.write_mode,
        )

        self.connect()
        cur = self._connection.cursor()

        total_written = 0
        buffer = []
        current_stream = None

        try:
            for rec in records:
                if current_stream and rec.stream != current_stream:
                    total_written += self._flush(cur, current_stream, buffer)
                    buffer = []

                current_stream = rec.stream
                buffer.append(rec.data)

                if len(buffer) >= self._batch_size:
                    total_written += self._flush(cur, current_stream, buffer)
                    buffer = []

            # Final flush
            if buffer:
                total_written += self._flush(cur, current_stream, buffer)

            self._connection.commit()

            logger.info(
                "mssql_destination_write_success",
                total_records=total_written,
            )

            return total_written

        except Exception as e:
            self._connection.rollback()
            logger.error(
                "mssql_destination_write_failed",
                error=str(e),
                exc_info=True,
            )
            raise

        finally:
            self.disconnect()

    # =========================================================================
    # Batch Insert
    # =========================================================================
    def _flush(self, cur, stream: str, batch: List[dict[str, Any]]) -> int:
        if not batch:
            return 0

        try:
            keys = list(batch[0].keys())

            # Parse schema.table
            if "." in stream:
                schema_name, table_name = stream.split(".", 1)
                full_name = f"[{schema_name}].[{table_name}]"
            else:
                full_name = f"[dbo].[{stream}]"

            columns = ", ".join([f"[{k}]" for k in keys])
            placeholders = ", ".join(["%s"] * len(keys))
            sql = f"INSERT INTO {full_name} ({columns}) VALUES ({placeholders})"

            values = [
                tuple(row.get(k) for k in keys)
                for row in batch
            ]

            cur.executemany(sql, values)

            logger.debug(
                "mssql_destination_batch_success",
                table=full_name,
                records=len(values),
            )

            return len(values)

        except Exception as e:
            logger.error(
                "mssql_destination_batch_failed",
                table=stream,
                error=str(e),
                exc_info=True,
            )
            return 0
