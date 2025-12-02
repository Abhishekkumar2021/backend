"""
MySQL Destination Connector — Unified ETL Framework Edition
Consistent with PostgreSQL, Oracle, SQLite, FileSystem destinations.
"""

from __future__ import annotations
from collections.abc import Iterator
from typing import Any, List

import pymysql

from app.connectors.base import (
    Column,
    ConnectionTestResult,
    DestinationConnector,
    Record,
    DataType,
)
from app.core.logging import get_logger
from app.schemas.connector_configs import MySQLConfig

logger = get_logger(__name__)


class MySQLDestination(DestinationConnector):
    """MySQL destination connector (production-grade)."""

    TYPE_MAPPING = {
        DataType.INTEGER: "BIGINT",
        DataType.FLOAT: "DOUBLE",
        DataType.STRING: "TEXT",
        DataType.BOOLEAN: "BOOLEAN",
        DataType.DATE: "DATE",
        DataType.DATETIME: "DATETIME",
        DataType.JSON: "JSON",
        DataType.BINARY: "BLOB",
        DataType.NULL: "TEXT",
    }

    def __init__(self, config: MySQLConfig):
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
            "mysql_destination_initialized",
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

        logger.info(
            "mysql_destination_connecting",
            host=self.host,
            port=self.port,
            database=self.database,
        )

        try:
            self._connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset="utf8mb4",
                autocommit=False,
                cursorclass=pymysql.cursors.DictCursor,
            )

            logger.info("mysql_destination_connected", host=self.host)

        except Exception as e:
            logger.error(
                "mysql_destination_connect_failed",
                error=str(e),
                exc_info=True,
            )
            raise

    def disconnect(self) -> None:
        if not self._connection:
            return

        try:
            self._connection.commit()
            logger.debug("mysql_destination_commit_success")
        except Exception as e:
            logger.warning(
                "mysql_destination_commit_failed",
                error=str(e),
                exc_info=True,
            )

        try:
            self._connection.close()
            logger.info("mysql_destination_disconnected")
        except Exception as e:
            logger.warning(
                "mysql_destination_disconnect_failed",
                error=str(e),
                exc_info=True,
            )
        finally:
            self._connection = None

    # =========================================================================
    # Test Connection
    # =========================================================================
    def test_connection(self) -> ConnectionTestResult:
        logger.info("mysql_destination_test_requested")

        try:
            self.connect()
            cur = self._connection.cursor()
            cur.execute("SELECT VERSION()")
            version = cur.fetchone()["VERSION()"]
            cur.close()

            logger.info("mysql_destination_test_success", version=version)

            return ConnectionTestResult(True, "Connected", {"version": version})

        except Exception as e:
            logger.error(
                "mysql_destination_test_failed",
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(False, str(e))

        finally:
            self.disconnect()

    # =========================================================================
    # Create Table
    # =========================================================================
    def create_stream(self, stream: str, schema: List[Column]) -> None:
        self.connect()

        logger.info("mysql_destination_create_stream", stream=stream)

        try:
            cur = self._connection.cursor()

            col_defs = []
            for col in schema:
                mysql_type = self.TYPE_MAPPING.get(col.data_type, "TEXT")
                not_null = "" if col.nullable else "NOT NULL"
                col_defs.append(f"`{col.name}` {mysql_type} {not_null}")

            pk_cols = [f"`{c.name}`" for c in schema if c.primary_key]
            if pk_cols:
                col_defs.append(f"PRIMARY KEY ({', '.join(pk_cols)})")

            sql = f"CREATE TABLE IF NOT EXISTS `{stream}` ({', '.join(col_defs)}) ENGINE=InnoDB"

            logger.debug("mysql_destination_create_sql", sql_preview=sql[:200])

            cur.execute(sql)
            self._connection.commit()

            logger.info("mysql_destination_stream_created", stream=stream)

        except Exception as e:
            logger.error(
                "mysql_destination_create_stream_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            self._connection.rollback()
            raise

        finally:
            self.disconnect()

    # =========================================================================
    # Write Records
    # =========================================================================
    def write(self, records: Iterator[Record]) -> int:
        logger.info(
            "mysql_destination_write_started",
            batch_size=self._batch_size,
            write_mode=self.write_mode,
        )

        self.connect()
        cur = self._connection.cursor()

        buffer = []
        total_written = 0
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

            if buffer:
                total_written += self._flush(cur, current_stream, buffer)

            self._connection.commit()

            logger.info("mysql_destination_write_success", total_records=total_written)
            return total_written

        except Exception as e:
            self._connection.rollback()
            logger.error(
                "mysql_destination_write_failed",
                error=str(e),
                exc_info=True,
            )
            raise

        finally:
            cur.close()
            self.disconnect()

    # =========================================================================
    # Batch Flush
    # =========================================================================
    def _flush(self, cur, stream: str, batch: List[dict[str, Any]]) -> int:
        if not batch:
            return 0

        try:
            keys = list(batch[0].keys())
            cols = ", ".join([f"`{k}`" for k in keys])
            placeholders = ", ".join(["%s"] * len(keys))

            sql = f"INSERT INTO `{stream}` ({cols}) VALUES ({placeholders})"

            rows = [
                [row.get(col) for col in keys]
                for row in batch
            ]

            cur.executemany(sql, rows)

            logger.debug(
                "mysql_destination_batch_success",
                stream=stream,
                records=len(rows),
            )
            return len(rows)

        except Exception as e:
            logger.error(
                "mysql_destination_batch_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            return 0
