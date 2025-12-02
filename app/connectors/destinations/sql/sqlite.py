"""
SQLite Destination Connector — Universal ETL Compatible
-------------------------------------------------------
Supports:
✓ Create table (schema)
✓ Insert
✓ Insert OR REPLACE (UPSERT behavior)
✓ Streaming batch writes
✓ Multi-stream pipelines
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from collections.abc import Iterator
from typing import Any, List

from app.connectors.base import (
    Column,
    ConnectionTestResult,
    DataType,
    DestinationConnector,
    Record,
)
from app.core.logging import get_logger
from app.schemas.connector_configs import SQLiteConfig

logger = get_logger(__name__)


class SQLiteDestination(DestinationConnector):

    SQLITE_TYPE = {
        DataType.INTEGER: "INTEGER",
        DataType.FLOAT: "REAL",
        DataType.STRING: "TEXT",
        DataType.BOOLEAN: "BOOLEAN",
        DataType.DATE: "TEXT",
        DataType.DATETIME: "TEXT",
        DataType.JSON: "TEXT",
        DataType.BINARY: "BLOB",
        DataType.NULL: "TEXT",
    }

    def __init__(self, config: SQLiteConfig):
        super().__init__(config)
        self.database_path = config.database_path
        self.batch_size = config.batch_size
        self._connection: sqlite3.Connection | None = None

        logger.debug(
            "sqlite_destination_initialized",
            database_path=self.database_path,
            batch_size=self.batch_size,
        )

    # ------------------------------------------------------------------
    # Universal Destination API
    # ------------------------------------------------------------------
    def supports_streams(self) -> bool:
        return True

    def supports_query(self) -> bool:
        return False

    def get_stream_identifier(self, config: dict) -> str:
        return config.get("stream") or config.get("table") or "main"

    # ------------------------------------------------------------------
    # Connection Handling
    # ------------------------------------------------------------------
    def connect(self) -> None:
        if self._connection:
            return

        logger.info("sqlite_destination_connecting", db=self.database_path)

        try:
            db_path = Path(self.database_path)
            if self.database_path != ":memory__":
                db_path.parent.mkdir(parents=True, exist_ok=True)

            self._connection = sqlite3.connect(self.database_path, check_same_thread=False)
            self._connection.execute("PRAGMA journal_mode=WAL;")
            self._connection.execute("PRAGMA synchronous=NORMAL;")
            self._connection.execute("PRAGMA foreign_keys=ON;")

            logger.info("sqlite_destination_connected", db=self.database_path)

        except Exception as e:
            logger.error("sqlite_destination_connection_failed", error=str(e), exc_info=True)
            raise

    def disconnect(self) -> None:
        if not self._connection:
            return

        try:
            self._connection.commit()
            logger.debug("sqlite_destination_commit_success")
        except Exception as e:
            logger.warning("sqlite_destination_commit_failed", error=str(e), exc_info=True)

        try:
            self._connection.close()
        except Exception as e:
            logger.warning("sqlite_destination_close_failed", error=str(e), exc_info=True)

        logger.info("sqlite_destination_disconnected")
        self._connection = None

    # ------------------------------------------------------------------
    # Test Connection
    # ------------------------------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        try:
            self.connect()
            cur = self._connection.cursor()
            cur.execute("SELECT sqlite_version();")
            version = cur.fetchone()[0]
            return ConnectionTestResult(True, "Write access confirmed", {"version": version})
        except Exception as e:
            return ConnectionTestResult(False, str(e))
        finally:
            self.disconnect()

    # ------------------------------------------------------------------
    # Create Table / Stream
    # ------------------------------------------------------------------
    def create_stream(self, stream: str, schema: List[Column]) -> None:
        logger.info("sqlite_create_stream", stream=stream)

        self.connect()
        cur = self._connection.cursor()

        try:
            col_defs = []
            for col in schema:
                col_type = self.SQLITE_TYPE.get(col.data_type, "TEXT")
                pk = " PRIMARY KEY" if col.primary_key else ""
                null = " NOT NULL" if not col.nullable and not col.primary_key else ""
                col_defs.append(f'"{col.name}" {col_type}{pk}{null}')

            sql = f'CREATE TABLE IF NOT EXISTS "{stream}" ({", ".join(col_defs)});'
            cur.execute(sql)
            self._connection.commit()

            logger.info("sqlite_stream_created", table=stream)

        except Exception as e:
            logger.error("sqlite_create_stream_failed", stream=stream, error=str(e), exc_info=True)
            raise
        finally:
            cur.close()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------
    def write(self, records: Iterator[Record]) -> int:
        """
        Multi-stream safe.
        Batching included.
        Uses INSERT OR REPLACE for upsert behavior.
        """
        logger.info("sqlite_write_started", batch_size=self.batch_size)

        self.connect()
        cur = self._connection.cursor()

        total = 0
        buffer: list[dict] = []
        current_stream = None

        try:
            for rec in records:

                # Stream switched -> flush
                if current_stream and rec.stream != current_stream:
                    total += self._flush(cur, current_stream, buffer)
                    buffer = []

                current_stream = rec.stream
                buffer.append(rec.data)

                if len(buffer) >= self.batch_size:
                    total += self._flush(cur, current_stream, buffer)
                    buffer = []

            # final flush
            if current_stream and buffer:
                total += self._flush(cur, current_stream, buffer)

            self._connection.commit()

            logger.info("sqlite_write_completed", written=total)
            return total

        except Exception as e:
            self._connection.rollback()
            logger.error("sqlite_write_failed", error=str(e), exc_info=True)
            raise

        finally:
            cur.close()
            self.disconnect()

    # ------------------------------------------------------------------
    # Flush Batch
    # ------------------------------------------------------------------
    def _flush(self, cur: sqlite3.Cursor, stream: str, rows: list[dict]) -> int:
        if not rows:
            return 0

        keys = list(rows[0].keys())
        placeholders = ", ".join(["?"] * len(keys))
        col_list = ", ".join([f'"{k}"' for k in keys])

        sql = f'INSERT OR REPLACE INTO "{stream}" ({col_list}) VALUES ({placeholders});'
        values = [[row.get(k) for k in keys] for row in rows]

        try:
            cur.executemany(sql, values)

            logger.debug(
                "sqlite_batch_written",
                stream=stream,
                batch_size=len(rows),
            )

            return len(rows)

        except Exception as e:
            logger.error(
                "sqlite_batch_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            raise
