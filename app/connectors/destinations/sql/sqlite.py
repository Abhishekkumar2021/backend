"""SQLite Destination Connector (Structured Logging Version)
"""

import sqlite3
from pathlib import Path
from collections.abc import Iterator
from typing import Any

from app.connectors.base import Column, ConnectionTestResult, DataType, DestinationConnector, Record
from app.core.logging import get_logger

logger = get_logger(__name__)


class SQLiteDestination(DestinationConnector):
    """SQLite destination connector
    Writes data to SQLite database
    """

    REVERSE_TYPE_MAPPING = {
        DataType.INTEGER: "INTEGER",
        DataType.FLOAT: "REAL",
        DataType.STRING: "TEXT",
        DataType.BOOLEAN: "BOOLEAN",
        DataType.DATE: "DATE",
        DataType.DATETIME: "DATETIME",
        DataType.JSON: "TEXT",
        DataType.BINARY: "BLOB",
        DataType.NULL: "TEXT",
    }

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.database_path = config["database_path"]
        self._batch_size = config.get("batch_size", 1000)
        self._connection = None

        logger.debug(
            "sqlite_destination_initialized",
            database_path=self.database_path,
            batch_size=self._batch_size,
        )

    # ----------------------------------------------------------------------
    # Connection Handling
    # ----------------------------------------------------------------------
    def connect(self) -> None:
        if self._connection is not None:
            return

        logger.info("sqlite_destination_connecting", database_path=self.database_path)

        try:
            db_path = Path(self.database_path)
            if self.database_path != ":memory:":
                db_path.parent.mkdir(parents=True, exist_ok=True)

            self._connection = sqlite3.connect(self.database_path, check_same_thread=False)

            logger.info("sqlite_destination_connected", database_path=self.database_path)

        except Exception as e:
            logger.error(
                "sqlite_destination_connection_failed",
                error=str(e),
                database_path=self.database_path,
                exc_info=True,
            )
            raise

    def disconnect(self) -> None:
        if not self._connection:
            return

        try:
            self._connection.close()
            logger.info("sqlite_destination_disconnected")
        except Exception as e:
            logger.warning(
                "sqlite_destination_disconnect_failed",
                error=str(e),
                exc_info=True,
            )
        finally:
            self._connection = None

    # ----------------------------------------------------------------------
    # Test Connection
    # ----------------------------------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        logger.info("sqlite_destination_test_connection_started", database_path=self.database_path)

        try:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute("SELECT sqlite_version();")
            version = cursor.fetchone()[0]
            cursor.close()

            logger.info(
                "sqlite_destination_test_connection_success",
                version=version,
                database_path=self.database_path,
            )

            return ConnectionTestResult(
                success=True,
                message="Write access confirmed",
                metadata={"version": f"SQLite {version}"},
            )

        except Exception as e:
            logger.error(
                "sqlite_destination_test_connection_failed",
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(success=False, message=str(e))

        finally:
            self.disconnect()

    # ----------------------------------------------------------------------
    # Create Stream (Table)
    # ----------------------------------------------------------------------
    def create_stream(self, stream: str, schema: list[Column]) -> None:
        logger.info("sqlite_destination_create_stream_started", stream=stream)

        self.connect()
        cursor = self._connection.cursor()

        try:
            col_defs = []
            for col in schema:
                col_type = self.REVERSE_TYPE_MAPPING.get(col.data_type, "TEXT")
                pk_def = " PRIMARY KEY" if col.primary_key else ""
                null_def = " NOT NULL" if not col.nullable and not col.primary_key else ""
                col_defs.append(f'"{col.name}" {col_type}{pk_def}{null_def}')

            create_sql = f'CREATE TABLE IF NOT EXISTS "{stream}" ({", ".join(col_defs)});'
            cursor.execute(create_sql)
            self._connection.commit()

            logger.info("sqlite_destination_stream_created", stream=stream)

        except Exception as e:
            logger.error(
                "sqlite_destination_create_stream_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            raise

        finally:
            cursor.close()
            self.disconnect()

    # ----------------------------------------------------------------------
    # Write
    # ----------------------------------------------------------------------
    def write(self, records: Iterator[Record]) -> int:
        logger.info(
            "sqlite_destination_write_started",
            batch_size=self._batch_size,
            database_path=self.database_path,
        )

        self.connect()
        cursor = self._connection.cursor()

        total_written = 0
        buffer = []
        current_stream = None

        try:
            for record in records:
                if current_stream and record.stream != current_stream:
                    if buffer:
                        self._flush_buffer(cursor, current_stream, buffer)
                        total_written += len(buffer)
                        buffer = []

                current_stream = record.stream
                buffer.append(record.data)

                if len(buffer) >= self._batch_size:
                    self._flush_buffer(cursor, current_stream, buffer)
                    total_written += len(buffer)
                    buffer = []

            # Flush leftover records
            if buffer and current_stream:
                self._flush_buffer(cursor, current_stream, buffer)
                total_written += len(buffer)

            self._connection.commit()

            logger.info(
                "sqlite_destination_write_success",
                total_records=total_written,
                stream=current_stream,
            )

            return total_written

        except Exception as e:
            self._connection.rollback()
            logger.error(
                "sqlite_destination_write_failed",
                error=str(e),
                exc_info=True,
            )
            raise

        finally:
            cursor.close()
            self.disconnect()

    # ----------------------------------------------------------------------
    # Flush Batch
    # ----------------------------------------------------------------------
    def _flush_buffer(self, cursor: sqlite3.Cursor, stream: str, data: list[dict[str, Any]]) -> None:
        if not data:
            return

        keys = list(data[0].keys())
        placeholders = ", ".join(["?" for _ in keys])
        columns = ", ".join([f'"{k}"' for k in keys])

        sql = f'INSERT OR REPLACE INTO "{stream}" ({columns}) VALUES ({placeholders})'

        values = [[item.get(k) for k in keys] for item in data]

        try:
            cursor.executemany(sql, values)

            logger.debug(
                "sqlite_destination_batch_write_success",
                stream=stream,
                records=len(data),
            )

        except Exception as e:
            logger.error(
                "sqlite_destination_batch_write_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            raise
