"""SQLite Destination Connector
"""

import logging
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from app.connectors.base import Column, ConnectionTestResult, DataType, DestinationConnector, Record

logger = logging.getLogger(__name__)


class SQLiteDestination(DestinationConnector):
    """SQLite destination connector
    Writes data to SQLite database
    """

    # Map DataType to SQLite types
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
        """Initialize SQLite destination connector

        Config format:
        {
            "database_path": "/path/to/database.db",
            "batch_size": 1000
        }
        """
        super().__init__(config)
        self.database_path = config["database_path"]
        self._batch_size = config.get("batch_size", 1000)
        self._connection = None

    def connect(self) -> None:
        """Establish connection to SQLite"""
        if self._connection is None:
            try:
                # Create DB if not exists
                db_path = Path(self.database_path)
                if self.database_path != ":memory:":
                    db_path.parent.mkdir(parents=True, exist_ok=True)

                self._connection = sqlite3.connect(self.database_path, check_same_thread=False)
                logger.info(f"Connected to SQLite: {self.database_path}")
            except Exception as e:
                logger.error(f"Failed to connect to SQLite: {e!s}")
                raise

    def disconnect(self) -> None:
        """Close SQLite connection"""
        if self._connection:
            try:
                self._connection.close()
                logger.info("SQLite connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {e!s}")
            finally:
                self._connection = None

    def test_connection(self) -> ConnectionTestResult:
        """Test SQLite connection"""
        try:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute("SELECT sqlite_version();")
            version = cursor.fetchone()[0]
            cursor.close()

            return ConnectionTestResult(
                success=True, message="Write access confirmed", metadata={"version": f"SQLite {version}"}
            )
        except Exception as e:
            return ConnectionTestResult(success=False, message=f"Connection failed: {e!s}")
        finally:
            self.disconnect()

    def create_stream(self, stream: str, schema: list[Column]) -> None:
        """Create table in SQLite
        """
        self.connect()
        cursor = self._connection.cursor()

        try:
            cols_def = []
            for col in schema:
                col_type = self.REVERSE_TYPE_MAPPING.get(col.data_type, "TEXT")
                pk_def = " PRIMARY KEY" if col.primary_key else ""
                null_def = " NOT NULL" if not col.nullable and not col.primary_key else ""
                cols_def.append(f'"{col.name}" {col_type}{pk_def}{null_def}')

            create_sql = f'CREATE TABLE IF NOT EXISTS "{stream}" ({", ".join(cols_def)});'
            cursor.execute(create_sql)
            self._connection.commit()
            logger.info(f"Created table: {stream}")

        except Exception as e:
            logger.error(f"Failed to create table {stream}: {e!s}")
            raise
        finally:
            cursor.close()
            self.disconnect()

    def write(self, records: Iterator[Record]) -> int:
        """Write records to SQLite
        """
        self.connect()
        cursor = self._connection.cursor()
        total_written = 0

        # Buffer for batch insert
        buffer = []
        current_stream = None

        try:
            for record in records:
                if current_stream and record.stream != current_stream:
                    # Stream changed, flush buffer
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

            # Flush remaining
            if buffer and current_stream:
                self._flush_buffer(cursor, current_stream, buffer)
                total_written += len(buffer)

            self._connection.commit()
            return total_written

        except Exception as e:
            self._connection.rollback()
            logger.error(f"Failed to write to SQLite: {e!s}")
            raise
        finally:
            cursor.close()
            self.disconnect()

    def _flush_buffer(self, cursor: sqlite3.Cursor, stream: str, data: list[dict[str, Any]]) -> None:
        """Insert batch of data"""
        if not data:
            return

        # Assume all records have same keys
        keys = list(data[0].keys())
        placeholders = ", ".join(["?" for _ in keys])
        columns = ", ".join([f'"{k}"' for k in keys])

        sql = f'INSERT OR REPLACE INTO "{stream}" ({columns}) VALUES ({placeholders})'

        values = []
        for item in data:
            values.append([item.get(k) for k in keys])

        cursor.executemany(sql, values)
