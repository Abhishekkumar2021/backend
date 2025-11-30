"""Oracle Destination Connector
"""

import logging
from collections.abc import Iterator
from typing import Any

import oracledb

from app.connectors.base import Column, ConnectionTestResult, DataType, DestinationConnector, Record

logger = logging.getLogger(__name__)


class OracleDestination(DestinationConnector):
    """Oracle destination connector
    """

    TYPE_MAPPING = {
        DataType.INTEGER: "NUMBER",
        DataType.FLOAT: "NUMBER",
        DataType.STRING: "VARCHAR2(4000)",  # Default, can be CLOB if needed
        DataType.BOOLEAN: "NUMBER(1)",  # Oracle has no BOOLEAN in SQL until recently (23c), stick to 0/1 standard
        DataType.DATE: "DATE",
        DataType.DATETIME: "TIMESTAMP",
        DataType.JSON: "CLOB",  # Or JSON type in new Oracle
        DataType.BINARY: "BLOB",
        DataType.NULL: "VARCHAR2(255)",
    }

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.user = config["user"]
        self.password = config["password"]
        self.dsn = config["dsn"]
        self._batch_size = config.get("batch_size", 1000)
        self._connection = None

    def connect(self) -> None:
        if self._connection is None:
            try:
                self._connection = oracledb.connect(user=self.user, password=self.password, dsn=self.dsn)
                self._connection.autocommit = False
                logger.info(f"Connected to Oracle Destination: {self.dsn}")
            except Exception as e:
                logger.error(f"Failed to connect to Oracle: {e!s}")
                raise

    def disconnect(self) -> None:
        if self._connection:
            try:
                self._connection.commit()
                self._connection.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {e!s}")
            finally:
                self._connection = None

    def test_connection(self) -> ConnectionTestResult:
        try:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute("SELECT * FROM v$version")
            version = cursor.fetchone()[0]
            cursor.close()
            return ConnectionTestResult(success=True, message="Connected", metadata={"version": version})
        except Exception as e:
            return ConnectionTestResult(success=False, message=str(e))
        finally:
            self.disconnect()

    def create_stream(self, stream: str, schema: list[Column]) -> None:
        self.connect()
        cursor = self._connection.cursor()
        try:
            col_defs = []
            for col in schema:
                ora_type = self.TYPE_MAPPING.get(col.data_type, "VARCHAR2(4000)")
                nullable = "" if col.nullable else "NOT NULL"
                col_defs.append(f'"{col.name}" {ora_type} {nullable}')

            # PK
            pks = [f'"{c.name}"' for c in schema if c.primary_key]
            if pks:
                col_defs.append(f"PRIMARY KEY ({', '.join(pks)})")

            sql = f'CREATE TABLE "{stream}" ({", ".join(col_defs)})'

            # Oracle doesn't have IF NOT EXISTS, catch error
            try:
                cursor.execute(sql)
            except oracledb.DatabaseError as e:
                (error,) = e.args
                if error.code == 955:  # ORA-00955: name is already used by an existing object
                    logger.info(f"Table {stream} already exists")
                else:
                    raise

            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise
        finally:
            cursor.close()
            self.disconnect()

    def write(self, records: Iterator[Record]) -> int:
        self.connect()
        cursor = self._connection.cursor()
        total = 0
        buffer = []
        current_stream = None

        try:
            for record in records:
                if current_stream and current_stream != record.stream:
                    self._flush(cursor, current_stream, buffer)
                    total += len(buffer)
                    buffer = []

                current_stream = record.stream
                buffer.append(record.data)

                if len(buffer) >= self._batch_size:
                    self._flush(cursor, current_stream, buffer)
                    total += len(buffer)
                    buffer = []

            if buffer:
                self._flush(cursor, current_stream, buffer)
                total += len(buffer)

            self._connection.commit()
            return total
        except Exception:
            self._connection.rollback()
            raise
        finally:
            self.disconnect()

    def _flush(self, cursor, stream: str, data: list[dict[str, Any]]) -> None:
        if not data:
            return
        keys = list(data[0].keys())
        cols = ", ".join([f'"{k}"' for k in keys])
        placeholders = ", ".join([f":{i + 1}" for i in range(len(keys))])

        sql = f'INSERT INTO "{stream}" ({cols}) VALUES ({placeholders})'

        # Convert dicts to list of tuples for executemany
        values = []
        for item in data:
            values.append([item.get(k) for k in keys])

        cursor.executemany(sql, values)
