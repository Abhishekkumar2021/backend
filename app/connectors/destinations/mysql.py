"""MySQL Destination Connector
"""

import logging
from collections.abc import Iterator
from typing import Any

import pymysql

from app.connectors.base import Column, ConnectionTestResult, DataType, DestinationConnector, Record

logger = logging.getLogger(__name__)


class MySQLDestination(DestinationConnector):
    """MySQL destination connector
    """

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

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.host = config["host"]
        self.port = config.get("port", 3306)
        self.user = config["user"]
        self.password = config["password"]
        self.database = config["database"]
        self._batch_size = config.get("batch_size", 1000)
        self._connection = None

    def connect(self) -> None:
        if self._connection is None:
            self._connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                autocommit=False,
            )

    def disconnect(self) -> None:
        if self._connection:
            self._connection.commit()
            self._connection.close()
            self._connection = None

    def test_connection(self) -> ConnectionTestResult:
        try:
            self.connect()
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()[0]
            return ConnectionTestResult(success=True, message="Connected", metadata={"version": version})
        except Exception as e:
            return ConnectionTestResult(success=False, message=str(e))
        finally:
            self.disconnect()

    def create_stream(self, stream: str, schema: list[Column]) -> None:
        self.connect()
        try:
            with self._connection.cursor() as cursor:
                col_defs = []
                for col in schema:
                    my_type = self.TYPE_MAPPING.get(col.data_type, "TEXT")
                    nullable = "" if col.nullable else "NOT NULL"
                    col_defs.append(f"`{col.name}` {my_type} {nullable}")

                pks = [f"`{c.name}`" for c in schema if c.primary_key]
                if pks:
                    col_defs.append(f"PRIMARY KEY ({', '.join(pks)})")

                sql = f"CREATE TABLE IF NOT EXISTS `{stream}` ({', '.join(col_defs)})"
                cursor.execute(sql)
                self._connection.commit()
        finally:
            self.disconnect()

    def write(self, records: Iterator[Record]) -> int:
        self.connect()
        total = 0
        buffer = []
        current_stream = None
        cursor = self._connection.cursor()

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
            cursor.close()
            self.disconnect()

    def _flush(self, cursor, stream: str, data: list[dict[str, Any]]) -> None:
        if not data:
            return
        keys = list(data[0].keys())
        cols = ", ".join([f"`{k}`" for k in keys])
        placeholders = ", ".join(["%s"] * len(keys))

        sql = f"INSERT INTO `{stream}` ({cols}) VALUES ({placeholders})"

        values = []
        for item in data:
            values.append([item.get(k) for k in keys])

        cursor.executemany(sql, values)
