"""MSSQL Destination Connector
"""

import logging
from collections.abc import Iterator
from typing import Any

import pymssql

from app.connectors.base import Column, ConnectionTestResult, DataType, DestinationConnector, Record

logger = logging.getLogger(__name__)


class MSSQLDestination(DestinationConnector):
    """MSSQL destination connector
    """

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

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.host = config["host"]
        self.port = config.get("port", 1433)
        self.user = config["user"]
        self.password = config["password"]
        self.database = config["database"]
        self._batch_size = config.get("batch_size", 1000)
        self._connection = None

    def connect(self) -> None:
        if self._connection is None:
            server = f"{self.host}:{self.port}" if self.port else self.host
            self._connection = pymssql.connect(
                server=server, user=self.user, password=self.password, database=self.database, autocommit=False
            )

    def disconnect(self) -> None:
        if self._connection:
            self._connection.commit()
            self._connection.close()
            self._connection = None

    def test_connection(self) -> ConnectionTestResult:
        try:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
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
                ms_type = self.TYPE_MAPPING.get(col.data_type, "NVARCHAR(MAX)")
                nullable = "" if col.nullable else "NOT NULL"
                col_defs.append(f"[{col.name}] {ms_type} {nullable}")

            pks = [f"[{c.name}]" for c in schema if c.primary_key]
            if pks:
                col_defs.append(f"PRIMARY KEY ({', '.join(pks)})")

            # Splitting stream into schema.table if dot present, else dbo.table
            if "." in stream:
                schema_name, table_name = stream.split(".", 1)
            else:
                schema_name, table_name = "dbo", stream

            full_name = f"[{schema_name}].[{table_name}]"

            # Check if table exists first to avoid error (IF NOT EXISTS in CREATE TABLE is newer MSSQL)
            cursor.execute(f"""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                               WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}')
                CREATE TABLE {full_name} ({", ".join(col_defs)})
            """)
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise
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
            self.disconnect()

    def _flush(self, cursor, stream: str, data: list[dict[str, Any]]) -> None:
        if not data:
            return
        keys = list(data[0].keys())

        # Handle schema prefix
        if "." in stream:
            schema_name, table_name = stream.split(".", 1)
            full_name = f"[{schema_name}].[{table_name}]"
        else:
            full_name = f"[dbo].[{stream}]"

        cols = ", ".join([f"[{k}]" for k in keys])
        placeholders = ", ".join(["%s"] * len(keys))

        sql = f"INSERT INTO {full_name} ({cols}) VALUES ({placeholders})"

        values = []
        for item in data:
            values.append(tuple([item.get(k) for k in keys]))

        cursor.executemany(sql, values)
