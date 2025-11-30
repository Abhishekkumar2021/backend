"""MSSQL Source Connector
"""

import logging
from collections.abc import Iterator
from typing import Any

import pymssql

from app.connectors.base import Column, ConnectionTestResult, DataType, Record, Schema, SourceConnector, State, Table

logger = logging.getLogger(__name__)


class MSSQLSource(SourceConnector):
    """MSSQL source connector
    """

    TYPE_MAPPING = {
        "int": DataType.INTEGER,
        "bigint": DataType.INTEGER,
        "smallint": DataType.INTEGER,
        "tinyint": DataType.INTEGER,
        "bit": DataType.BOOLEAN,
        "decimal": DataType.FLOAT,
        "numeric": DataType.FLOAT,
        "money": DataType.FLOAT,
        "smallmoney": DataType.FLOAT,
        "float": DataType.FLOAT,
        "real": DataType.FLOAT,
        "datetime": DataType.DATETIME,
        "smalldatetime": DataType.DATETIME,
        "char": DataType.STRING,
        "varchar": DataType.STRING,
        "text": DataType.STRING,
        "nchar": DataType.STRING,
        "nvarchar": DataType.STRING,
        "ntext": DataType.STRING,
        "binary": DataType.BINARY,
        "varbinary": DataType.BINARY,
        "image": DataType.BINARY,
        "date": DataType.DATE,
        "time": DataType.STRING,
        "datetime2": DataType.DATETIME,
        "datetimeoffset": DataType.DATETIME,
    }

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.host = config["host"]  # server
        self.port = config.get("port", 1433)
        self.user = config["user"]
        self.password = config["password"]
        self.database = config["database"]
        self._batch_size = config.get("batch_size", 1000)
        self._connection = None

    def connect(self) -> None:
        if self._connection is None:
            try:
                # pymssql connects using 'server' argument which can include port
                server = f"{self.host}:{self.port}" if self.port else self.host
                self._connection = pymssql.connect(
                    server=server, user=self.user, password=self.password, database=self.database, as_dict=True
                )
                logger.info(f"Connected to MSSQL: {self.host}")
            except Exception as e:
                logger.error(f"Failed to connect to MSSQL: {e!s}")
                raise

    def disconnect(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None

    def test_connection(self) -> ConnectionTestResult:
        try:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute("SELECT @@VERSION as version")
            row = cursor.fetchone()
            version = row["version"]
            cursor.close()
            return ConnectionTestResult(success=True, message="Connected", metadata={"version": version})
        except Exception as e:
            return ConnectionTestResult(success=False, message=str(e))
        finally:
            self.disconnect()

    def discover_schema(self) -> Schema:
        self.connect()
        tables = []
        cursor = self._connection.cursor()
        try:
            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
            """)
            table_list = cursor.fetchall()

            for tbl in table_list:
                schema_name = tbl["TABLE_SCHEMA"]
                table_name = tbl["TABLE_NAME"]
                full_name = f"{schema_name}.{table_name}"

                cursor.execute(
                    """
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION
                """,
                    (schema_name, table_name),
                )

                columns = []
                for col in cursor.fetchall():
                    c_name = col["COLUMN_NAME"]
                    c_type = col["DATA_TYPE"]
                    is_nullable = col["IS_NULLABLE"] == "YES"

                    d_type = self.TYPE_MAPPING.get(c_type, DataType.STRING)

                    columns.append(
                        Column(name=c_name, data_type=d_type, nullable=is_nullable, default_value=col["COLUMN_DEFAULT"])
                    )

                cursor.execute(f"SELECT COUNT(*) as cnt FROM {schema_name}.{table_name}")
                count = cursor.fetchone()["cnt"]

                tables.append(Table(name=full_name, schema=schema_name, columns=columns, row_count=count))

            return Schema(tables=tables)
        finally:
            cursor.close()
            self.disconnect()

    def read(self, stream: str, state: State | None = None, query: str | None = None) -> Iterator[Record]:
        self.connect()
        cursor = self._connection.cursor()
        try:
            # Stream expected to be schema.table
            if query:
                sql = query
                params = []
            elif state and state.cursor_field:
                sql = f"SELECT * FROM {stream} WHERE {state.cursor_field} > %s ORDER BY {state.cursor_field}"
                params = [state.cursor_value]
            else:
                sql = f"SELECT * FROM {stream}"
                params = []

            cursor.execute(sql, tuple(params))

            while True:
                rows = cursor.fetchmany(self._batch_size)
                if not rows:
                    break
                for row in rows:
                    yield Record(stream=stream, data=row)
        finally:
            cursor.close()
            self.disconnect()

    def get_record_count(self, stream: str) -> int:
        self.connect()
        cursor = self._connection.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) as cnt FROM {stream}")
            return cursor.fetchone()["cnt"]
        finally:
            cursor.close()
            self.disconnect()
