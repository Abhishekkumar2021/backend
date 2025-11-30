"""MySQL Source Connector
"""

import logging
from collections.abc import Iterator
from typing import Any

import pymysql
import pymysql.cursors

from app.connectors.base import Column, ConnectionTestResult, DataType, Record, Schema, SourceConnector, State, Table

logger = logging.getLogger(__name__)


class MySQLSource(SourceConnector):
    """MySQL source connector
    """

    TYPE_MAPPING = {
        "int": DataType.INTEGER,
        "tinyint": DataType.INTEGER,
        "smallint": DataType.INTEGER,
        "mediumint": DataType.INTEGER,
        "bigint": DataType.INTEGER,
        "float": DataType.FLOAT,
        "double": DataType.FLOAT,
        "decimal": DataType.FLOAT,
        "varchar": DataType.STRING,
        "char": DataType.STRING,
        "text": DataType.STRING,
        "tinytext": DataType.STRING,
        "mediumtext": DataType.STRING,
        "longtext": DataType.STRING,
        "json": DataType.JSON,
        "date": DataType.DATE,
        "datetime": DataType.DATETIME,
        "timestamp": DataType.DATETIME,
        "blob": DataType.BINARY,
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
            try:
                self._connection = pymysql.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    cursorclass=pymysql.cursors.DictCursor,
                )
                logger.info(f"Connected to MySQL: {self.host}")
            except Exception as e:
                logger.error(f"Failed to connect to MySQL: {e!s}")
                raise

    def disconnect(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None

    def test_connection(self) -> ConnectionTestResult:
        try:
            self.connect()
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()["VERSION()"]
            return ConnectionTestResult(success=True, message="Connected", metadata={"version": version})
        except Exception as e:
            return ConnectionTestResult(success=False, message=str(e))
        finally:
            self.disconnect()

    def discover_schema(self) -> Schema:
        self.connect()
        tables = []
        try:
            with self._connection.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                table_names = [list(row.values())[0] for row in cursor.fetchall()]

                for table_name in table_names:
                    cursor.execute(f"DESCRIBE `{table_name}`")
                    columns = []
                    for col_row in cursor.fetchall():
                        # Field, Type, Null, Key, Default, Extra
                        col_name = col_row["Field"]
                        col_type_raw = col_row["Type"].split("(")[0]
                        is_nullable = col_row["Null"] == "YES"
                        is_pk = col_row["Key"] == "PRI"

                        data_type = self.TYPE_MAPPING.get(col_type_raw, DataType.STRING)

                        columns.append(
                            Column(
                                name=col_name,
                                data_type=data_type,
                                nullable=is_nullable,
                                primary_key=is_pk,
                                default_value=col_row["Default"],
                            )
                        )

                    cursor.execute(f"SELECT COUNT(*) as cnt FROM `{table_name}`")
                    count = cursor.fetchone()["cnt"]

                    tables.append(Table(name=table_name, columns=columns, row_count=count))
            return Schema(tables=tables)
        finally:
            self.disconnect()

    def read(self, stream: str, state: State | None = None, query: str | None = None) -> Iterator[Record]:
        self.connect()
        try:
            with self._connection.cursor() as cursor:
                if query:
                    sql = query
                    params = []
                elif state and state.cursor_field:
                    sql = f"SELECT * FROM `{stream}` WHERE `{state.cursor_field}` > %s ORDER BY `{state.cursor_field}`"
                    params = [state.cursor_value]
                else:
                    sql = f"SELECT * FROM `{stream}`"
                    params = []

                cursor.execute(sql, params)

                while True:
                    rows = cursor.fetchmany(self._batch_size)
                    if not rows:
                        break
                    for row in rows:
                        yield Record(stream=stream, data=row)
        finally:
            self.disconnect()

    def get_record_count(self, stream: str) -> int:
        self.connect()
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) as cnt FROM `{stream}`")
                return cursor.fetchone()["cnt"]
        finally:
            self.disconnect()
