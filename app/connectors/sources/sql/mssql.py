"""MSSQL Source Connector
"""

from collections.abc import Iterator
from typing import Any

import pymssql

from app.connectors.base import (
    Column, ConnectionTestResult, DataType,
    Record, Schema, SourceConnector, State, Table
)
from app.core.logging import get_logger

logger = get_logger(__name__)


class MSSQLSource(SourceConnector):
    """MSSQL source connector."""

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

        self.host = config["host"]
        self.port = config.get("port", 1433)
        self.user = config["user"]
        self.password = config["password"]
        self.database = config["database"]
        self._batch_size = config.get("batch_size", 1000)

        self._connection = None

        logger.debug(
            "mssql_source_initialized",
            host=self.host,
            port=self.port,
            database=self.database,
            batch_size=self._batch_size,
        )

    # ===================================================================
    # Connection Handling
    # ===================================================================
    def connect(self) -> None:
        if self._connection is not None:
            return

        logger.info(
            "mssql_connect_requested",
            host=self.host,
            port=self.port,
            database=self.database,
        )

        try:
            server = f"{self.host}:{self.port}" if self.port else self.host
            self._connection = pymssql.connect(
                server=server,
                user=self.user,
                password=self.password,
                database=self.database,
                as_dict=True,
            )

            logger.info(
                "mssql_connect_success",
                host=self.host,
                database=self.database,
            )

        except Exception as e:
            logger.error(
                "mssql_connect_failed",
                host=self.host,
                database=self.database,
                error=str(e),
                exc_info=True,
            )
            raise

    def disconnect(self) -> None:
        if self._connection:
            logger.debug("mssql_disconnect_requested")
            self._connection.close()
            self._connection = None
            logger.debug("mssql_disconnected")

    # ===================================================================
    # Test Connection
    # ===================================================================
    def test_connection(self) -> ConnectionTestResult:
        logger.info(
            "mssql_test_connection_requested",
            host=self.host,
            database=self.database,
        )

        try:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute("SELECT @@VERSION as version")
            row = cursor.fetchone()
            cursor.close()

            version = row["version"]

            logger.info(
                "mssql_test_connection_success",
                host=self.host,
                version=version,
            )

            return ConnectionTestResult(
                success=True,
                message="Connected",
                metadata={"version": version},
            )

        except Exception as e:
            logger.error(
                "mssql_test_connection_failed",
                host=self.host,
                database=self.database,
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(success=False, message=str(e))

        finally:
            self.disconnect()

    # ===================================================================
    # Schema Discovery
    # ===================================================================
    def discover_schema(self) -> Schema:
        logger.info(
            "mssql_schema_discovery_requested",
            database=self.database,
        )

        self.connect()
        cursor = self._connection.cursor()

        try:
            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
            """)

            table_list = cursor.fetchall()
            logger.debug(
                "mssql_schema_table_list_retrieved",
                count=len(table_list),
            )

            tables = []

            for tbl in table_list:
                schema_name = tbl["TABLE_SCHEMA"]
                table_name = tbl["TABLE_NAME"]

                logger.debug(
                    "mssql_schema_table_processing",
                    table=f"{schema_name}.{table_name}",
                )

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
                    name = col["COLUMN_NAME"]
                    sql_type = col["DATA_TYPE"]
                    nullable = col["IS_NULLABLE"] == "YES"
                    default = col["COLUMN_DEFAULT"]

                    mapped_type = self.TYPE_MAPPING.get(sql_type, DataType.STRING)

                    columns.append(
                        Column(
                            name=name,
                            data_type=mapped_type,
                            nullable=nullable,
                            default_value=default,
                        )
                    )

                cursor.execute(f"SELECT COUNT(*) as cnt FROM {schema_name}.{table_name}")
                count = cursor.fetchone()["cnt"]

                tables.append(
                    Table(
                        name=f"{schema_name}.{table_name}",
                        schema=schema_name,
                        columns=columns,
                        row_count=count,
                    )
                )

            logger.info(
                "mssql_schema_discovery_completed",
                table_count=len(tables),
            )

            return Schema(tables=tables)

        except Exception as e:
            logger.error(
                "mssql_schema_discovery_failed",
                error=str(e),
                exc_info=True,
            )
            return Schema(tables=[])

        finally:
            cursor.close()
            self.disconnect()

    # ===================================================================
    # Read Data
    # ===================================================================
    def read(
        self,
        stream: str,
        state: State | None = None,
        query: str | None = None,
    ) -> Iterator[Record]:
        logger.info(
            "mssql_read_requested",
            stream=stream,
            database=self.database,
            has_state=bool(state),
            has_query=bool(query),
        )

        self.connect()
        cursor = self._connection.cursor()

        try:
            # Build SQL query
            if query:
                sql = query
                params = []
            elif state and state.cursor_field:
                sql = (
                    f"SELECT * FROM {stream} "
                    f"WHERE {state.cursor_field} > %s "
                    f"ORDER BY {state.cursor_field}"
                )
                params = [state.cursor_value]
            else:
                sql = f"SELECT * FROM {stream}"
                params = []

            logger.debug(
                "mssql_read_query_prepared",
                sql_preview=sql[:250],
                param_count=len(params),
            )

            cursor.execute(sql, tuple(params))

            while True:
                rows = cursor.fetchmany(self._batch_size)
                if not rows:
                    break

                logger.debug(
                    "mssql_read_batch_fetched",
                    batch_size=len(rows),
                )

                for row in rows:
                    yield Record(stream=stream, data=row)

        except Exception as e:
            logger.error(
                "mssql_read_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            raise

        finally:
            cursor.close()
            self.disconnect()

    # ===================================================================
    # Record Count
    # ===================================================================
    def get_record_count(self, stream: str) -> int:
        logger.info(
            "mssql_record_count_requested",
            stream=stream,
            database=self.database,
        )

        self.connect()
        cursor = self._connection.cursor()

        try:
            cursor.execute(f"SELECT COUNT(*) as cnt FROM {stream}")
            count = cursor.fetchone()["cnt"]

            logger.info(
                "mssql_record_count_retrieved",
                stream=stream,
                record_count=count,
            )

            return count

        except Exception as e:
            logger.error(
                "mssql_record_count_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            raise

        finally:
            cursor.close()
            self.disconnect()
