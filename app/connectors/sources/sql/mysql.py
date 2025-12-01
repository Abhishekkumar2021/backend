"""MySQL Source Connector
"""

from collections.abc import Iterator
from typing import Any

import pymysql
import pymysql.cursors

from app.connectors.base import (
    Column, ConnectionTestResult, DataType, Record,
    Schema, SourceConnector, State, Table
)
from app.core.logging import get_logger

logger = get_logger(__name__)


class MySQLSource(SourceConnector):
    """MySQL source connector"""

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

        logger.debug(
            "mysql_source_initialized",
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
            "mysql_connect_requested",
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
                cursorclass=pymysql.cursors.DictCursor,
            )

            logger.info(
                "mysql_connect_success",
                host=self.host,
                database=self.database,
            )

        except Exception as e:
            logger.error(
                "mysql_connect_failed",
                host=self.host,
                database=self.database,
                error=str(e),
                exc_info=True,
            )
            raise

    def disconnect(self) -> None:
        if self._connection:
            logger.debug("mysql_disconnect_requested")
            self._connection.close()
            self._connection = None
            logger.debug("mysql_disconnected")

    # ===================================================================
    # Test Connection
    # ===================================================================
    def test_connection(self) -> ConnectionTestResult:
        logger.info(
            "mysql_test_connection_requested",
            host=self.host,
            database=self.database,
        )

        try:
            self.connect()
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()["VERSION()"]

            logger.info(
                "mysql_test_connection_success",
                version=version,
            )

            return ConnectionTestResult(
                success=True,
                message="Connected",
                metadata={"version": version},
            )

        except Exception as e:
            logger.error(
                "mysql_test_connection_failed",
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
            "mysql_schema_discovery_requested",
            database=self.database,
        )

        self.connect()
        tables = []

        try:
            with self._connection.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                table_names = [list(row.values())[0] for row in cursor.fetchall()]

                logger.debug(
                    "mysql_schema_table_list_retrieved",
                    table_count=len(table_names),
                )

                for table_name in table_names:
                    logger.debug(
                        "mysql_schema_describe_table_started",
                        table=table_name,
                    )

                    cursor.execute(f"DESCRIBE `{table_name}`")
                    columns = []

                    for col_row in cursor.fetchall():
                        col_name = col_row["Field"]
                        col_type_raw = col_row["Type"].split("(")[0]
                        nullable = col_row["Null"] == "YES"
                        is_pk = col_row["Key"] == "PRI"

                        mapped_type = self.TYPE_MAPPING.get(col_type_raw, DataType.STRING)

                        columns.append(
                            Column(
                                name=col_name,
                                data_type=mapped_type,
                                nullable=nullable,
                                primary_key=is_pk,
                                default_value=col_row["Default"],
                            )
                        )

                    cursor.execute(f"SELECT COUNT(*) as cnt FROM `{table_name}`")
                    count = cursor.fetchone()["cnt"]

                    tables.append(
                        Table(
                            name=table_name,
                            columns=columns,
                            row_count=count,
                        )
                    )

                logger.info(
                    "mysql_schema_discovery_completed",
                    table_count=len(tables),
                )

                return Schema(tables=tables)

        except Exception as e:
            logger.error(
                "mysql_schema_discovery_failed",
                error=str(e),
                exc_info=True,
            )
            return Schema(tables=[])

        finally:
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
            "mysql_read_requested",
            stream=stream,
            database=self.database,
            has_state=bool(state),
            has_query=bool(query),
        )

        self.connect()

        try:
            with self._connection.cursor() as cursor:
                if query:
                    sql = query
                    params = []
                elif state and state.cursor_field:
                    sql = (
                        f"SELECT * FROM `{stream}` "
                        f"WHERE `{state.cursor_field}` > %s "
                        f"ORDER BY `{state.cursor_field}`"
                    )
                    params = [state.cursor_value]
                else:
                    sql = f"SELECT * FROM `{stream}`"
                    params = []

                logger.debug(
                    "mysql_read_query_prepared",
                    sql_preview=sql[:250],
                    param_count=len(params),
                )

                cursor.execute(sql, params)

                while True:
                    rows = cursor.fetchmany(self._batch_size)
                    if not rows:
                        break

                    logger.debug(
                        "mysql_read_batch_fetched",
                        batch_size=len(rows),
                    )

                    for row in rows:
                        yield Record(stream=stream, data=row)

        except Exception as e:
            logger.error(
                "mysql_read_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            raise

        finally:
            self.disconnect()

    # ===================================================================
    # Record Count
    # ===================================================================
    def get_record_count(self, stream: str) -> int:
        logger.info(
            "mysql_record_count_requested",
            stream=stream,
            database=self.database,
        )

        self.connect()

        try:
            with self._connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) as cnt FROM `{stream}`")
                count = cursor.fetchone()["cnt"]

                logger.info(
                    "mysql_record_count_retrieved",
                    stream=stream,
                    record_count=count,
                )

                return count

        except Exception as e:
            logger.error(
                "mysql_record_count_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            raise

        finally:
            self.disconnect()
