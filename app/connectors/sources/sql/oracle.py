"""Oracle Source Connector
"""

from collections.abc import Iterator
from typing import Any

import oracledb

from app.connectors.base import (
    Column, ConnectionTestResult, DataType, Record,
    Schema, SourceConnector, State, Table
)
from app.core.logging import get_logger

logger = get_logger(__name__)


class OracleSource(SourceConnector):
    """Oracle source connector"""

    TYPE_MAPPING = {
        "NUMBER": DataType.FLOAT,
        "INTEGER": DataType.INTEGER,
        "FLOAT": DataType.FLOAT,
        "VARCHAR2": DataType.STRING,
        "NVARCHAR2": DataType.STRING,
        "CHAR": DataType.STRING,
        "NCHAR": DataType.STRING,
        "CLOB": DataType.STRING,
        "BLOB": DataType.BINARY,
        "DATE": DataType.DATETIME,
        "TIMESTAMP": DataType.DATETIME,
        "TIMESTAMP WITH TIME ZONE": DataType.DATETIME,
        "TIMESTAMP WITH LOCAL TIME ZONE": DataType.DATETIME,
        "RAW": DataType.BINARY,
        "LONG": DataType.STRING,
    }

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.user = config["user"]
        self.password = config["password"]
        self.dsn = config["dsn"]
        self._batch_size = config.get("batch_size", 1000)
        self._connection = None

        logger.debug(
            "oracle_source_initialized",
            dsn=self.dsn,
            batch_size=self._batch_size,
        )

    # ===================================================================
    # Connection Handling
    # ===================================================================
    def connect(self) -> None:
        if self._connection is not None:
            return

        logger.info(
            "oracle_connect_requested",
            dsn=self.dsn,
        )

        try:
            self._connection = oracledb.connect(
                user=self.user,
                password=self.password,
                dsn=self.dsn,
            )

            logger.info(
                "oracle_connect_success",
                dsn=self.dsn,
            )
        except Exception as e:
            logger.error(
                "oracle_connect_failed",
                dsn=self.dsn,
                error=str(e),
                exc_info=True,
            )
            raise

    def disconnect(self) -> None:
        if not self._connection:
            return

        logger.debug("oracle_disconnect_requested")

        try:
            self._connection.close()
            logger.debug("oracle_disconnected")
        except Exception as e:
            logger.warning(
                "oracle_disconnect_failed",
                error=str(e),
                exc_info=True,
            )
        finally:
            self._connection = None

    # ===================================================================
    # Test Connection
    # ===================================================================
    def test_connection(self) -> ConnectionTestResult:
        logger.info("oracle_test_connection_requested", dsn=self.dsn)

        try:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute("SELECT * FROM v$version")
            version = cursor.fetchone()[0]
            cursor.close()

            logger.info(
                "oracle_test_connection_success",
                version=version,
            )

            return ConnectionTestResult(
                success=True,
                message="Connected successfully",
                metadata={"version": version},
            )

        except Exception as e:
            logger.error(
                "oracle_test_connection_failed",
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(
                success=False,
                message=f"Connection failed: {str(e)}",
            )

        finally:
            self.disconnect()

    # ===================================================================
    # Schema Discovery
    # ===================================================================
    def discover_schema(self) -> Schema:
        logger.info("oracle_schema_discovery_requested", dsn=self.dsn)

        self.connect()
        cursor = self._connection.cursor()
        tables = []

        try:
            cursor.execute("SELECT table_name FROM user_tables ORDER BY table_name")
            table_names = [row[0] for row in cursor.fetchall()]

            logger.debug(
                "oracle_schema_table_list_retrieved",
                table_count=len(table_names),
            )

            for table_name in table_names:
                logger.debug(
                    "oracle_schema_process_table",
                    table=table_name,
                )

                cursor.execute(
                    """
                    SELECT column_name, data_type, nullable, data_default
                    FROM user_tab_columns
                    WHERE table_name = :1
                    ORDER BY column_id
                    """,
                    [table_name],
                )

                columns = []
                for col_row in cursor.fetchall():
                    col_name = col_row[0]
                    col_type_raw = col_row[1].split("(")[0]
                    is_nullable = col_row[2] == "Y"

                    mapped_type = self.TYPE_MAPPING.get(col_type_raw, DataType.STRING)

                    columns.append(
                        Column(
                            name=col_name,
                            data_type=mapped_type,
                            nullable=is_nullable,
                            default_value=col_row[3],
                        )
                    )

                cursor.execute(
                    "SELECT num_rows FROM user_tables WHERE table_name = :1",
                    [table_name],
                )
                row_count_res = cursor.fetchone()
                row_count = row_count_res[0] if row_count_res else 0

                tables.append(
                    Table(
                        name=table_name,
                        columns=columns,
                        row_count=row_count,
                    )
                )

            logger.info(
                "oracle_schema_discovery_completed",
                table_count=len(tables),
            )

            return Schema(tables=tables)

        except Exception as e:
            logger.error(
                "oracle_schema_discovery_failed",
                error=str(e),
                exc_info=True,
            )
            raise

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
            "oracle_read_requested",
            stream=stream,
            dsn=self.dsn,
            has_state=bool(state),
            has_query=bool(query),
        )

        self.connect()
        cursor = self._connection.cursor()

        try:
            if query:
                sql = query
                params = []
            elif state and state.cursor_field:
                sql = (
                    f'SELECT * FROM "{stream}" '
                    f'WHERE "{state.cursor_field}" > :1 '
                    f'ORDER BY "{state.cursor_field}"'
                )
                params = [state.cursor_value]
            else:
                sql = f'SELECT * FROM "{stream}"'
                params = []

            logger.debug(
                "oracle_read_query_prepared",
                sql_preview=sql[:250],
                param_count=len(params),
            )

            cursor.execute(sql, params)

            column_names = [col[0] for col in cursor.description]

            while True:
                rows = cursor.fetchmany(self._batch_size)
                if not rows:
                    break

                logger.debug(
                    "oracle_read_batch_fetched",
                    batch_size=len(rows),
                )

                for row in rows:
                    data = dict(zip(column_names, row))
                    yield Record(stream=stream, data=data)

        except Exception as e:
            logger.error(
                "oracle_read_failed",
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
            "oracle_record_count_requested",
            stream=stream,
            dsn=self.dsn,
        )

        self.connect()
        cursor = self._connection.cursor()

        try:
            cursor.execute(f'SELECT COUNT(*) FROM "{stream}"')
            count = cursor.fetchone()[0]

            logger.info(
                "oracle_record_count_retrieved",
                stream=stream,
                record_count=count,
            )

            return count

        except Exception as e:
            logger.error(
                "oracle_record_count_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            raise

        finally:
            self.disconnect()
