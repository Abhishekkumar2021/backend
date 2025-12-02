"""
Universal ETL — MSSQL Source Connector
--------------------------------------
✓ Optional stream (table) or custom SQL query
✓ Incremental sync using state.cursor_field
✓ Streaming fetch with batch_size
✓ Structured logging everywhere
✓ Schema discovery using INFORMATION_SCHEMA
✓ Fully aligned with MySQL/Postgres/Oracle connectors
"""

from __future__ import annotations
from collections.abc import Iterator
from typing import Any

import pymssql

from app.connectors.base import (
    Column,
    ConnectionTestResult,
    DataType,
    Record,
    Schema,
    SourceConnector,
    State,
    Table,
)
from app.core.logging import get_logger
from app.schemas.connector_configs import MSSQLConfig

logger = get_logger(__name__)


class MSSQLSource(SourceConnector):
    """Universal ETL MSSQL Source Connector"""

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
        "date": DataType.DATE,
        "datetime": DataType.DATETIME,
        "smalldatetime": DataType.DATETIME,
        "datetime2": DataType.DATETIME,
        "datetimeoffset": DataType.DATETIME,
        "char": DataType.STRING,
        "varchar": DataType.STRING,
        "nvarchar": DataType.STRING,
        "nchar": DataType.STRING,
        "text": DataType.STRING,
        "ntext": DataType.STRING,
        "binary": DataType.BINARY,
        "varbinary": DataType.BINARY,
        "image": DataType.BINARY,
        "time": DataType.STRING,
    }

    # ------------------------------------------
    # Universal metadata capabilities
    # ------------------------------------------
    def supports_streams(self) -> bool:
        return True

    def supports_query(self) -> bool:
        return True

    def get_stream_identifier(self, config: dict) -> str:
        return config.get("stream") or config.get("table")

    # ------------------------------------------
    # Initialization
    # ------------------------------------------
    def __init__(self, config: MSSQLConfig):
        super().__init__(config)

        self.host = config.host
        self.port = config.port
        self.user = config.user
        self.password = config.password.get_secret_value()
        self.database = config.database
        self.batch_size = config.batch_size

        self._connection = None

        logger.debug(
            "mssql_source_initialized",
            host=self.host,
            port=self.port,
            database=self.database,
            batch_size=self.batch_size,
        )

    # ------------------------------------------
    # Connection Functions
    # ------------------------------------------
    def connect(self) -> None:
        if self._connection:
            return

        logger.info(
            "mssql_connecting",
            host=self.host,
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

            logger.info("mssql_connected", database=self.database)

        except Exception as e:
            logger.error("mssql_connect_failed", error=str(e), exc_info=True)
            raise

    def disconnect(self) -> None:
        if not self._connection:
            return

        try:
            self._connection.close()
            logger.debug("mssql_disconnected")
        except Exception as e:
            logger.warning("mssql_disconnect_failed", error=str(e), exc_info=True)
        finally:
            self._connection = None

    # ------------------------------------------
    # Test Connection
    # ------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        logger.info("mssql_test_connection_started", host=self.host)

        try:
            self.connect()
            cur = self._connection.cursor()
            cur.execute("SELECT @@VERSION AS version")
            version = cur.fetchone()["version"]
            cur.close()

            return ConnectionTestResult(
                True,
                "Connected successfully",
                metadata={"version": version},
            )

        except Exception as e:
            logger.error("mssql_test_connection_failed", error=str(e), exc_info=True)
            return ConnectionTestResult(False, str(e))

        finally:
            self.disconnect()

    # ------------------------------------------
    # Schema Discovery
    # ------------------------------------------
    def discover_schema(self) -> Schema:
        logger.info("mssql_schema_discovery_started", database=self.database)

        self.connect()
        cur = self._connection.cursor()

        try:
            # List tables
            cur.execute(
                """
                SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE='BASE TABLE'
                ORDER BY TABLE_SCHEMA, TABLE_NAME
                """
            )
            tables_raw = cur.fetchall()

            logger.debug("mssql_tables_listed", count=len(tables_raw))

            tables = []

            for row in tables_raw:
                schema = row["TABLE_SCHEMA"]
                table = row["TABLE_NAME"]
                fq_name = f"{schema}.{table}"

                tables.append(self._describe_table(cur, schema, table, fq_name))

            logger.info("mssql_schema_discovery_completed", table_count=len(tables))
            return Schema(tables=tables)

        except Exception as e:
            logger.error("mssql_schema_discovery_failed", error=str(e), exc_info=True)
            return Schema(tables=[])

        finally:
            cur.close()
            self.disconnect()

    def _describe_table(self, cur, schema: str, table: str, fq_name: str) -> Table:
        cur.execute(
            """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
            ORDER BY ORDINAL_POSITION
            """,
            (schema, table),
        )
        col_rows = cur.fetchall()

        columns = []
        for col in col_rows:
            sql_type = col["DATA_TYPE"]
            mapped = self.TYPE_MAPPING.get(sql_type.lower(), DataType.STRING)

            columns.append(
                Column(
                    name=col["COLUMN_NAME"],
                    data_type=mapped,
                    nullable=(col["IS_NULLABLE"] == "YES"),
                    default_value=col["COLUMN_DEFAULT"],
                )
            )

        # Row count
        cur.execute(f"SELECT COUNT(*) AS cnt FROM {schema}.{table}")
        row_count = cur.fetchone()["cnt"]

        return Table(
            name=fq_name,
            schema=schema,
            columns=columns,
            row_count=row_count,
        )

    # ------------------------------------------
    # Read Data (Streaming)
    # ------------------------------------------
    def read(
        self,
        stream: str,
        state: State | None = None,
        query: str | None = None,
    ) -> Iterator[Record]:

        if not stream and not query:
            raise ValueError("MSSQLSource requires either stream or query.")

        logger.info(
            "mssql_read_started",
            stream=stream,
            has_state=bool(state),
            has_query=bool(query),
        )

        self.connect()
        cur = self._connection.cursor()

        try:
            # -------------------------
            # Select SQL
            # -------------------------
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

            logger.debug("mssql_sql_prepared", sql_preview=sql[:250], params=params)

            cur.execute(sql, params)

            # -------------------------
            # Stream rows
            # -------------------------
            while True:
                rows = cur.fetchmany(self.batch_size)
                if not rows:
                    break

                logger.debug("mssql_batch_fetched", batch_size=len(rows))

                for row in rows:
                    yield Record(stream=stream, data=row)

        except Exception as e:
            logger.error("mssql_read_failed", error=str(e), exc_info=True)
            raise

        finally:
            cur.close()
            self.disconnect()

    # ------------------------------------------
    # Record Count
    # ------------------------------------------
    def get_record_count(self, stream: str) -> int:
        logger.info("mssql_record_count_requested", stream=stream)

        self.connect()
        cur = self._connection.cursor()

        try:
            cur.execute(f"SELECT COUNT(*) AS cnt FROM {stream}")
            return cur.fetchone()["cnt"]

        except Exception as e:
            logger.error("mssql_record_count_failed", error=str(e), exc_info=True)
            raise

        finally:
            cur.close()
            self.disconnect()
