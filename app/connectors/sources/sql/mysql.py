"""
MySQL Source Connector — Universal ETL Compatible
-------------------------------------------------
✓ Optional stream (table) — or custom SQL query
✓ Incremental sync (state.cursor_field)
✓ Streaming fetch with cursor arraysize
✓ Structured logging everywhere
✓ Schema discovery using INFORMATION_SCHEMA
"""

from __future__ import annotations
from collections.abc import Iterator
from typing import Any

import pymysql
import pymysql.cursors

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
from app.schemas.connector_configs import MySQLConfig

logger = get_logger(__name__)


class MySQLSource(SourceConnector):
    """Universal ETL MySQL Source Connector"""

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

    # ------------------------------------------------------------------
    # Universal Metadata
    # ------------------------------------------------------------------
    def supports_streams(self) -> bool:
        return True

    def supports_query(self) -> bool:
        return True

    def get_stream_identifier(self, config: dict) -> str:
        return config.get("stream") or config.get("table")

    # ------------------------------------------------------------------
    # Init
    # ------------------------------------------------------------------
    def __init__(self, config: MySQLConfig):
        super().__init__(config)

        self.host = config.host
        self.port = config.port
        self.user = config.user
        self.password = config.password.get_secret_value()
        self.database = config.database
        self.batch_size = config.batch_size

        self._connection = None

        logger.debug(
            "mysql_source_initialized",
            host=self.host,
            port=self.port,
            database=self.database,
            batch_size=self.batch_size,
        )

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------
    def connect(self) -> None:
        if self._connection:
            return

        logger.info("mysql_connecting", host=self.host, database=self.database)

        try:
            self._connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=False,
            )
            logger.info("mysql_connected", database=self.database)
        except Exception as e:
            logger.error("mysql_connect_failed", error=str(e), exc_info=True)
            raise

    def disconnect(self) -> None:
        if not self._connection:
            return

        try:
            self._connection.close()
            logger.debug("mysql_disconnected")
        except Exception as e:
            logger.warning("mysql_disconnect_failed", error=str(e), exc_info=True)
        finally:
            self._connection = None

    # ------------------------------------------------------------------
    # Test Connection
    # ------------------------------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        logger.info("mysql_test_connection_started", host=self.host)

        try:
            self.connect()
            with self._connection.cursor() as cur:
                cur.execute("SELECT VERSION() AS version")
                version = cur.fetchone()["version"]

            return ConnectionTestResult(
                True,
                "Connected successfully",
                metadata={"version": version},
            )

        except Exception as e:
            logger.error("mysql_test_failed", error=str(e), exc_info=True)
            return ConnectionTestResult(False, str(e))

        finally:
            self.disconnect()

    # ------------------------------------------------------------------
    # Schema Discovery
    # ------------------------------------------------------------------
    def discover_schema(self) -> Schema:
        logger.info("mysql_schema_discovery_started", database=self.database)

        self.connect()
        tables = []

        try:
            with self._connection.cursor() as cur:
                # List tables
                cur.execute("SHOW TABLES")
                table_names = [list(row.values())[0] for row in cur.fetchall()]

                logger.debug("mysql_tables_listed", count=len(table_names))

                for table in table_names:
                    tables.append(self._describe_table(cur, table))

            logger.info("mysql_schema_discovery_completed", table_count=len(tables))
            return Schema(tables=tables)

        except Exception as e:
            logger.error("mysql_schema_discovery_failed", error=str(e), exc_info=True)
            return Schema(tables=[])

        finally:
            self.disconnect()

    def _describe_table(self, cur, table: str) -> Table:
        cur.execute(f"DESCRIBE `{table}`")
        col_rows = cur.fetchall()

        cols = []
        for row in col_rows:
            raw_type = row["Type"].split("(")[0]
            mapped = self.TYPE_MAPPING.get(raw_type, DataType.STRING)

            cols.append(
                Column(
                    name=row["Field"],
                    data_type=mapped,
                    nullable=(row["Null"] == "YES"),
                    primary_key=(row["Key"] == "PRI"),
                    default_value=row["Default"],
                )
            )

        cur.execute(f"SELECT COUNT(*) AS cnt FROM `{table}`")
        count = cur.fetchone()["cnt"]

        return Table(
            name=table,
            columns=cols,
            row_count=count,
        )

    # ------------------------------------------------------------------
    # Record Count
    # ------------------------------------------------------------------
    def get_record_count(self, stream: str) -> int:
        logger.info("mysql_record_count_requested", stream=stream)

        self.connect()
        try:
            with self._connection.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) AS cnt FROM `{stream}`")
                return cur.fetchone()["cnt"]
        except Exception as e:
            logger.error("mysql_record_count_failed", error=str(e), exc_info=True)
            raise
        finally:
            self.disconnect()

    # ------------------------------------------------------------------
    # Streaming Read
    # ------------------------------------------------------------------
    def read(
        self,
        stream: str,
        state: State | None = None,
        query: str | None = None,
    ) -> Iterator[Record]:

        if not stream and not query:
            raise ValueError("MySQLSource requires either stream or query.")

        logger.info(
            "mysql_read_started",
            stream=stream,
            has_state=bool(state),
            has_query=bool(query),
        )

        self.connect()

        try:
            with self._connection.cursor() as cur:
                # -------------------------
                # Choose SQL
                # -------------------------
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

                logger.debug("mysql_sql_prepared", sql_preview=sql[:250], params=params)

                # Execute
                cur.execute(sql, params)

                # -------------------------
                # Stream rows
                # -------------------------
                while True:
                    rows = cur.fetchmany(self.batch_size)
                    if not rows:
                        break

                    logger.debug("mysql_batch_fetched", batch_size=len(rows))

                    for row in rows:
                        yield Record(stream=stream, data=row)

        except Exception as e:
            logger.error("mysql_read_failed", error=str(e), exc_info=True)
            raise

        finally:
            self.disconnect()
