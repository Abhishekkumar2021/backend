"""
Oracle Source Connector — Universal ETL Compatible
--------------------------------------------------
✓ Optional stream (table) — can also use query
✓ Incremental sync (state.cursor_field)
✓ Batch streaming
✓ Structured logging
✓ Optimized array-size fetches
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import oracledb

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
from app.schemas.connector_configs import OracleConfig

logger = get_logger(__name__)


class OracleSource(SourceConnector):
    """Oracle Source Connector"""

    # Oracle → Universal DataType mapping
    TYPE_MAPPING = {
        "NUMBER": DataType.FLOAT,
        "INTEGER": DataType.INTEGER,
        "FLOAT": DataType.FLOAT,
        "BINARY_FLOAT": DataType.FLOAT,
        "BINARY_DOUBLE": DataType.FLOAT,
        "VARCHAR2": DataType.STRING,
        "NVARCHAR2": DataType.STRING,
        "CHAR": DataType.STRING,
        "NCHAR": DataType.STRING,
        "CLOB": DataType.STRING,
        "NCLOB": DataType.STRING,
        "BLOB": DataType.BINARY,
        "RAW": DataType.BINARY,
        "DATE": DataType.DATETIME,
        "TIMESTAMP": DataType.DATETIME,
        "TIMESTAMP WITH TIME ZONE": DataType.DATETIME,
        "TIMESTAMP WITH LOCAL TIME ZONE": DataType.DATETIME,
        "LONG": DataType.STRING,
    }

    def __init__(self, config: OracleConfig):
        super().__init__(config)
        self.user = config.user
        self.password = config.password.get_secret_value()
        self.dsn = config.dsn
        self.batch_size = config.batch_size
        self._connection: oracledb.Connection | None = None

        logger.debug(
            "oracle_source_initialized",
            dsn=self.dsn,
            batch_size=self.batch_size,
        )

    # ------------------------------------------------------------------
    # Universal Source API
    # ------------------------------------------------------------------
    def supports_streams(self) -> bool:
        return True

    def supports_query(self) -> bool:
        return True

    def get_stream_identifier(self, config: dict) -> str:
        return config.get("stream") or config.get("table")

    # ------------------------------------------------------------------
    # Connection Handling
    # ------------------------------------------------------------------
    def connect(self) -> None:
        if self._connection:
            return

        logger.info("oracle_connecting", dsn=self.dsn)

        try:
            self._connection = oracledb.connect(
                user=self.user,
                password=self.password,
                dsn=self.dsn,
            )
            logger.info("oracle_connected", dsn=self.dsn)
        except Exception as e:
            logger.error("oracle_connect_failed", dsn=self.dsn, error=str(e), exc_info=True)
            raise

    def disconnect(self) -> None:
        if not self._connection:
            return

        try:
            self._connection.close()
            logger.debug("oracle_disconnected")
        except Exception as e:
            logger.warning("oracle_disconnect_failed", error=str(e), exc_info=True)
        finally:
            self._connection = None

    # ------------------------------------------------------------------
    # Test Connection
    # ------------------------------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        logger.info("oracle_test_connection_requested", dsn=self.dsn)

        try:
            self.connect()
            cur = self._connection.cursor()
            cur.execute("SELECT banner FROM v$version")
            version = cur.fetchone()[0]
            cur.close()

            return ConnectionTestResult(
                success=True,
                message="Connected successfully",
                metadata={"version": version},
            )
        except Exception as e:
            return ConnectionTestResult(False, f"Connection failed: {e}")
        finally:
            self.disconnect()

    # ------------------------------------------------------------------
    # Schema Discovery
    # ------------------------------------------------------------------
    def discover_schema(self) -> Schema:
        logger.info("oracle_schema_discovery_started", dsn=self.dsn)

        self.connect()
        cur = self._connection.cursor()

        try:
            cur.execute("SELECT table_name FROM user_tables ORDER BY table_name")
            tables = []
            for (table_name,) in cur.fetchall():
                tables.append(self._describe_table(table_name))

            logger.info(
                "oracle_schema_discovery_completed",
                table_count=len(tables),
            )

            return Schema(tables=tables)

        except Exception as e:
            logger.error("oracle_schema_discovery_failed", error=str(e), exc_info=True)
            raise
        finally:
            self.disconnect()

    def _describe_table(self, table_name: str) -> Table:
        cur = self._connection.cursor()
        try:
            cur.execute(
                """
                SELECT column_name, data_type, nullable, data_default
                FROM user_tab_columns
                WHERE table_name = :tbl
                ORDER BY column_id
                """,
                [table_name],
            )

            cols = []
            for col_name, raw_type, nullable, default_value in cur.fetchall():
                base_type = raw_type.split("(")[0]
                mapped = self.TYPE_MAPPING.get(base_type, DataType.STRING)

                cols.append(
                    Column(
                        name=col_name,
                        data_type=mapped,
                        nullable=(nullable == "Y"),
                        default_value=default_value,
                    )
                )

            cur.execute(
                "SELECT num_rows FROM user_tables WHERE table_name = :tbl",
                [table_name],
            )
            row_count = cur.fetchone()
            row_count = row_count[0] if row_count else 0

            return Table(
                name=table_name,
                columns=cols,
                row_count=row_count,
            )
        finally:
            cur.close()

    # ------------------------------------------------------------------
    # Record Count
    # ------------------------------------------------------------------
    def get_record_count(self, stream: str) -> int:
        logger.info("oracle_record_count_requested", stream=stream)

        self.connect()
        cur = self._connection.cursor()

        try:
            cur.execute(f'SELECT COUNT(*) FROM "{stream}"')
            return cur.fetchone()[0]
        except Exception as e:
            logger.error("oracle_record_count_failed", error=str(e), exc_info=True)
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
        logger.info(
            "oracle_read_started",
            stream=stream,
            has_state=bool(state),
            has_query=bool(query),
        )

        if not stream and not query:
            raise ValueError("OracleSource requires either stream (table) OR custom query.")

        self.connect()
        cur = self._connection.cursor()

        # Speed improvement for Oracle:
        cur.arraysize = self.batch_size

        try:
            # ------------------------------
            # Query Selection Logic
            # ------------------------------
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
                "oracle_read_sql",
                preview=sql[:250],
                param_count=len(params),
            )

            cur.execute(sql, params)
            column_names = [c[0] for c in cur.description]

            # ------------------------------
            # Stream results in batches
            # ------------------------------
            while True:
                rows = cur.fetchmany(self.batch_size)
                if not rows:
                    break

                logger.debug("oracle_batch_fetched", batch_size=len(rows))

                for row in rows:
                    yield Record(
                        stream=stream,
                        data=dict(zip(column_names, row)),
                    )

        except Exception as e:
            logger.error("oracle_read_failed", stream=stream, error=str(e), exc_info=True)
            raise

        finally:
            self.disconnect()
