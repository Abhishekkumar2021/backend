"""
SQLite Source Connector — Universal ETL Compatible
--------------------------------------------------

Implements universal connector contract:
✓ get_stream_identifier
✓ get_query
✓ supports_streams/query
✓ state-based incremental extraction
✓ safe connection lifecycle
"""

import sqlite3
from pathlib import Path
from typing import Any, Iterator

from app.connectors.base import (
    Record,
    State,
    Schema,
    Table,
    Column,
    DataType,
    ConnectionTestResult,
    SourceConnector,
)
from app.schemas.connector_configs import SQLiteConfig
from app.core.logging import get_logger

logger = get_logger(__name__)


TYPE_MAPPING = {
    "INTEGER": DataType.INTEGER,
    "INT": DataType.INTEGER,
    "TINYINT": DataType.INTEGER,
    "SMALLINT": DataType.INTEGER,
    "MEDIUMINT": DataType.INTEGER,
    "BIGINT": DataType.INTEGER,
    "REAL": DataType.FLOAT,
    "DOUBLE": DataType.FLOAT,
    "FLOAT": DataType.FLOAT,
    "NUMERIC": DataType.FLOAT,
    "DECIMAL": DataType.FLOAT,
    "TEXT": DataType.STRING,
    "VARCHAR": DataType.STRING,
    "CHAR": DataType.STRING,
    "CLOB": DataType.STRING,
    "BLOB": DataType.BINARY,
    "BOOLEAN": DataType.BOOLEAN,
    "DATE": DataType.DATE,
    "DATETIME": DataType.DATETIME,
    "TIMESTAMP": DataType.DATETIME,
}


class SQLiteSource(SourceConnector):
    """SQLite source connector (universal ETL compatible)."""

    # ------------------------------------------------------------------
    # INIT
    # ------------------------------------------------------------------
    def __init__(self, config: SQLiteConfig):
        super().__init__(config)
        self.database_path = config.database_path
        self.batch_size = config.batch_size
        self._connection = None

        logger.debug(
            "sqlite_source_initialized",
            database_path=self.database_path,
            batch_size=self.batch_size,
        )

    # ------------------------------------------------------------------
    # UNIVERSAL CONNECTOR API
    # ------------------------------------------------------------------
    def get_stream_identifier(self, config: dict) -> str | None:
        """SQLite requires a table name."""
        stream = config.get("stream")
        if not stream:
            raise ValueError(
                "SQLiteSource requires 'stream' (table name) in source_config"
            )
        return stream

    def get_query(self, config: dict) -> str | None:
        """Return optional SQL query."""
        return config.get("query")

    def supports_streams(self) -> bool:
        return True

    def supports_query(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # CONNECTION
    # ------------------------------------------------------------------
    def connect(self):
        if self._connection:
            return

        try:
            if self.database_path != ":memory:":
                if not Path(self.database_path).exists():
                    raise FileNotFoundError(
                        f"SQLite file not found: {self.database_path}"
                    )

            self._connection = sqlite3.connect(
                self.database_path, check_same_thread=False
            )
            self._connection.row_factory = sqlite3.Row

            logger.info("sqlite_connected", database=self.database_path)

        except Exception as e:
            logger.error("sqlite_connect_failed", error=str(e), exc_info=True)
            raise

    def disconnect(self):
        if not self._connection:
            return

        try:
            self._connection.close()
        except Exception as e:
            logger.warning("sqlite_disconnect_failed", error=str(e))
        finally:
            self._connection = None

    # ------------------------------------------------------------------
    # TEST CONNECTION
    # ------------------------------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        try:
            self.connect()
            version = self._connection.execute("SELECT sqlite_version();").fetchone()[0]
            return ConnectionTestResult(
                success=True,
                message="Connected successfully",
                metadata={"version": version},
            )
        except Exception as e:
            return ConnectionTestResult(success=False, message=str(e))
        finally:
            self.disconnect()

    # ------------------------------------------------------------------
    # SCHEMA DISCOVERY
    # ------------------------------------------------------------------
    def discover_schema(self) -> Schema:
        self.connect()
        cursor = self._connection.cursor()

        tables = []

        try:
            cursor.execute(
                """
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%';
            """
            )

            table_names = [row[0] for row in cursor.fetchall()]

            for tbl in table_names:
                # Columns
                cursor.execute(f"PRAGMA table_info('{tbl}');")
                col_rows = cursor.fetchall()

                # Foreign keys
                cursor.execute(f"PRAGMA foreign_key_list('{tbl}');")
                fk_map = {row[3]: f"{row[2]}.{row[4]}" for row in cursor.fetchall()}

                columns = []
                for col in col_rows:
                    col_type = col[2].upper().split("(")[0]
                    columns.append(
                        Column(
                            name=col[1],
                            data_type=TYPE_MAPPING.get(col_type, DataType.STRING),
                            nullable=(col[3] == 0),
                            primary_key=(col[5] > 0),
                            foreign_key=fk_map.get(col[1]),
                            default_value=col[4],
                        )
                    )

                row_count = self._connection.execute(
                    f'SELECT COUNT(*) FROM "{tbl}"'
                ).fetchone()[0]

                tables.append(Table(name=tbl, columns=columns, row_count=row_count))

            version = self._connection.execute("SELECT sqlite_version();").fetchone()[0]
            return Schema(tables=tables, version=f"SQLite {version}")

        finally:
            self.disconnect()

    # ------------------------------------------------------------------
    # READ (STATE + QUERY)
    # ------------------------------------------------------------------
    def read(
        self,
        stream: str,
        state: State | None = None,
        query: str | None = None,
    ) -> Iterator[Record]:

        self.connect()

        sql = None
        params = ()

        if query:
            sql = query

        elif state and state.cursor_field:
            sql = (
                f'SELECT * FROM "{stream}" '
                f'WHERE "{state.cursor_field}" > ? '
                f'ORDER BY "{state.cursor_field}"'
            )
            params = (state.cursor_value,)

        else:
            sql = f'SELECT * FROM "{stream}"'

        cursor = self._connection.execute(sql, params)

        try:
            while True:
                rows = cursor.fetchmany(self.batch_size)
                if not rows:
                    break

                for row in rows:
                    yield Record(stream=stream, data=dict(row))

        finally:
            cursor.close()
            self.disconnect()

    # ------------------------------------------------------------------
    # COUNT
    # ------------------------------------------------------------------
    def get_record_count(self, stream: str) -> int:
        self.connect()
        try:
            row = self._connection.execute(
                f'SELECT COUNT(*) FROM "{stream}"'
            ).fetchone()
            return row[0]
        finally:
            self.disconnect()
