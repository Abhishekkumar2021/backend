"""
PostgreSQL Source Connector — Universal ETL Compatible
------------------------------------------------------
Implements the universal contract:
✓ get_stream_identifier
✓ get_query
✓ supports_streams/query
✓ state-based incremental reading
✓ safe connection lifecycle
"""

from __future__ import annotations

from typing import Iterator, Any
import psycopg2
import psycopg2.extras

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
from app.schemas.connector_configs import PostgresConfig
from app.core.logging import get_logger

logger = get_logger(__name__)


class PostgreSQLSource(SourceConnector):

    TYPE_MAPPING = {
        "integer": DataType.INTEGER,
        "bigint": DataType.INTEGER,
        "smallint": DataType.INTEGER,
        "numeric": DataType.FLOAT,
        "real": DataType.FLOAT,
        "double precision": DataType.FLOAT,
        "decimal": DataType.FLOAT,
        "money": DataType.FLOAT,
        "character varying": DataType.STRING,
        "varchar": DataType.STRING,
        "character": DataType.STRING,
        "char": DataType.STRING,
        "text": DataType.STRING,
        "boolean": DataType.BOOLEAN,
        "date": DataType.DATE,
        "timestamp": DataType.DATETIME,
        "timestamp without time zone": DataType.DATETIME,
        "timestamp with time zone": DataType.DATETIME,
        "json": DataType.JSON,
        "jsonb": DataType.JSON,
        "uuid": DataType.STRING,
        "bytea": DataType.BINARY,
    }

    # ------------------------------------------------------------------
    # INIT
    # ------------------------------------------------------------------
    def __init__(self, config: PostgresConfig):
        super().__init__(config)
        self.schema = config.schema_
        self.batch_size = config.batch_size
        self._connection = None

        logger.debug(
            "postgres_source_initialized",
            host=config.host,
            database=config.database,
            schema=self.schema,
            batch_size=self.batch_size,
        )

    # ------------------------------------------------------------------
    # UNIVERSAL CONNECTOR API
    # ------------------------------------------------------------------
    def get_stream_identifier(self, config: dict) -> str | None:
        """PostgreSQL requires a 'stream' (table name) unless query is provided."""
        stream = config.get("stream")
        query = config.get("query")

        if not stream and not query:
            raise ValueError(
                "PostgreSQLSource requires either 'stream' (table name) "
                "or a custom 'query' in source_config."
            )

        return stream

    def get_query(self, config: dict) -> str | None:
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
            self._connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password.get_secret_value(),
                connect_timeout=10,
            )
            logger.info(
                "postgres_connected",
                host=self.config.host,
                database=self.config.database,
            )

        except Exception as e:
            logger.error("postgres_connect_failed", error=str(e), exc_info=True)
            raise

    def disconnect(self):
        if not self._connection:
            return

        try:
            self._connection.close()
        except Exception:
            logger.warning("postgres_disconnect_failed")
        finally:
            self._connection = None

    # ------------------------------------------------------------------
    # TEST CONNECTION
    # ------------------------------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        try:
            self.connect()
            version = self._connection.cursor().execute("SELECT version();").fetchone()[0]
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
        cursor = self._connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name;
                """,
                (self.schema,),
            )
            tables_meta = cursor.fetchall()

            tables = []

            for tbl in tables_meta:
                name = tbl["table_name"]

                # Columns
                cursor.execute(
                    """
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position;
                    """,
                    (self.schema, name),
                )
                columns_raw = cursor.fetchall()

                # PKs
                cursor.execute(
                    """
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a
                      ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass
                      AND i.indisprimary;
                    """,
                    (f"{self.schema}.{name}",),
                )
                pk_names = {row["attname"] for row in cursor.fetchall()}

                columns = []
                for col in columns_raw:
                    pg_type = col["data_type"].lower()
                    mapped = self.TYPE_MAPPING.get(pg_type, DataType.STRING)

                    columns.append(
                        Column(
                            name=col["column_name"],
                            data_type=mapped,
                            nullable=(col["is_nullable"] == "YES"),
                            primary_key=col["column_name"] in pk_names,
                            default_value=col["column_default"],
                        )
                    )

                # row count estimate
                cursor.execute(
                    "SELECT reltuples::bigint AS estimate FROM pg_class WHERE oid = %s::regclass;",
                    (f"{self.schema}.{name}",),
                )
                row_count = cursor.fetchone()["estimate"]

                tables.append(Table(name=name, schema=self.schema, columns=columns, row_count=row_count))

            # version
            version = (
                self._connection.cursor().execute("SELECT version();").fetchone()[0].split(",")[0]
            )

            return Schema(tables=tables, version=version)

        finally:
            cursor.close()
            self.disconnect()

    # ------------------------------------------------------------------
    # READ
    # ------------------------------------------------------------------
    def read(
        self,
        stream: str,
        state: State | None = None,
        query: str | None = None,
    ) -> Iterator[Record]:

        self.connect()
        cursor = self._connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Determine SQL
        if query:
            sql = query
            params = ()

        elif state and state.cursor_field:
            sql = (
                f"SELECT * FROM {self.schema}.{stream} "
                f"WHERE {state.cursor_field} > %s "
                f"ORDER BY {state.cursor_field}"
            )
            params = (state.cursor_value,)

        else:
            sql = f"SELECT * FROM {self.schema}.{stream}"
            params = ()

        try:
            cursor.execute(sql, params)

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
            cursor = self._connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {self.schema}.{stream}")
            return cursor.fetchone()[0]
        finally:
            cursor.close()
            self.disconnect()
