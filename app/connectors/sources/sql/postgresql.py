"""PostgreSQL Source Connector
"""

from collections.abc import Iterator
from typing import Any

import psycopg2
import psycopg2.extras

from app.connectors.base import (
    Column, ConnectionTestResult, DataType,
    Record, Schema, SourceConnector, State, Table
)
from app.core.logging import get_logger
from app.schemas.connector_configs import PostgresConfig

logger = get_logger(__name__)


class PostgreSQLSource(SourceConnector):
    """PostgreSQL source connector"""

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
        "time": DataType.STRING,
        "json": DataType.JSON,
        "jsonb": DataType.JSON,
        "uuid": DataType.STRING,
        "bytea": DataType.BINARY,
    }

    def __init__(self, config: PostgresConfig):
        super().__init__(config)
        self.schema = config.schema_
        self._batch_size = config.batch_size
        self._connection = None

        logger.debug(
            "postgres_source_initialized",
            host=config.host,
            database=config.database,
            schema=self.schema,
            batch_size=self._batch_size,
        )

    # ===================================================================
    # Connection Handling
    # ===================================================================
    def connect(self) -> None:
        if self._connection:
            return

        logger.info(
            "postgres_connect_requested",
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
        )

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
                "postgres_connect_success",
                host=self.config.host,
                database=self.config.database,
            )

        except Exception as e:
            logger.error(
                "postgres_connect_failed",
                error=str(e),
                exc_info=True,
            )
            raise

    def disconnect(self) -> None:
        if not self._connection:
            return

        logger.debug("postgres_disconnect_requested")

        try:
            self._connection.close()
            logger.debug("postgres_disconnected")
        except Exception as e:
            logger.warning(
                "postgres_disconnect_failed",
                error=str(e),
                exc_info=True,
            )
        finally:
            self._connection = None

    # ===================================================================
    # Test Connection
    # ===================================================================
    def test_connection(self) -> ConnectionTestResult:
        logger.info(
            "postgres_test_connection_requested",
            host=self.config.host,
            database=self.config.database,
        )

        try:
            self.connect()

            cursor = self._connection.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            cursor.close()

            logger.info(
                "postgres_test_connection_success",
                version=version,
            )

            return ConnectionTestResult(
                success=True,
                message="Connected successfully",
                metadata={"version": version},
            )

        except Exception as e:
            logger.error(
                "postgres_test_connection_failed",
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
        logger.info(
            "postgres_schema_discovery_requested",
            schema=self.schema,
            database=self.config.database,
        )

        self.connect()
        cursor = self._connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        tables = []

        try:
            cursor.execute(
                """
                SELECT 
                    table_name,
                    (SELECT obj_description(c.oid)) as table_comment
                FROM information_schema.tables t
                LEFT JOIN pg_class c ON c.relname = t.table_name
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name;
                """,
                (self.schema,),
            )

            table_rows = cursor.fetchall()

            logger.debug(
                "postgres_schema_table_list_retrieved",
                table_count=len(table_rows),
            )

            for table_row in table_rows:
                table_name = table_row["table_name"]

                logger.debug(
                    "postgres_schema_processing_table",
                    table=table_name,
                )

                # Columns
                cursor.execute(
                    """
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        character_maximum_length,
                        col_description((table_schema||'.'||table_name)::regclass::oid, ordinal_position) as column_comment
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position;
                    """,
                    (self.schema, table_name),
                )
                column_rows = cursor.fetchall()

                # Primary keys
                cursor.execute(
                    """
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a 
                        ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass AND i.indisprimary;
                    """,
                    (f"{self.schema}.{table_name}",),
                )
                primary_keys = {row["attname"] for row in cursor.fetchall()}

                # Foreign keys
                cursor.execute(
                    """
                    SELECT
                        kcu.column_name,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                        AND tc.table_schema = %s
                        AND tc.table_name = %s;
                    """,
                    (self.schema, table_name),
                )

                foreign_keys = {
                    row["column_name"]: f"{row['foreign_table_name']}.{row['foreign_column_name']}"
                    for row in cursor.fetchall()
                }

                # Build columns
                columns = []
                for col_row in column_rows:
                    col_name = col_row["column_name"]
                    pg_type = col_row["data_type"].lower()

                    mapped_type = self.TYPE_MAPPING.get(pg_type, DataType.STRING)

                    columns.append(
                        Column(
                            name=col_name,
                            data_type=mapped_type,
                            nullable=(col_row["is_nullable"] == "YES"),
                            primary_key=(col_name in primary_keys),
                            foreign_key=foreign_keys.get(col_name),
                            default_value=col_row["column_default"],
                            description=col_row["column_comment"],
                        )
                    )

                # Row count estimate
                cursor.execute(
                    """
                    SELECT reltuples::bigint AS estimate
                    FROM pg_class
                    WHERE oid = %s::regclass;
                    """,
                    (f"{self.schema}.{table_name}",),
                )
                row_count = cursor.fetchone()["estimate"]

                tables.append(
                    Table(
                        name=table_name,
                        schema=self.schema,
                        columns=columns,
                        row_count=row_count,
                        description=table_row["table_comment"],
                    )
                )

            # PostgreSQL version
            cursor = self._connection.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0].split(",")[0]

            logger.info(
                "postgres_schema_discovery_completed",
                table_count=len(tables),
                version=version,
            )

            return Schema(tables=tables, version=version)

        except Exception as e:
            logger.error(
                "postgres_schema_discovery_failed",
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
            "postgres_read_requested",
            stream=stream,
            schema=self.schema,
            has_state=bool(state),
            has_query=bool(query),
        )

        self.connect()
        cursor = self._connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:
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

            logger.debug(
                "postgres_read_query_prepared",
                sql_preview=sql[:250],
                param_count=len(params),
            )

            cursor.execute(sql, params)

            while True:
                rows = cursor.fetchmany(self._batch_size)
                if not rows:
                    break

                logger.debug(
                    "postgres_read_batch_fetched",
                    batch_size=len(rows),
                )

                for row in rows:
                    yield Record(stream=stream, data=dict(row))

        except Exception as e:
            logger.error(
                "postgres_read_failed",
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
            "postgres_record_count_requested",
            stream=stream,
            schema=self.schema,
        )

        self.connect()
        cursor = self._connection.cursor()

        try:
            cursor.execute(f"SELECT COUNT(*) FROM {self.schema}.{stream}")
            count = cursor.fetchone()[0]

            logger.info(
                "postgres_record_count_retrieved",
                stream=stream,
                record_count=count,
            )

            return count

        except Exception as e:
            logger.error(
                "postgres_record_count_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            raise

        finally:
            self.disconnect()
