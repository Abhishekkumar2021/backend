"""PostgreSQL Source Connector
"""

import logging
from collections.abc import Iterator
from typing import Any

import psycopg2
import psycopg2.extras

from app.connectors.base import Column, ConnectionTestResult, DataType, Record, Schema, SourceConnector, State, Table

logger = logging.getLogger(__name__)


class PostgreSQLSource(SourceConnector):
    """PostgreSQL source connector
    Supports full refresh and incremental syncs
    """

    # Map PostgreSQL types to standard DataType enum
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

    def __init__(self, config: dict[str, Any]):
        """Initialize PostgreSQL source connector

        Config format:
        {
            "host": "localhost",
            "port": 5432,
            "database": "mydb",
            "user": "postgres",
            "password": "secret",
            "schema": "public"  # Optional, defaults to public
        }
        """
        super().__init__(config)
        self.schema = config.get("schema", "public")
        self._batch_size = config.get("batch_size", 1000)

    def connect(self) -> None:
        """Establish connection to PostgreSQL"""
        if self._connection is None:
            try:
                self._connection = psycopg2.connect(
                    host=self.config["host"],
                    port=self.config.get("port", 5432),
                    database=self.config["database"],
                    user=self.config["user"],
                    password=self.config["password"],
                    connect_timeout=10,
                )
                logger.info(
                    f"Connected to PostgreSQL: {self.config['host']}:{self.config.get('port', 5432)}/{self.config['database']}"
                )
            except Exception as e:
                logger.error(f"Failed to connect to PostgreSQL: {e!s}")
                raise

    def disconnect(self) -> None:
        """Close PostgreSQL connection"""
        if self._connection:
            try:
                self._connection.close()
                logger.info("PostgreSQL connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {e!s}")
            finally:
                self._connection = None

    def test_connection(self) -> ConnectionTestResult:
        """Test PostgreSQL connection"""
        try:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            cursor.close()

            return ConnectionTestResult(success=True, message="Connected successfully", metadata={"version": version})
        except Exception as e:
            return ConnectionTestResult(success=False, message=f"Connection failed: {e!s}")
        finally:
            self.disconnect()

    def discover_schema(self) -> Schema:
        """Discover schema from PostgreSQL database
        Returns tables, columns, primary keys, and foreign keys
        """
        self.connect()
        cursor = self._connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        tables = []

        try:
            # Get all tables in schema
            cursor.execute(
                """
                SELECT 
                    table_name,
                    (SELECT obj_description(c.oid)) as table_comment
                FROM information_schema.tables t
                LEFT JOIN pg_class c ON c.relname = t.table_name
                WHERE table_schema = %s 
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """,
                (self.schema,),
            )

            table_rows = cursor.fetchall()

            for table_row in table_rows:
                table_name = table_row["table_name"]

                # Get columns for this table
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

                # Get primary keys
                cursor.execute(
                    """
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass AND i.indisprimary;
                """,
                    (f"{self.schema}.{table_name}",),
                )

                primary_keys = {row["attname"] for row in cursor.fetchall()}

                # Get foreign keys
                cursor.execute(
                    """
                    SELECT
                        kcu.column_name,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                        AND ccu.table_schema = tc.table_schema
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
                    data_type = self.TYPE_MAPPING.get(col_row["data_type"].lower(), DataType.STRING)

                    columns.append(
                        Column(
                            name=col_name,
                            data_type=data_type,
                            nullable=(col_row["is_nullable"] == "YES"),
                            primary_key=(col_name in primary_keys),
                            foreign_key=foreign_keys.get(col_name),
                            default_value=col_row["column_default"],
                            description=col_row["column_comment"],
                        )
                    )

                # Get row count estimate
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

            cursor.close()

            # Get PostgreSQL version
            cursor = self._connection.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0].split(",")[0]
            cursor.close()

            return Schema(tables=tables, version=version)

        except Exception as e:
            logger.error(f"Schema discovery failed: {e!s}")
            raise
        finally:
            self.disconnect()

    def read(self, stream: str, state: State | None = None, query: str | None = None) -> Iterator[Record]:
        """Read data from PostgreSQL table

        Args:
            stream: Table name to read from
            state: Optional state for incremental sync
            query: Optional custom SQL query

        Yields:
            Record objects with data

        """
        self.connect()
        cursor = self._connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:
            # Build query
            if query:
                # Use custom query
                sql = query
            elif state and state.cursor_field:
                # Incremental sync
                sql = f"""
                    SELECT * FROM {self.schema}.{stream}
                    WHERE {state.cursor_field} > %s
                    ORDER BY {state.cursor_field}
                """
                cursor.execute(sql, (state.cursor_value,))
            else:
                # Full refresh
                sql = f"SELECT * FROM {self.schema}.{stream}"

            if not query:
                cursor.execute(sql)

            # Fetch in batches
            while True:
                rows = cursor.fetchmany(self._batch_size)
                if not rows:
                    break

                for row in rows:
                    yield Record(stream=stream, data=dict(row))

            cursor.close()

        except Exception as e:
            logger.error(f"Failed to read from {stream}: {e!s}")
            raise
        finally:
            self.disconnect()

    def get_record_count(self, stream: str) -> int:
        """Get exact record count for a table"""
        self.connect()
        cursor = self._connection.cursor()

        try:
            cursor.execute(f"SELECT COUNT(*) FROM {self.schema}.{stream}")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            logger.error(f"Failed to get record count for {stream}: {e!s}")
            raise
        finally:
            self.disconnect()
