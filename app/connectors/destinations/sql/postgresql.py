"""PostgreSQL Destination Connector
"""

import logging
from collections.abc import Iterator
from typing import Any

import psycopg2
import psycopg2.extras

from app.connectors.base import Column, ConnectionTestResult, DataType, DestinationConnector, Record

logger = logging.getLogger(__name__)


class PostgreSQLDestination(DestinationConnector):
    """PostgreSQL destination connector
    Supports insert, upsert, and replace modes
    """

    # Reverse mapping: DataType -> PostgreSQL type
    TYPE_MAPPING = {
        DataType.INTEGER: "BIGINT",
        DataType.FLOAT: "DOUBLE PRECISION",
        DataType.STRING: "TEXT",
        DataType.BOOLEAN: "BOOLEAN",
        DataType.DATE: "DATE",
        DataType.DATETIME: "TIMESTAMP",
        DataType.JSON: "JSONB",
        DataType.BINARY: "BYTEA",
        DataType.NULL: "TEXT",
    }

    def __init__(self, config: dict[str, Any]):
        """Initialize PostgreSQL destination connector

        Config format:
        {
            "host": "localhost",
            "port": 5432,
            "database": "mydb",
            "user": "postgres",
            "password": "secret",
            "schema": "public",
            "write_mode": "insert"  # Options: insert, upsert, replace
        }
        """
        super().__init__(config)
        self.schema = config.get("schema", "public")
        self.write_mode = config.get("write_mode", "insert")

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
                self._connection.autocommit = False
                logger.info(f"Connected to PostgreSQL destination: {self.config['host']}")
            except Exception as e:
                logger.error(f"Failed to connect to PostgreSQL: {e!s}")
                raise

    def disconnect(self) -> None:
        """Close connection and commit pending transactions"""
        if self._connection:
            try:
                self._connection.commit()
                self._connection.close()
                logger.info("PostgreSQL destination connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {e!s}")
            finally:
                self._connection = None

    def test_connection(self) -> ConnectionTestResult:
        """Test PostgreSQL connection and write permissions"""
        try:
            self.connect()
            cursor = self._connection.cursor()

            # Test schema exists
            cursor.execute(
                """
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = %s
            """,
                (self.schema,),
            )

            if not cursor.fetchone():
                return ConnectionTestResult(success=False, message=f"Schema '{self.schema}' does not exist")

            # Test write permission
            cursor.execute(
                """
                SELECT has_schema_privilege(%s, 'CREATE');
            """,
                (self.schema,),
            )

            has_permission = cursor.fetchone()[0]
            cursor.close()

            if not has_permission:
                return ConnectionTestResult(success=False, message=f"No CREATE permission on schema '{self.schema}'")

            return ConnectionTestResult(success=True, message="Connected successfully with write permissions")

        except Exception as e:
            return ConnectionTestResult(success=False, message=f"Connection failed: {e!s}")
        finally:
            self.disconnect()

    def create_stream(self, stream: str, schema: list[Column]) -> None:
        """Create table if it doesn't exist

        Args:
            stream: Table name
            schema: List of Column objects

        """
        self.connect()
        cursor = self._connection.cursor()

        try:
            # Build column definitions
            col_defs = []
            for col in schema:
                pg_type = self.TYPE_MAPPING.get(col.data_type, "TEXT")
                nullable = "" if col.nullable else "NOT NULL"
                col_defs.append(f"{col.name} {pg_type} {nullable}")

            # Add primary key constraint
            pk_cols = [col.name for col in schema if col.primary_key]
            if pk_cols:
                col_defs.append(f"PRIMARY KEY ({', '.join(pk_cols)})")

            # Create table
            sql = f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{stream} (
                    {", ".join(col_defs)}
                )
            """

            cursor.execute(sql)
            self._connection.commit()
            cursor.close()

            logger.info(f"Table {self.schema}.{stream} created/verified")

        except Exception as e:
            self._connection.rollback()
            logger.error(f"Failed to create table {stream}: {e!s}")
            raise

    def write(self, records: Iterator[Record]) -> int:
        """Write records to PostgreSQL

        Args:
            records: Iterator of Record objects

        Returns:
            Number of records written

        """
        self.connect()
        cursor = self._connection.cursor()

        total_written = 0
        current_stream = None
        batch = []

        try:
            for record in records:
                # If stream changes, flush batch
                if current_stream and current_stream != record.stream:
                    total_written += self._write_batch(cursor, current_stream, batch)
                    batch = []

                current_stream = record.stream
                batch.append(record.data)

                # Flush batch when it reaches batch_size
                if len(batch) >= self._batch_size:
                    total_written += self._write_batch(cursor, current_stream, batch)
                    batch = []

            # Flush remaining records
            if batch:
                total_written += self._write_batch(cursor, current_stream, batch)

            self._connection.commit()
            cursor.close()

            logger.info(f"Successfully wrote {total_written} records")
            return total_written

        except Exception as e:
            self._connection.rollback()
            logger.error(f"Failed to write records: {e!s}")
            raise
        finally:
            self.disconnect()

    def _write_batch(self, cursor, stream: str, batch: list[dict[str, Any]]) -> int:
        """Write a batch of records to PostgreSQL"""
        if not batch:
            return 0

        # Get columns from first record
        columns = list(batch[0].keys())
        placeholders = ",".join(["%s"] * len(columns))

        if self.write_mode == "insert":
            # Simple insert
            sql = f"""
                INSERT INTO {self.schema}.{stream} ({",".join(columns)})
                VALUES ({placeholders})
            """

            for record in batch:
                values = [record.get(col) for col in columns]
                cursor.execute(sql, values)

        elif self.write_mode == "replace":
            # Truncate and insert
            cursor.execute(f"TRUNCATE TABLE {self.schema}.{stream}")

            sql = f"""
                INSERT INTO {self.schema}.{stream} ({",".join(columns)})
                VALUES ({placeholders})
            """

            for record in batch:
                values = [record.get(col) for col in columns]
                cursor.execute(sql, values)

        return len(batch)
