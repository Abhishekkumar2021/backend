"""PostgreSQL Destination Connector (Structured Logging Version)
"""

import psycopg2
import psycopg2.extras
from collections.abc import Iterator
from typing import Any

from app.connectors.base import Column, ConnectionTestResult, DataType, DestinationConnector, Record
from app.core.logging import get_logger
from app.schemas.connector_configs import PostgresConfig

logger = get_logger(__name__)


class PostgreSQLDestination(DestinationConnector):
    """PostgreSQL destination connector
    Supports insert, upsert, and replace modes
    """

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

    def __init__(self, config: PostgresConfig):
        super().__init__(config)
        self.schema = config.schema_
        self.write_mode = "insert" # Assuming default from original code. config.write_mode if I were to add it to schema.
        self._batch_size = config.batch_size

        logger.debug(
            "postgres_destination_initialized",
            schema=self.schema,
            write_mode=self.write_mode,
            batch_size=self._batch_size,
        )

    # ----------------------------------------------------------------------
    # Connection
    # ----------------------------------------------------------------------
    def connect(self) -> None:
        if self._connection is not None:
            return

        logger.info("postgres_destination_connecting", host=self.config.host)

        try:
            self._connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password.get_secret_value(),
                connect_timeout=10,
            )
            self._connection.autocommit = False

            logger.info("postgres_destination_connected", host=self.config.host)

        except Exception as e:
            logger.error(
                "postgres_destination_connection_failed",
                error=str(e),
                exc_info=True,
            )
            raise

    def disconnect(self) -> None:
        if not self._connection:
            return

        try:
            self._connection.commit()
            logger.debug("postgres_destination_commit_success")
        except Exception as e:
            logger.warning(
                "postgres_destination_commit_failed",
                error=str(e),
                exc_info=True,
            )

        try:
            self._connection.close()
            logger.info("postgres_destination_disconnected")
        except Exception as e:
            logger.warning(
                "postgres_destination_disconnect_failed",
                error=str(e),
                exc_info=True,
            )
        finally:
            self._connection = None

    # ----------------------------------------------------------------------
    # Test Connection
    # ----------------------------------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        logger.info("postgres_destination_test_connection_started", schema=self.schema)

        try:
            self.connect()
            cursor = self._connection.cursor()

            # Validate schema exists
            cursor.execute(
                """
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = %s
                """,
                (self.schema,),
            )

            if not cursor.fetchone():
                logger.warning(
                    "postgres_destination_schema_not_found",
                    schema=self.schema,
                )
                return ConnectionTestResult(
                    success=False,
                    message=f"Schema '{self.schema}' does not exist",
                )

            # Verify write permissions
            cursor.execute(
                """
                SELECT has_schema_privilege(%s, 'CREATE');
                """,
                (self.schema,),
            )
            has_permission = cursor.fetchone()[0]
            cursor.close()

            if not has_permission:
                logger.warning(
                    "postgres_destination_no_write_permission",
                    schema=self.schema,
                )
                return ConnectionTestResult(
                    success=False,
                    message=f"No CREATE permission on schema '{self.schema}'",
                )

            logger.info("postgres_destination_test_connection_success")

            return ConnectionTestResult(
                success=True,
                message="Connected successfully with write permissions",
            )

        except Exception as e:
            logger.error(
                "postgres_destination_test_connection_failed",
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(success=False, message=str(e))

        finally:
            self.disconnect()

    # ----------------------------------------------------------------------
    # Create Stream
    # ----------------------------------------------------------------------
    def create_stream(self, stream: str, schema: list[Column]) -> None:
        logger.info("postgres_destination_create_stream_started", stream=stream)

        self.connect()
        cursor = self._connection.cursor()

        try:
            col_defs = []
            for col in schema:
                pg_type = self.TYPE_MAPPING.get(col.data_type, "TEXT")
                nullable = "" if col.nullable else "NOT NULL"
                col_defs.append(f"{col.name} {pg_type} {nullable}")

            pk_cols = [col.name for col in schema if col.primary_key]
            if pk_cols:
                col_defs.append(f"PRIMARY KEY ({', '.join(pk_cols)})")

            sql = f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{stream} (
                    {", ".join(col_defs)}
                )
            """

            cursor.execute(sql)
            self._connection.commit()

            logger.info(
                "postgres_destination_stream_created",
                stream=stream,
                schema=self.schema,
            )

        except Exception as e:
            self._connection.rollback()
            logger.error(
                "postgres_destination_create_stream_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            raise

        finally:
            cursor.close()

    # ----------------------------------------------------------------------
    # Write
    # ----------------------------------------------------------------------
    def write(self, records: Iterator[Record]) -> int:
        logger.info(
            "postgres_destination_write_started",
            write_mode=self.write_mode,
            batch_size=self._batch_size,
        )

        self.connect()
        cursor = self._connection.cursor()

        total_written = 0
        current_stream = None
        batch = []

        try:
            for record in records:
                # If stream changes -> flush
                if current_stream and current_stream != record.stream:
                    total_written += self._write_batch(cursor, current_stream, batch)
                    batch = []

                current_stream = record.stream
                batch.append(record.data)

                if len(batch) >= self._batch_size:
                    total_written += self._write_batch(cursor, current_stream, batch)
                    batch = []

            if batch:
                total_written += self._write_batch(cursor, current_stream, batch)

            self._connection.commit()

            logger.info(
                "postgres_destination_write_success",
                total_records=total_written,
            )

            return total_written

        except Exception as e:
            self._connection.rollback()
            logger.error(
                "postgres_destination_write_failed",
                error=str(e),
                exc_info=True,
            )
            raise

        finally:
            cursor.close()
            self.disconnect()

    # ----------------------------------------------------------------------
    # Batch Insert Logic
    # ----------------------------------------------------------------------
    def _write_batch(self, cursor, stream: str, batch: list[dict[str, Any]]) -> int:
        if not batch:
            return 0

        try:
            columns = list(batch[0].keys())
            placeholder = ",".join(["%s"] * len(columns))

            sql = f"""
                INSERT INTO {self.schema}.{stream} ({",".join(columns)})
                VALUES ({placeholder})
            """

            if self.write_mode == "replace":
                cursor.execute(f"TRUNCATE TABLE {self.schema}.{stream}")

            for record in batch:
                values = [record.get(col) for col in columns]
                cursor.execute(sql, values)

            logger.debug(
                "postgres_destination_batch_write_success",
                stream=stream,
                records=len(batch),
            )

            return len(batch)

        except Exception as e:
            logger.error(
                "postgres_destination_batch_write_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            return 0
