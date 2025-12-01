"""MSSQL Destination Connector (Structured Logging)"""

from collections.abc import Iterator
from typing import Any

import pymssql

from app.connectors.base import Column, ConnectionTestResult, DestinationConnector, Record
from app.core.logging import get_logger
from app.schemas.connector_configs import MSSQLConfig

logger = get_logger(__name__)


class MSSQLDestination(DestinationConnector):
    """MSSQL destination connector"""

    TYPE_MAPPING = {
        "INTEGER": "BIGINT",
        "FLOAT": "FLOAT",
        "STRING": "NVARCHAR(MAX)",
        "BOOLEAN": "BIT",
        "DATE": "DATE",
        "DATETIME": "DATETIME2",
        "JSON": "NVARCHAR(MAX)",
        "BINARY": "VARBINARY(MAX)",
        "NULL": "NVARCHAR(MAX)",
    }

    def __init__(self, config: MSSQLConfig):
        super().__init__(config)
        self.host = config.host
        self.port = config.port
        self.user = config.user
        self.password = config.password.get_secret_value()
        self.database = config.database
        self._batch_size = config.batch_size
        self._connection = None

        logger.debug(
            "mssql_destination_initialized",
            host=self.host,
            port=self.port,
            database=self.database,
            batch_size=self._batch_size,
        )

    # ------------------------------------------------------------------
    # Connection Handling
    # ------------------------------------------------------------------
    def connect(self) -> None:
        """Establish MSSQL connection."""
        if self._connection is not None:
            return

        server = f"{self.host}:{self.port}" if self.port else self.host

        logger.info(
            "mssql_destination_connecting",
            host=self.host,
            port=self.port,
            server=server,
            database=self.database,
        )

        try:
            self._connection = pymssql.connect(
                server=server,
                user=self.user,
                password=self.password,
                database=self.database,
                autocommit=False,
            )

            logger.info("mssql_destination_connected", server=server)

        except Exception as e:
            logger.error(
                "mssql_destination_connection_failed",
                error=str(e),
                exc_info=True,
            )
            raise

    def disconnect(self) -> None:
        """Close connection."""
        if self._connection:
            try:
                self._connection.commit()
                logger.debug("mssql_destination_commit_success")
            except Exception as e:
                logger.warning("mssql_destination_commit_failed", error=str(e), exc_info=True)

            try:
                self._connection.close()
                logger.info("mssql_destination_disconnected")
            except Exception as e:
                logger.warning("mssql_destination_disconnect_failed", error=str(e), exc_info=True)

            finally:
                self._connection = None

    # ------------------------------------------------------------------
    # Test Connection
    # ------------------------------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        logger.info("mssql_destination_test_connection_started")

        try:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]

            logger.info("mssql_destination_test_connection_success", version=version)

            return ConnectionTestResult(
                success=True,
                message="Connected",
                metadata={"version": version},
            )
        except Exception as e:
            logger.error(
                "mssql_destination_test_connection_failed",
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(success=False, message=str(e))
        finally:
            self.disconnect()

    # ------------------------------------------------------------------
    # Stream/Table Creation
    # ------------------------------------------------------------------
    def create_stream(self, stream: str, schema: list[Column]) -> None:
        """Create table if not exists."""
        logger.info("mssql_destination_create_stream_started", stream=stream)

        self.connect()
        cursor = self._connection.cursor()

        try:
            col_defs = []
            for col in schema:
                ms_type = self.TYPE_MAPPING.get(col.data_type.name, "NVARCHAR(MAX)")
                nullable = "" if not col.nullable else "NULL"
                col_defs.append(f"[{col.name}] {ms_type} {nullable}")

            pks = [f"[{c.name}]" for c in schema if c.primary_key]
            if pks:
                col_defs.append(f"PRIMARY KEY ({', '.join(pks)})")

            # Split stream into schema + table
            if "." in stream:
                schema_name, table_name = stream.split(".", 1)
            else:
                schema_name, table_name = "dbo", stream

            full_name = f"[{schema_name}].[{table_name}]"

            logger.debug(
                "mssql_destination_create_table_sql_prepared",
                full_table_name=full_name,
                columns=len(col_defs),
            )

            cursor.execute(f"""
                IF NOT EXISTS (
                    SELECT * FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
                )
                CREATE TABLE {full_name} ({", ".join(col_defs)})
            """)

            self._connection.commit()

            logger.info("mssql_destination_table_created", table=full_name)

        except Exception as e:
            self._connection.rollback()
            logger.error(
                "mssql_destination_table_creation_failed",
                error=str(e),
                table=stream,
                exc_info=True,
            )
            raise

        finally:
            self.disconnect()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------
    def write(self, records: Iterator[Record]) -> int:
        """Write records in batches."""
        logger.info("mssql_destination_write_started", batch_size=self._batch_size)

        self.connect()
        cursor = self._connection.cursor()

        total = 0
        buffer = []
        current_stream = None

        try:
            for record in records:
                if current_stream and current_stream != record.stream:
                    total += self._flush(cursor, current_stream, buffer)
                    buffer = []

                current_stream = record.stream
                buffer.append(record.data)

                if len(buffer) >= self._batch_size:
                    total += self._flush(cursor, current_stream, buffer)
                    buffer = []

            if buffer:
                total += self._flush(cursor, current_stream, buffer)

            self._connection.commit()

            logger.info("mssql_destination_write_completed", records_written=total)
            return total

        except Exception as e:
            self._connection.rollback()
            logger.error(
                "mssql_destination_write_failed",
                error=str(e),
                exc_info=True,
            )
            raise

        finally:
            self.disconnect()

    # ------------------------------------------------------------------
    # Internal Batch Writer
    # ------------------------------------------------------------------
    def _flush(self, cursor, stream: str, data: list[dict[str, Any]]) -> int:
        """Insert batch into MSSQL."""
        if not data:
            return 0

        try:
            keys = list(data[0].keys())

            # Table name handling
            if "." in stream:
                schema_name, table_name = stream.split(".", 1)
                full_name = f"[{schema_name}].[{table_name}]"
            else:
                full_name = f"[dbo].[{stream}]"

            cols = ", ".join([f"[{k}]" for k in keys])
            placeholders = ", ".join(["%s"] * len(keys))
            sql = f"INSERT INTO {full_name} ({cols}) VALUES ({placeholders})"

            values = [tuple(item.get(k) for k in keys) for item in data]

            cursor.executemany(sql, values)

            logger.debug(
                "mssql_destination_write_batch_success",
                table=full_name,
                records=len(values),
            )

            return len(values)

        except Exception as e:
            logger.error(
                "mssql_destination_write_batch_failed",
                table=stream,
                error=str(e),
                exc_info=True,
            )
            return 0
