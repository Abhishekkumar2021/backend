"""Oracle Destination Connector (Structured Logging Version)"""

from collections.abc import Iterator
from typing import Any

import oracledb

from app.connectors.base import Column, ConnectionTestResult, DataType, DestinationConnector, Record
from app.core.logging import get_logger
from app.schemas.connector_configs import OracleConfig

logger = get_logger(__name__)


class OracleDestination(DestinationConnector):
    """Oracle destination connector."""

    TYPE_MAPPING = {
        DataType.INTEGER: "NUMBER",
        DataType.FLOAT: "NUMBER",
        DataType.STRING: "VARCHAR2(4000)",
        DataType.BOOLEAN: "NUMBER(1)",   # BOOLEAN emulated as 0/1
        DataType.DATE: "DATE",
        DataType.DATETIME: "TIMESTAMP",
        DataType.JSON: "CLOB",
        DataType.BINARY: "BLOB",
        DataType.NULL: "VARCHAR2(255)",
    }

    def __init__(self, config: OracleConfig):
        super().__init__(config)

        self.user = config.user
        self.password = config.password.get_secret_value()
        self.dsn = config.dsn
        self._batch_size = config.batch_size
        self._connection = None

        logger.debug(
            "oracle_destination_initialized",
            dsn=self.dsn,
            batch_size=self._batch_size,
        )

    # ----------------------------------------------------------------------
    # Connection Handling
    # ----------------------------------------------------------------------
    def connect(self) -> None:
        if self._connection is not None:
            return

        logger.info("oracle_destination_connecting", dsn=self.dsn)

        try:
            self._connection = oracledb.connect(
                user=self.user,
                password=self.password,
                dsn=self.dsn,
            )
            self._connection.autocommit = False

            logger.info("oracle_destination_connected", dsn=self.dsn)

        except Exception as e:
            logger.error(
                "oracle_destination_connection_failed",
                dsn=self.dsn,
                error=str(e),
                exc_info=True,
            )
            raise

    def disconnect(self) -> None:
        if not self._connection:
            return

        try:
            self._connection.commit()
            logger.debug("oracle_destination_commit_success")
        except Exception as e:
            logger.warning(
                "oracle_destination_commit_failed",
                error=str(e),
                exc_info=True,
            )

        try:
            self._connection.close()
            logger.info("oracle_destination_disconnected")
        except Exception as e:
            logger.warning(
                "oracle_destination_disconnect_failed",
                error=str(e),
                exc_info=True,
            )
        finally:
            self._connection = None

    # ----------------------------------------------------------------------
    # Test Connection
    # ----------------------------------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        logger.info("oracle_destination_test_connection_started", dsn=self.dsn)

        try:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute("SELECT * FROM v$version")
            version = cursor.fetchone()[0]
            cursor.close()

            logger.info(
                "oracle_destination_test_connection_success",
                version=version,
            )

            return ConnectionTestResult(
                success=True,
                message="Connected",
                metadata={"version": version},
            )

        except Exception as e:
            logger.error(
                "oracle_destination_test_connection_failed",
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(success=False, message=str(e))

        finally:
            self.disconnect()

    # ----------------------------------------------------------------------
    # Create Stream (Table)
    # ----------------------------------------------------------------------
    def create_stream(self, stream: str, schema: list[Column]) -> None:
        self.connect()

        logger.info("oracle_destination_create_stream_started", stream=stream)

        cursor = self._connection.cursor()

        try:
            col_defs = []
            for col in schema:
                ora_type = self.TYPE_MAPPING.get(col.data_type, "VARCHAR2(4000)")
                nullable = "" if not col.nullable else "NULL"
                col_defs.append(f'"{col.name}" {ora_type} {nullable}')

            pks = [f'"{c.name}"' for c in schema if c.primary_key]
            if pks:
                col_defs.append(f"PRIMARY KEY ({', '.join(pks)})")

            sql = f'CREATE TABLE "{stream}" ({", ".join(col_defs)})'

            logger.debug("oracle_destination_create_table_sql_prepared", stream=stream)

            try:
                cursor.execute(sql)
                logger.info("oracle_destination_table_created", stream=stream)
            except oracledb.DatabaseError as e:
                (error,) = e.args
                if error.code == 955:  # ORA-00955
                    logger.info("oracle_destination_table_exists", stream=stream)
                else:
                    logger.error(
                        "oracle_destination_create_stream_failed",
                        stream=stream,
                        error=str(e),
                        exc_info=True,
                    )
                    raise

            self._connection.commit()

        except Exception as e:
            logger.error(
                "oracle_destination_create_stream_exception",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            self._connection.rollback()
            raise

        finally:
            cursor.close()
            self.disconnect()

    # ----------------------------------------------------------------------
    # Write Records
    # ----------------------------------------------------------------------
    def write(self, records: Iterator[Record]) -> int:
        logger.info("oracle_destination_write_started", batch_size=self._batch_size)

        self.connect()
        cursor = self._connection.cursor()

        total = 0
        buffer = []
        current_stream = None

        try:
            for record in records:
                if current_stream and record.stream != current_stream:
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

            logger.info(
                "oracle_destination_write_completed",
                total_records=total,
            )

            return total

        except Exception as e:
            self._connection.rollback()
            logger.error(
                "oracle_destination_write_failed",
                error=str(e),
                exc_info=True,
            )
            raise

        finally:
            cursor.close()
            self.disconnect()

    # ----------------------------------------------------------------------
    # Batch Flush
    # ----------------------------------------------------------------------
    def _flush(self, cursor, stream: str, data: list[dict[str, Any]]) -> int:
        if not data:
            return 0

        try:
            keys = list(data[0].keys())
            cols = ", ".join([f'"{k}"' for k in keys])
            placeholders = ", ".join([f":{i+1}" for i in range(len(keys))])

            sql = f'INSERT INTO "{stream}" ({cols}) VALUES ({placeholders})'

            values = [[item.get(k) for k in keys] for item in data]

            cursor.executemany(sql, values)

            logger.debug(
                "oracle_destination_write_batch_success",
                stream=stream,
                records=len(values),
            )

            return len(values)

        except Exception as e:
            logger.error(
                "oracle_destination_write_batch_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            return 0
