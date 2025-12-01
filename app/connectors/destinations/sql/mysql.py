"""MySQL Destination Connector (Structured Logging Version)"""

from collections.abc import Iterator
from typing import Any

import pymysql

from app.connectors.base import Column, ConnectionTestResult, DestinationConnector, Record, DataType
from app.core.logging import get_logger
from app.schemas.connector_configs import MySQLConfig

logger = get_logger(__name__)


class MySQLDestination(DestinationConnector):
    """MySQL destination connector"""

    TYPE_MAPPING = {
        DataType.INTEGER: "BIGINT",
        DataType.FLOAT: "DOUBLE",
        DataType.STRING: "TEXT",
        DataType.BOOLEAN: "BOOLEAN",
        DataType.DATE: "DATE",
        DataType.DATETIME: "DATETIME",
        DataType.JSON: "JSON",
        DataType.BINARY: "BLOB",
        DataType.NULL: "TEXT",
    }

    def __init__(self, config: MySQLConfig):
        super().__init__(config)
        self.host = config.host
        self.port = config.port
        self.user = config.user
        self.password = config.password.get_secret_value()
        self.database = config.database
        self._batch_size = config.batch_size
        self._connection = None

        logger.debug(
            "mysql_destination_initialized",
            host=self.host,
            port=self.port,
            database=self.database,
            batch_size=self._batch_size,
        )

    # ----------------------------------------------------------------------
    # Connection
    # ----------------------------------------------------------------------
    def connect(self) -> None:
        if self._connection is not None:
            return

        logger.info(
            "mysql_destination_connecting",
            host=self.host,
            port=self.port,
            database=self.database,
        )

        try:
            self._connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                autocommit=False,
            )

            logger.info("mysql_destination_connected", host=self.host, port=self.port)

        except Exception as e:
            logger.error(
                "mysql_destination_connection_failed",
                error=str(e),
                exc_info=True,
            )
            raise

    def disconnect(self) -> None:
        if self._connection:
            try:
                self._connection.commit()
                logger.debug("mysql_destination_commit_success")
            except Exception as e:
                logger.warning("mysql_destination_commit_failed", error=str(e), exc_info=True)

            try:
                self._connection.close()
                logger.info("mysql_destination_disconnected")
            except Exception as e:
                logger.warning("mysql_destination_disconnect_failed", error=str(e), exc_info=True)

            finally:
                self._connection = None

    # ----------------------------------------------------------------------
    # Test
    # ----------------------------------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        logger.info("mysql_destination_test_connection_started")

        try:
            self.connect()
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()[0]

            logger.info("mysql_destination_test_connection_success", version=version)

            return ConnectionTestResult(
                success=True,
                message="Connected",
                metadata={"version": version},
            )

        except Exception as e:
            logger.error(
                "mysql_destination_test_connection_failed",
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

        logger.info("mysql_destination_create_stream_started", stream=stream)

        try:
            with self._connection.cursor() as cursor:
                col_defs = []
                for col in schema:
                    my_type = self.TYPE_MAPPING.get(col.data_type, "TEXT")
                    nullable = "" if not col.nullable else "NULL"
                    col_defs.append(f"`{col.name}` {my_type} {nullable}")

                pks = [f"`{c.name}`" for c in schema if c.primary_key]
                if pks:
                    col_defs.append(f"PRIMARY KEY ({', '.join(pks)})")

                sql = f"CREATE TABLE IF NOT EXISTS `{stream}` ({', '.join(col_defs)})"

                logger.debug("mysql_destination_create_table_sql_prepared", stream=stream)

                cursor.execute(sql)
                self._connection.commit()

                logger.info("mysql_destination_table_created", stream=stream)

        except Exception as e:
            logger.error(
                "mysql_destination_create_stream_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            self._connection.rollback()
            raise

        finally:
            self.disconnect()

    # ----------------------------------------------------------------------
    # Write
    # ----------------------------------------------------------------------
    def write(self, records: Iterator[Record]) -> int:
        logger.info("mysql_destination_write_started", batch_size=self._batch_size)

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

            logger.info("mysql_destination_write_completed", records_written=total)
            return total

        except Exception as e:
            self._connection.rollback()
            logger.error(
                "mysql_destination_write_failed",
                error=str(e),
                exc_info=True,
            )
            raise

        finally:
            cursor.close()
            self.disconnect()

    # ----------------------------------------------------------------------
    # Internal Batch Flush
    # ----------------------------------------------------------------------
    def _flush(self, cursor, stream: str, data: list[dict[str, Any]]) -> int:
        if not data:
            return 0

        try:
            keys = list(data[0].keys())
            cols = ", ".join([f"`{k}`" for k in keys])
            placeholders = ", ".join(["%s"] * len(keys))
            sql = f"INSERT INTO `{stream}` ({cols}) VALUES ({placeholders})"

            values = [[item.get(k) for k in keys] for item in data]

            cursor.executemany(sql, values)

            logger.debug(
                "mysql_destination_write_batch_success",
                stream=stream,
                records=len(values),
            )

            return len(values)

        except Exception as e:
            logger.error(
                "mysql_destination_write_batch_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            return 0
