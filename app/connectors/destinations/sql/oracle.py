"""
Oracle Destination Connector — Unified ETL Framework Edition
Supports streaming batch inserts, structured logging,
LOB handling, and safe table creation.
"""

from __future__ import annotations
from collections.abc import Iterator
from typing import Any, List

import oracledb

from app.connectors.base import (
    Column,
    ConnectionTestResult,
    DataType,
    DestinationConnector,
    Record,
)
from app.core.logging import get_logger
from app.schemas.connector_configs import OracleConfig

logger = get_logger(__name__)


class OracleDestination(DestinationConnector):
    """
    Oracle destination connector.
    Fully aligned with unified connector contract.
    """

    TYPE_MAPPING = {
        DataType.INTEGER: "NUMBER",
        DataType.FLOAT: "NUMBER",
        DataType.STRING: "VARCHAR2(4000)",
        DataType.BOOLEAN: "NUMBER(1)",
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

        self.write_mode = getattr(config, "write_mode", "insert")

        logger.debug(
            "oracle_destination_initialized",
            dsn=self.dsn,
            write_mode=self.write_mode,
            batch_size=self._batch_size,
        )

    # =========================================================================
    # Connection
    # =========================================================================
    def connect(self) -> None:
        if self._connection:
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
                "oracle_destination_connect_failed",
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

    # =========================================================================
    # Test Connection
    # =========================================================================
    def test_connection(self) -> ConnectionTestResult:
        logger.info("oracle_destination_test_requested", dsn=self.dsn)

        try:
            self.connect()
            cur = self._connection.cursor()
            cur.execute("SELECT * FROM v$version")
            version = cur.fetchone()[0]
            cur.close()

            logger.info("oracle_destination_test_success", version=version)

            return ConnectionTestResult(True, "Connected", {"version": version})

        except Exception as e:
            logger.error(
                "oracle_destination_test_failed",
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(False, str(e))

        finally:
            self.disconnect()

    # =========================================================================
    # Create Table (Stream)
    # =========================================================================
    def create_stream(self, stream: str, schema: List[Column]) -> None:
        self.connect()
        cur = self._connection.cursor()

        logger.info("oracle_destination_create_stream", stream=stream)

        try:
            col_defs = []

            for col in schema:
                ora_type = self.TYPE_MAPPING.get(col.data_type, "VARCHAR2(4000)")
                null_sql = "" if not col.nullable else "NULL"
                col_defs.append(f'"{col.name}" {ora_type} {null_sql}')

            pk_cols = [f'"{c.name}"' for c in schema if c.primary_key]
            if pk_cols:
                col_defs.append(f"PRIMARY KEY ({', '.join(pk_cols)})")

            sql = f'CREATE TABLE "{stream}" ({", ".join(col_defs)})'

            try:
                cur.execute(sql)
                logger.info("oracle_destination_stream_created", stream=stream)
            except oracledb.DatabaseError as e:
                (error,) = e.args
                if error.code == 955:  # ORA-00955: already exists
                    logger.info("oracle_destination_stream_exists", stream=stream)
                else:
                    raise

            self._connection.commit()

        except Exception as e:
            logger.error(
                "oracle_destination_create_stream_failed",
                error=str(e),
                exc_info=True,
            )
            self._connection.rollback()
            raise

        finally:
            cur.close()

    # =========================================================================
    # Write Records
    # =========================================================================
    def write(self, records: Iterator[Record]) -> int:
        logger.info(
            "oracle_destination_write_started",
            batch_size=self._batch_size,
            write_mode=self.write_mode,
        )

        self.connect()
        cur = self._connection.cursor()

        buffer = []
        total_written = 0
        current_stream = None

        try:
            for rec in records:
                if current_stream and rec.stream != current_stream:
                    total_written += self._flush(cur, current_stream, buffer)
                    buffer = []

                current_stream = rec.stream
                buffer.append(rec.data)

                if len(buffer) >= self._batch_size:
                    total_written += self._flush(cur, current_stream, buffer)
                    buffer = []

            if buffer:
                total_written += self._flush(cur, current_stream, buffer)

            self._connection.commit()

            logger.info(
                "oracle_destination_write_success",
                total_records=total_written,
            )

            return total_written

        except Exception as e:
            self._connection.rollback()
            logger.error(
                "oracle_destination_write_failed",
                error=str(e),
                exc_info=True,
            )
            raise

        finally:
            cur.close()
            self.disconnect()

    # =========================================================================
    # Batch Flush — Safe for LOBs
    # =========================================================================
    def _flush(self, cur, stream: str, batch: List[dict[str, Any]]) -> int:
        if not batch:
            return 0

        try:
            keys = list(batch[0].keys())
            columns = ", ".join([f'"{k}"' for k in keys])
            placeholders = ", ".join([f":{i+1}" for i in range(len(keys))])

            insert_sql = f'INSERT INTO "{stream}" ({columns}) VALUES ({placeholders})'

            # Oracle LOB handling
            cur.setinputsizes(**{
                f"{i+1}": oracledb.DB_TYPE_CLOB
                for i, k in enumerate(keys)
                if isinstance(batch[0].get(k), (dict, list))
            })

            rows = [
                [row.get(col) for col in keys]
                for row in batch
            ]

            cur.executemany(insert_sql, rows)

            logger.debug(
                "oracle_destination_batch_success",
                stream=stream,
                records=len(rows),
            )
            return len(rows)

        except Exception as e:
            logger.error(
                "oracle_destination_batch_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            return 0
