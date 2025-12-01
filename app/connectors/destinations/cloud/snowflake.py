from collections.abc import Iterator
from typing import Any, List

import snowflake.connector
from app.connectors.base import Column, ConnectionTestResult, DestinationConnector, Record
from app.connectors.utils import map_data_type_to_sql_type
from app.core.logging import get_logger
from app.schemas.connector_configs import SnowflakeConfig

logger = get_logger(__name__)


class SnowflakeDestination(DestinationConnector):
    """
    Snowflake Destination Connector.
    Loads data into Snowflake tables.
    """

    def __init__(self, config: SnowflakeConfig):
        super().__init__(config)
        self.account = config.account
        self.username = config.username
        self.password = config.password.get_secret_value()
        self.role = config.role
        self.warehouse = config.warehouse
        self.database = config.database
        self.schema = config.schema_
        self._batch_size = config.batch_size
        
        # Extra fields from pipeline config
        self.table = getattr(config, "table", None)
        self.write_mode = getattr(config, "write_mode", "append")

        logger.debug(
            "snowflake_destination_initialized",
            account=self.account,
            database=self.database,
            schema=self.schema,
            table=self.table,
            write_mode=self.write_mode,
        )

    # ------------------------------------------------------------------
    # Connection Handling
    # ------------------------------------------------------------------
    def connect(self) -> None:
        try:
            self._connection = snowflake.connector.connect(
                user=self.username,
                password=self.password,
                account=self.account,
                role=self.role,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
            )

            logger.info(
                "snowflake_connect_success",
                account=self.account,
                database=self.database,
                schema=self.schema,
            )

        except Exception as e:
            logger.error(
                "snowflake_connect_failed",
                error=str(e),
                exc_info=True,
            )
            raise

    def disconnect(self) -> None:
        if self._connection:
            try:
                self._connection.close()
                logger.debug("snowflake_connection_closed")
            except Exception as e:
                logger.warning(
                    "snowflake_connection_close_failed",
                    error=str(e),
                    exc_info=True,
                )
            finally:
                self._connection = None

    # ------------------------------------------------------------------
    # Test Connection
    # ------------------------------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        logger.info("snowflake_test_connection_started")

        try:
            with self:
                self._connection.cursor().execute("SELECT 1").close()

            logger.info("snowflake_test_connection_success")
            return ConnectionTestResult(success=True, message="Successfully connected to Snowflake.")

        except Exception as e:
            logger.error(
                "snowflake_test_connection_failed",
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(success=False, message=f"Failed to connect to Snowflake: {e}")

    # ------------------------------------------------------------------
    # Create Table
    # ------------------------------------------------------------------
    def create_stream(self, stream: str, schema: List[Column]) -> None:
        if not self.database or not self.schema or not stream:
            raise ValueError("Database, Schema, and Table name must be provided.")

        with self:
            cursor = self._connection.cursor()
            try:
                columns_sql = []
                for col in schema:
                    sql_type = map_data_type_to_sql_type(col.data_type)
                    nullable = "NULL" if col.nullable else "NOT NULL"
                    columns_sql.append(f'{col.name} {sql_type} {nullable}')

                create_sql = (
                    f"CREATE TABLE IF NOT EXISTS "
                    f"{self.database}.{self.schema}.{stream} ("
                    + ", ".join(columns_sql)
                    + ")"
                )

                logger.debug(
                    "snowflake_create_table_sql_generated",
                    table=stream,
                    columns=len(schema),
                )

                cursor.execute(create_sql)

                logger.info(
                    "snowflake_create_table_completed",
                    table=stream,
                )

            finally:
                cursor.close()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------
    def write(self, records: Iterator[Record]) -> int:
        if not self.database or not self.schema or not self.table:
            raise ValueError("Database, Schema, and Table must be provided.")

        logger.info(
            "snowflake_write_started",
            table=self.table,
            batch_size=self._batch_size,
        )

        inserted_count = 0
        batch: List[dict[str, Any]] = []

        with self:
            cursor = self._connection.cursor()

            try:
                for record in records:
                    batch.append(record.data)

                    if len(batch) >= self._batch_size:
                        inserted_count += self._insert_batch(cursor, self.table, batch)
                        batch = []

                if batch:
                    inserted_count += self._insert_batch(cursor, self.table, batch)

                self._connection.commit()

                logger.info(
                    "snowflake_write_completed",
                    total_inserted=inserted_count,
                )

                return inserted_count

            except Exception as e:
                logger.error(
                    "snowflake_write_failed",
                    error=str(e),
                    exc_info=True,
                )
                raise

            finally:
                cursor.close()

    # ------------------------------------------------------------------
    # Insert Batch
    # ------------------------------------------------------------------
    def _insert_batch(self, cursor: Any, table_name: str, batch: List[dict[str, Any]]) -> int:
        if not batch:
            return 0

        columns = list(batch[0].keys())
        columns_sql = ", ".join([f'"{c}"' for c in columns])
        placeholders = ", ".join(["%s"] * len(columns))

        insert_sql = (
            f"INSERT INTO {self.database}.{self.schema}.{table_name} "
            f"({columns_sql}) VALUES ({placeholders})"
        )

        data_rows = [[row.get(col) for col in columns] for row in batch]

        try:
            cursor.executemany(insert_sql, data_rows)

            logger.debug(
                "snowflake_write_batch_inserted",
                table=table_name,
                row_count=len(batch),
            )

            return len(batch)

        except Exception as e:
            logger.error(
                "snowflake_write_batch_failed",
                table=table_name,
                error=str(e),
                exc_info=True,
            )
            return 0
