"""SQLite Source Connector
"""

import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from app.connectors.base import (
    Column, ConnectionTestResult, DataType,
    Record, Schema, SourceConnector, State, Table
)
from app.core.logging import get_logger

logger = get_logger(__name__)

# SQLite type mapping
TYPE_MAPPING = {
    "INTEGER": DataType.INTEGER,
    "INT": DataType.INTEGER,
    "TINYINT": DataType.INTEGER,
    "SMALLINT": DataType.INTEGER,
    "MEDIUMINT": DataType.INTEGER,
    "BIGINT": DataType.INTEGER,
    "REAL": DataType.FLOAT,
    "DOUBLE": DataType.FLOAT,
    "FLOAT": DataType.FLOAT,
    "NUMERIC": DataType.FLOAT,
    "DECIMAL": DataType.FLOAT,
    "TEXT": DataType.STRING,
    "VARCHAR": DataType.STRING,
    "CHAR": DataType.STRING,
    "CLOB": DataType.STRING,
    "BLOB": DataType.BINARY,
    "BOOLEAN": DataType.BOOLEAN,
    "DATE": DataType.DATE,
    "DATETIME": DataType.DATETIME,
    "TIMESTAMP": DataType.DATETIME,
}


class SQLiteSource(SourceConnector):
    """SQLite source connector"""

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.database_path = config["database_path"]
        self._batch_size = config.get("batch_size", 1000)
        self._connection = None

        logger.debug(
            "sqlite_source_initialized",
            database_path=self.database_path,
            batch_size=self._batch_size,
        )

    # ===================================================================
    # Connection Handling
    # ===================================================================
    def connect(self) -> None:
        if self._connection is not None:
            return

        logger.info(
            "sqlite_connect_requested",
            database_path=self.database_path,
        )

        try:
            if self.database_path != ":memory:":
                db_path = Path(self.database_path)
                if not db_path.exists():
                    raise FileNotFoundError(f"Database file not found: {self.database_path}")

            self._connection = sqlite3.connect(self.database_path, check_same_thread=False)
            self._connection.row_factory = sqlite3.Row

            logger.info(
                "sqlite_connect_success",
                database_path=self.database_path,
            )

        except Exception as e:
            logger.error(
                "sqlite_connect_failed",
                database_path=self.database_path,
                error=str(e),
                exc_info=True,
            )
            raise

    def disconnect(self) -> None:
        if not self._connection:
            return

        logger.debug("sqlite_disconnect_requested")

        try:
            self._connection.close()
            logger.debug("sqlite_disconnected")
        except Exception as e:
            logger.warning(
                "sqlite_disconnect_failed",
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
            "sqlite_test_connection_requested",
            database_path=self.database_path,
        )

        try:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute("SELECT sqlite_version();")
            version = cursor.fetchone()[0]
            cursor.close()

            logger.info(
                "sqlite_test_connection_success",
                version=version,
            )

            return ConnectionTestResult(
                success=True,
                message="Connected successfully",
                metadata={"version": f"SQLite {version}"},
            )

        except FileNotFoundError as e:
            logger.error(
                "sqlite_test_connection_missing_file",
                database_path=self.database_path,
                error=str(e),
            )
            return ConnectionTestResult(success=False, message=str(e))

        except Exception as e:
            logger.error(
                "sqlite_test_connection_failed",
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
            "sqlite_schema_discovery_requested",
            database_path=self.database_path,
        )

        self.connect()
        cursor = self._connection.cursor()
        tables = []

        try:
            cursor.execute("""
                SELECT name 
                FROM sqlite_master 
                WHERE type='table' 
                  AND name NOT LIKE 'sqlite_%'
                ORDER BY name;
            """)
            table_names = [row[0] for row in cursor.fetchall()]

            logger.debug(
                "sqlite_schema_table_list_retrieved",
                table_count=len(table_names),
            )

            for table_name in table_names:
                logger.debug(
                    "sqlite_schema_processing_table",
                    table=table_name,
                )

                # Columns
                cursor.execute(f"PRAGMA table_info('{table_name}');")
                column_rows = cursor.fetchall()

                # Foreign keys
                cursor.execute(f"PRAGMA foreign_key_list('{table_name}');")
                fk_rows = cursor.fetchall()

                foreign_keys = {
                    fk_row[3]: f"{fk_row[2]}.{fk_row[4]}"
                    for fk_row in fk_rows
                }

                columns = []
                for col_row in column_rows:
                    col_name = col_row[1]
                    col_type = col_row[2].upper().split("(")[0]
                    mapped_type = TYPE_MAPPING.get(col_type, DataType.STRING)

                    columns.append(
                        Column(
                            name=col_name,
                            data_type=mapped_type,
                            nullable=(col_row[3] == 0),
                            primary_key=(col_row[5] > 0),
                            foreign_key=foreign_keys.get(col_name),
                            default_value=col_row[4],
                        )
                    )

                cursor.execute(f"SELECT COUNT(*) FROM '{table_name}';")
                row_count = cursor.fetchone()[0]

                tables.append(
                    Table(
                        name=table_name,
                        columns=columns,
                        row_count=row_count,
                    )
                )

            # SQLite version
            cursor = self._connection.cursor()
            cursor.execute("SELECT sqlite_version();")
            version = cursor.fetchone()[0]

            logger.info(
                "sqlite_schema_discovery_completed",
                table_count=len(tables),
                version=version,
            )

            return Schema(tables=tables, version=f"SQLite {version}")

        except Exception as e:
            logger.error(
                "sqlite_schema_discovery_failed",
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
            "sqlite_read_requested",
            stream=stream,
            database_path=self.database_path,
            has_state=bool(state),
            has_query=bool(query),
        )

        self.connect()
        cursor = self._connection.cursor()

        try:
            if query:
                sql = query
                params = ()
            elif state and state.cursor_field:
                sql = (
                    f'SELECT * FROM "{stream}" '
                    f'WHERE "{state.cursor_field}" > ? '
                    f'ORDER BY "{state.cursor_field}"'
                )
                params = (state.cursor_value,)
            else:
                sql = f'SELECT * FROM "{stream}"'
                params = ()

            logger.debug(
                "sqlite_read_query_prepared",
                sql_preview=sql[:250],
                param_count=len(params),
            )

            cursor.execute(sql, params)

            while True:
                rows = cursor.fetchmany(self._batch_size)
                if not rows:
                    break

                logger.debug(
                    "sqlite_read_batch_fetched",
                    batch_size=len(rows),
                )

                for row in rows:
                    yield Record(stream=stream, data=dict(row))

        except Exception as e:
            logger.error(
                "sqlite_read_failed",
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
            "sqlite_record_count_requested",
            stream=stream,
            database_path=self.database_path,
        )

        self.connect()
        cursor = self._connection.cursor()

        try:
            cursor.execute(f'SELECT COUNT(*) FROM "{stream}"')
            count = cursor.fetchone()[0]

            logger.info(
                "sqlite_record_count_retrieved",
                stream=stream,
                record_count=count,
            )

            return count

        except Exception as e:
            logger.error(
                "sqlite_record_count_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            raise

        finally:
            self.disconnect()
