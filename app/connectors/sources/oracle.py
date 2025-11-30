"""Oracle Source Connector
"""

import logging
from collections.abc import Iterator
from typing import Any

import oracledb

from app.connectors.base import Column, ConnectionTestResult, DataType, Record, Schema, SourceConnector, State, Table

logger = logging.getLogger(__name__)


class OracleSource(SourceConnector):
    """Oracle source connector
    Supports full refresh and incremental syncs
    """

    # Map Oracle types to standard DataType enum
    TYPE_MAPPING = {
        "NUMBER": DataType.FLOAT,  # Oracle NUMBER can be int or float
        "INTEGER": DataType.INTEGER,
        "FLOAT": DataType.FLOAT,
        "VARCHAR2": DataType.STRING,
        "NVARCHAR2": DataType.STRING,
        "CHAR": DataType.STRING,
        "NCHAR": DataType.STRING,
        "CLOB": DataType.STRING,
        "BLOB": DataType.BINARY,
        "DATE": DataType.DATETIME,
        "TIMESTAMP": DataType.DATETIME,
        "TIMESTAMP WITH TIME ZONE": DataType.DATETIME,
        "TIMESTAMP WITH LOCAL TIME ZONE": DataType.DATETIME,
        "RAW": DataType.BINARY,
        "LONG": DataType.STRING,
    }

    def __init__(self, config: dict[str, Any]):
        """Initialize Oracle source connector

        Config format:
        {
            "user": "user",
            "password": "password",
            "dsn": "host:port/service_name", # or connection string
            "wallet_location": "/path/to/wallet", # Optional for cloud
            "batch_size": 1000
        }
        """
        super().__init__(config)
        self.user = config["user"]
        self.password = config["password"]
        self.dsn = config["dsn"]
        self._batch_size = config.get("batch_size", 1000)
        self._connection = None

    def connect(self) -> None:
        """Establish connection to Oracle"""
        if self._connection is None:
            try:
                # Enable thin mode by default (no instant client needed)
                self._connection = oracledb.connect(user=self.user, password=self.password, dsn=self.dsn)
                logger.info(f"Connected to Oracle: {self.dsn}")
            except Exception as e:
                logger.error(f"Failed to connect to Oracle: {e!s}")
                raise

    def disconnect(self) -> None:
        """Close Oracle connection"""
        if self._connection:
            try:
                self._connection.close()
                logger.info("Oracle connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {e!s}")
            finally:
                self._connection = None

    def test_connection(self) -> ConnectionTestResult:
        """Test Oracle connection"""
        try:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute("SELECT * FROM v$version")
            version = cursor.fetchone()[0]
            cursor.close()

            return ConnectionTestResult(success=True, message="Connected successfully", metadata={"version": version})
        except Exception as e:
            return ConnectionTestResult(success=False, message=f"Connection failed: {e!s}")
        finally:
            self.disconnect()

    def discover_schema(self) -> Schema:
        """Discover schema from Oracle database
        """
        self.connect()
        cursor = self._connection.cursor()
        tables = []

        try:
            # Get tables (user's schema only for simplicity, or configurable)
            # Querying USER_TABLES or ALL_TABLES
            cursor.execute("SELECT table_name FROM user_tables ORDER BY table_name")
            table_names = [row[0] for row in cursor.fetchall()]

            for table_name in table_names:
                # Get columns
                cursor.execute(
                    """
                    SELECT column_name, data_type, nullable, data_default
                    FROM user_tab_columns
                    WHERE table_name = :1
                    ORDER BY column_id
                """,
                    [table_name],
                )

                columns = []
                for col_row in cursor.fetchall():
                    col_name = col_row[0]
                    col_type_raw = col_row[1].split("(")[0]  # Remove size e.g., VARCHAR2(50)
                    is_nullable = col_row[2] == "Y"

                    data_type = self.TYPE_MAPPING.get(col_type_raw, DataType.STRING)

                    # Check PK
                    # simplified PK check

                    columns.append(
                        Column(name=col_name, data_type=data_type, nullable=is_nullable, default_value=col_row[3])
                    )

                # Row count estimate
                cursor.execute("SELECT num_rows FROM user_tables WHERE table_name = :1", [table_name])
                row_count_res = cursor.fetchone()
                row_count = row_count_res[0] if row_count_res else 0

                tables.append(Table(name=table_name, columns=columns, row_count=row_count))

            return Schema(tables=tables)

        except Exception as e:
            logger.error(f"Schema discovery failed: {e!s}")
            raise
        finally:
            self.disconnect()

    def read(self, stream: str, state: State | None = None, query: str | None = None) -> Iterator[Record]:
        """Read data from Oracle table"""
        self.connect()
        cursor = self._connection.cursor()

        # Oracle cursors can return dictionaries if configured, but standard is tuple
        # We'll map using column description

        try:
            if query:
                sql = query
                params = []
            elif state and state.cursor_field:
                sql = f'SELECT * FROM "{stream}" WHERE "{state.cursor_field}" > :1 ORDER BY "{state.cursor_field}"'
                params = [state.cursor_value]
            else:
                sql = f'SELECT * FROM "{stream}"'
                params = []

            cursor.execute(sql, params)

            # Get columns for mapping
            cols = [col[0] for col in cursor.description]

            while True:
                rows = cursor.fetchmany(self._batch_size)
                if not rows:
                    break

                for row in rows:
                    data = dict(zip(cols, row))
                    # Handle LOBs if necessary (oracledb usually handles them in default mode well enough)
                    yield Record(stream=stream, data=data)

        except Exception as e:
            logger.error(f"Failed to read from {stream}: {e!s}")
            raise
        finally:
            self.disconnect()

    def get_record_count(self, stream: str) -> int:
        self.connect()
        cursor = self._connection.cursor()
        try:
            cursor.execute(f'SELECT COUNT(*) FROM "{stream}"')
            return cursor.fetchone()[0]
        finally:
            self.disconnect()
