"""
MongoDB Source Connector — Universal ETL Compatible
--------------------------------------------------

Implements universal connector API:
✓ get_stream_identifier()
✓ get_query()  → MongoDB filter dict
✓ supports_streams() = True  (collections)
✓ supports_query() = True    (Mongo filters)
"""

from __future__ import annotations

from typing import Iterator, Any, Optional

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

from app.connectors.base import (
    SourceConnector,
    ConnectionTestResult,
    Record,
    Schema,
    Table,
    Column,
    DataType,
    State,
)
from app.connectors.utils import map_mongo_type_to_data_type
from app.schemas.connector_configs import MongoDBConfig
from app.core.logging import get_logger

logger = get_logger(__name__)


class MongoDBSource(SourceConnector):

    # ------------------------------------------------------------------
    # INIT
    # ------------------------------------------------------------------
    def __init__(self, config: MongoDBConfig):
        super().__init__(config)

        self.host = config.host
        self.port = config.port
        self.username = config.username
        self.password = config.password.get_secret_value() if config.password else None
        self.database = config.database
        self.auth_source = config.auth_source or self.database
        self.replication_key = config.replication_key  # optional

        self._connection: Optional[MongoClient] = None

        logger.debug(
            "mongodb_source_initialized",
            host=self.host,
            port=self.port,
            database=self.database,
            replication_key=self.replication_key,
        )

    # ------------------------------------------------------------------
    # UNIVERSAL CONNECTOR API
    # ------------------------------------------------------------------
    def supports_streams(self) -> bool:
        return True   # MongoDB has collections

    def supports_query(self) -> bool:
        return True   # We accept filter dicts

    def get_stream_identifier(self, config: dict) -> str | None:
        """Return collection name."""
        stream = config.get("stream")
        if not stream:
            raise ValueError("MongoDB requires a 'stream' (collection name)")
        return stream

    def get_query(self, config: dict) -> dict | None:
        """Return MongoDB filter dict."""
        q = config.get("query")
        return q if isinstance(q, dict) else None

    # ------------------------------------------------------------------
    # CONNECTION
    # ------------------------------------------------------------------
    def connect(self) -> None:
        if self._connection is not None:
            return

        try:
            self._connection = MongoClient(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                authSource=self.auth_source,
                serverSelectionTimeoutMS=5000,
            )

            # Validate connection
            self._connection.admin.command("ping")

            logger.info(
                "mongodb_connected",
                host=self.host,
                port=self.port,
                database=self.database,
            )

        except Exception as e:
            logger.error("mongodb_connect_failed", error=str(e), exc_info=True)
            raise

    def disconnect(self) -> None:
        if self._connection:
            try:
                self._connection.close()
            finally:
                self._connection = None

    # ------------------------------------------------------------------
    # TEST CONNECTION
    # ------------------------------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        try:
            with self:
                return ConnectionTestResult(
                    success=True,
                    message="Connected successfully",
                    metadata={"host": self.host, "database": self.database},
                )
        except Exception as e:
            return ConnectionTestResult(success=False, message=str(e))

    # ------------------------------------------------------------------
    # SCHEMA DISCOVERY
    # ------------------------------------------------------------------
    def discover_schema(self) -> Schema:
        if not self.database:
            raise ValueError("MongoDB database must be provided")

        with self:
            db = self._connection[self.database]
            collections = db.list_collection_names()

            tables: list[Table] = []

            for col_name in collections:
                sample = db[col_name].find_one()
                cols: list[Column] = []

                if sample:
                    for key, value in sample.items():
                        if key == "_id":
                            continue
                        mapped = map_mongo_type_to_data_type(type(value))
                        cols.append(Column(name=key, data_type=mapped, nullable=True))

                tables.append(Table(name=col_name, schema=self.database, columns=cols))

            return Schema(tables=tables)

    # ------------------------------------------------------------------
    # READ
    # ------------------------------------------------------------------
    def read(
        self,
        stream: str,
        state: Optional[State] = None,
        query: Optional[dict] = None,
    ) -> Iterator[Record]:

        if not self.database:
            raise ValueError("MongoDB database not set")

        if not stream:
            raise ValueError("MongoDB requires collection name (stream)")

        with self:
            db = self._connection[self.database]
            collection = db[stream]

            mongo_filter = query.copy() if query else {}

            # Incremental sync
            if state and self.replication_key:
                if state.cursor_value is not None:
                    mongo_filter[self.replication_key] = {"$gt": state.cursor_value}

                logger.debug(
                    "mongodb_incremental_filter_applied",
                    replication_key=self.replication_key,
                    last_value=state.cursor_value,
                )

            # Use projection to exclude "_id"
            projection = {"_id": False}

            try:
                cursor = collection.find(
                    filter=mongo_filter,
                    projection=projection,
                    no_cursor_timeout=True,
                )

                for doc in cursor:
                    yield Record(stream=stream, data=doc)

            except Exception as e:
                logger.error("mongodb_read_failed", stream=stream, error=str(e), exc_info=True)
                raise

    # ------------------------------------------------------------------
    # COUNT
    # ------------------------------------------------------------------
    def get_record_count(self, stream: str) -> int:
        if not self.database:
            raise ValueError("MongoDB database not set")

        with self:
            db = self._connection[self.database]
            return db[stream].count_documents({})
