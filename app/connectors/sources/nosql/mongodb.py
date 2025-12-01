from collections.abc import Iterator
from typing import Any

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

from app.connectors.base import (
    ConnectionTestResult,
    Record,
    Schema,
    SourceConnector,
    Table,
    Column,
    DataType,
)
from app.connectors.utils import map_mongo_type_to_data_type
from app.core.logging import get_logger
from app.schemas.connector_configs import MongoDBConfig

logger = get_logger(__name__)


class MongoDBSource(SourceConnector):
    """
    MongoDB Source Connector.
    Connects to a MongoDB database and extracts data from collections.
    """

    def __init__(self, config: MongoDBConfig):
        super().__init__(config)

        self.host = config.host
        self.port = config.port
        self.username = config.username
        self.password = config.password.get_secret_value() if config.password else None
        self.database = config.database
        self.auth_source = config.auth_source or self.database

        self._connection = None
        self.logger = logger

        logger.debug(
            "mongodb_source_initialized",
            host=self.host,
            port=self.port,
            database=self.database,
            auth_source=self.auth_source,
        )

    # ===================================================================
    # Connection
    # ===================================================================
    def connect(self) -> None:
        """Establish connection to MongoDB."""
        logger.info(
            "mongodb_connect_requested",
            host=self.host,
            port=self.port,
            database=self.database,
        )

        try:
            self._connection = MongoClient(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                authSource=self.auth_source,
                serverSelectionTimeoutMS=5000,
            )

            # Validate server connection
            self._connection.admin.command("ismaster")

            logger.info(
                "mongodb_connect_success",
                host=self.host,
                port=self.port,
            )

        except ConnectionFailure as e:
            logger.error(
                "mongodb_connection_failure",
                host=self.host,
                port=self.port,
                error=str(e),
                exc_info=True,
            )
            raise ConnectionFailure(f"MongoDB connection failed: {e}")

        except OperationFailure as e:
            logger.error(
                "mongodb_auth_failure",
                host=self.host,
                port=self.port,
                error=str(e),
                exc_info=True,
            )
            raise OperationFailure(e.details.get("errmsg", str(e)))

        except Exception as e:
            logger.error(
                "mongodb_connect_unexpected_error",
                host=self.host,
                port=self.port,
                error=str(e),
                exc_info=True,
            )
            raise Exception(f"An unexpected error occurred during MongoDB connection: {e}")

    def disconnect(self) -> None:
        """Close connection to MongoDB."""
        if self._connection:
            logger.debug("mongodb_disconnect_requested")
            self._connection.close()
            self._connection = None
            logger.debug("mongodb_disconnected")

    # ===================================================================
    # Test Connection
    # ===================================================================
    def test_connection(self) -> ConnectionTestResult:
        logger.info(
            "mongodb_test_connection_requested",
            host=self.host,
            port=self.port,
        )

        try:
            with self:
                logger.info(
                    "mongodb_test_connection_success",
                    host=self.host,
                )
                return ConnectionTestResult(success=True, message="Successfully connected to MongoDB.")

        except ConnectionFailure as e:
            logger.warning(
                "mongodb_test_connection_failure",
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(success=False, message=f"Failed to connect to MongoDB: {e}")

        except OperationFailure as e:
            logger.warning(
                "mongodb_test_auth_failure",
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(
                success=False,
                message=f"MongoDB authentication failed: {e.details.get('errmsg', str(e))}",
            )

        except Exception as e:
            logger.error(
                "mongodb_test_unexpected_error",
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(success=False, message=f"An unexpected error occurred: {e}")

    # ===================================================================
    # Schema Discovery
    # ===================================================================
    def discover_schema(self) -> Schema:
        logger.info(
            "mongodb_schema_discovery_requested",
            database=self.database,
        )

        if not self.database:
            logger.error(
                "mongodb_schema_discovery_invalid_config",
                database=self.database,
            )
            raise ValueError("Database name must be provided in config to discover schema.")

        with self:
            db = self._connection[self.database]
            tables = []

            try:
                collection_names = db.list_collection_names()
                logger.debug(
                    "mongodb_collections_listed",
                    count=len(collection_names),
                )

                for collection_name in collection_names:
                    sample_document = db[collection_name].find_one()
                    columns = []

                    if sample_document:
                        for key, value in sample_document.items():
                            if key == "_id":
                                continue  # Skip internal ID field

                            data_type = map_mongo_type_to_data_type(type(value))

                            columns.append(
                                Column(
                                    name=key,
                                    data_type=data_type,
                                    nullable=True,
                                )
                            )

                    tables.append(
                        Table(
                            name=collection_name,
                            schema=self.database,
                            columns=columns,
                        )
                    )

                logger.info(
                    "mongodb_schema_discovery_completed",
                    table_count=len(tables),
                )

                return Schema(tables=tables)

            except Exception as e:
                logger.error(
                    "mongodb_schema_discovery_failed",
                    error=str(e),
                    exc_info=True,
                )
                return Schema(tables=[])

    # ===================================================================
    # Read Data
    # ===================================================================
    def read(
        self,
        stream: str,
        state: dict | None = None,
        query: dict | None = None,
    ) -> Iterator[Record]:
        logger.info(
            "mongodb_read_requested",
            database=self.database,
            stream=stream,
            has_state=bool(state),
            has_query=bool(query),
        )

        if not self.database:
            logger.error(
                "mongodb_read_invalid_config",
                database=self.database,
            )
            raise ValueError("Database name must be provided in config to read data.")

        if not stream:
            logger.error("mongodb_read_missing_stream")
            raise ValueError("Collection name (stream) must be provided to read data.")

        with self:
            db = self._connection[self.database]
            collection = db[stream]

            mongo_filter = query if query else {}

            # Incremental sync
            if state and self.config.get("replication_key"):
                key = self.config["replication_key"]
                last_value = state.get("cursor_value")

                logger.debug(
                    "mongodb_read_incremental_state_applied",
                    replication_key=key,
                    last_value=str(last_value),
                )

                if last_value:
                    mongo_filter[key] = {"$gt": last_value}

            try:
                cursor = collection.find(mongo_filter)
                for doc in cursor:
                    doc.pop("_id", None)

                    logger.debug(
                        "mongodb_record_fetched",
                        stream=stream,
                    )

                    yield Record(stream=stream, data=doc)

            except Exception as e:
                logger.error(
                    "mongodb_read_failed",
                    stream=stream,
                    error=str(e),
                    exc_info=True,
                )
                raise

    # ===================================================================
    # Record Count
    # ===================================================================
    def get_record_count(self, stream: str) -> int:
        logger.info(
            "mongodb_record_count_requested",
            database=self.database,
            stream=stream,
        )

        if not self.database or not stream:
            logger.error(
                "mongodb_record_count_invalid_config",
                database=self.database,
                stream=stream,
            )
            raise ValueError("Database and collection name must be provided.")

        with self:
            try:
                db = self._connection[self.database]
                count = db[stream].count_documents({})

                logger.info(
                    "mongodb_record_count_retrieved",
                    stream=stream,
                    record_count=count,
                )

                return count

            except Exception as e:
                logger.error(
                    "mongodb_record_count_failed",
                    stream=stream,
                    error=str(e),
                    exc_info=True,
                )
                raise
