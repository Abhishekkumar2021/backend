from collections.abc import Iterator
from typing import Any, List

from pymongo import MongoClient, errors
from pymongo.errors import ConnectionFailure, OperationFailure

from app.connectors.base import ConnectionTestResult, DestinationConnector, Record, Column
from app.core.logging import get_logger
from app.schemas.connector_configs import MongoDBConfig

logger = get_logger(__name__)


class MongoDBDestination(DestinationConnector):
    """
    MongoDB Destination Connector.
    Handles writing batches of records into a MongoDB collection.
    """

    def __init__(self, config: MongoDBConfig):
        super().__init__(config)
        self.host = config.host
        self.port = config.port
        self.username = config.username
        self.password = config.password.get_secret_value() if config.password else None
        self.database = config.database
        self.auth_source = config.auth_source or self.database
        self._batch_size = config.batch_size
        
        # These are pipeline-specific but passed in config due to merging in worker
        # Since we enabled extra="allow" in BaseConnectorConfig, we can access them.
        self.collection = getattr(config, "collection", None)

        logger.debug(
            "mongodb_destination_initialized",
            host=self.host,
            port=self.port,
            database=self.database,
            collection=self.collection,
        )

    # ----------------------------------------------------------------------
    # Connection
    # ----------------------------------------------------------------------
    def connect(self) -> None:
        """Establish connection to MongoDB."""
        logger.info(
            "mongodb_destination_connecting",
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
            self._connection.admin.command("ismaster")

            logger.info(
                "mongodb_destination_connected",
                host=self.host,
                port=self.port,
            )

        except ConnectionFailure as e:
            logger.error("mongodb_destination_connection_failed", error=str(e), exc_info=True)
            raise ConnectionFailure(f"MongoDB connection failed: {e}")

        except OperationFailure as e:
            logger.error("mongodb_destination_auth_failed", error=str(e), exc_info=True)
            raise OperationFailure(f"MongoDB authentication failed: {e.details.get('errmsg', str(e))}")

        except Exception as e:
            logger.error("mongodb_destination_connect_unexpected_error", error=str(e), exc_info=True)
            raise Exception(f"An unexpected error occurred during MongoDB connection: {e}")

    # ----------------------------------------------------------------------
    # Disconnect
    # ----------------------------------------------------------------------
    def disconnect(self) -> None:
        """Close MongoDB connection."""
        if self._connection:
            try:
                self._connection.close()
                logger.info("mongodb_destination_disconnected")
            except Exception as e:
                logger.warning("mongodb_destination_disconnect_failed", error=str(e))
            finally:
                self._connection = None

    # ----------------------------------------------------------------------
    # Test Connection
    # ----------------------------------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        """Test if connection is valid."""
        logger.info("mongodb_destination_test_connection_started")

        try:
            with self:
                pass

            logger.info("mongodb_destination_test_connection_success")
            return ConnectionTestResult(success=True, message="Successfully connected to MongoDB.")

        except ConnectionFailure as e:
            logger.error("mongodb_destination_test_connection_failed", error=str(e))
            return ConnectionTestResult(success=False, message=f"Failed to connect to MongoDB: {e}")

        except OperationFailure as e:
            logger.error("mongodb_destination_test_auth_failed", error=str(e))
            msg = e.details.get("errmsg", str(e))
            return ConnectionTestResult(success=False, message=f"MongoDB authentication failed: {msg}")

        except Exception as e:
            logger.error("mongodb_destination_test_unexpected_error", error=str(e), exc_info=True)
            return ConnectionTestResult(success=False, message=f"An unexpected error occurred: {e}")

    # ----------------------------------------------------------------------
    # Write
    # ----------------------------------------------------------------------
    def write(self, records: Iterator[Record]) -> int:
        """Write records into MongoDB."""
        if not self.database:
            raise ValueError("Database name must be provided.")
        if not self.collection:
            raise ValueError("Collection name must be provided.")

        logger.info(
            "mongodb_destination_write_started",
            database=self.database,
            collection=self.collection,
            batch_size=self._batch_size,
        )

        inserted_count = 0
        batch: List[dict[str, Any]] = []

        with self:
            db = self._connection[self.database]
            collection = db[self.collection]

            for record in records:
                batch.append(record.data)

                if len(batch) >= self._batch_size:
                    inserted_count += self._flush_batch(collection, batch)
                    batch = []

            if batch:
                inserted_count += self._flush_batch(collection, batch)

        logger.info("mongodb_destination_write_completed", records_inserted=inserted_count)
        return inserted_count

    # Internal batch-flush helper
    def _flush_batch(self, collection, batch: List[dict[str, Any]]) -> int:
        """Insert a batch into MongoDB with structured logging."""
        try:
            result = collection.insert_many(batch)
            count = len(result.inserted_ids)

            logger.debug(
                "mongodb_destination_write_batch_success",
                inserted_count=count,
            )
            return count

        except errors.BulkWriteError as bwe:
            # Only logs metadata (never logs actual documents)
            inserted = bwe.details.get("nInserted", 0)
            logger.error(
                "mongodb_destination_bulk_write_error",
                inserted=inserted,
                error=str(bwe),
                exc_info=True,
            )
            return inserted

        except Exception as e:
            logger.error(
                "mongodb_destination_insert_failed",
                error=str(e),
                exc_info=True,
            )
            return 0

    # ----------------------------------------------------------------------
    # Stream Creation
    # ----------------------------------------------------------------------
    def create_stream(self, stream: str, schema: List[Column]) -> None:
        """Ensure collection exists; optionally create indexes."""
        if not self.database or not stream:
            raise ValueError("Database and collection name must be provided.")

        logger.info("mongodb_destination_create_stream", stream=stream)

        with self:
            db = self._connection[self.database]

            if stream not in db.list_collection_names():
                db.create_collection(stream)
                logger.info("mongodb_destination_stream_created", stream=stream)
            else:
                logger.debug("mongodb_destination_stream_exists", stream=stream)

            # Future: index creation from schema
            # logger.debug("mongodb_destination_index_creation_skipped")
