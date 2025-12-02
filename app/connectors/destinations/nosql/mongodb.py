"""
MongoDB Destination Connector — Unified ETL Framework Edition
Fully standardized to match PostgreSQL / MySQL / Oracle / MSSQL / SQLite connectors.
"""

from __future__ import annotations
from collections.abc import Iterator
from typing import Any, List

from pymongo import MongoClient, errors
from pymongo.errors import ConnectionFailure, OperationFailure

from app.connectors.base import (
    ConnectionTestResult,
    DestinationConnector,
    Record,
    Column,
)
from app.core.logging import get_logger
from app.schemas.connector_configs import MongoDBConfig

logger = get_logger(__name__)


class MongoDBDestination(DestinationConnector):
    """
    MongoDB destination connector.
    Supports batched inserts, collection auto-creation,
    and full structured logging (fast, safe, no PII).
    """

    def __init__(self, config: MongoDBConfig):
        super().__init__(config)

        self.host = config.host
        self.port = config.port
        self.username = config.username
        self.password = config.password.get_secret_value() if config.password else None
        self.database = config.database
        self.auth_source = config.auth_source or self.database

        # Provided dynamically from pipeline settings
        self.collection = getattr(config, "collection", None)

        self._batch_size = config.batch_size
        self._connection: MongoClient | None = None

        logger.debug(
            "mongodb_destination_initialized",
            host=self.host,
            port=self.port,
            database=self.database,
            collection=self.collection,
            batch_size=self._batch_size,
        )

    # =========================================================================
    # CONNECT / DISCONNECT
    # =========================================================================
    def connect(self) -> None:
        """Establish MongoDB connection."""
        if self._connection:
            return

        logger.info(
            "mongodb_destination_connect_requested",
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
            # Validate connection immediately
            self._connection.admin.command("ping")

            logger.info(
                "mongodb_destination_connected",
                host=self.host,
                port=self.port,
            )

        except ConnectionFailure as e:
            logger.error(
                "mongodb_destination_connection_failed",
                error=str(e),
                exc_info=True,
            )
            raise ConnectionFailure(f"MongoDB connection failed: {e}") from e

        except OperationFailure as e:
            message = e.details.get("errmsg", str(e))
            logger.error(
                "mongodb_destination_auth_failed",
                error=message,
                exc_info=True,
            )
            raise OperationFailure(message) from e

        except Exception as e:
            logger.error(
                "mongodb_destination_connect_unexpected_error",
                error=str(e),
                exc_info=True,
            )
            raise RuntimeError(f"Unexpected MongoDB error: {e}") from e

    def disconnect(self) -> None:
        """Close connection safely."""
        if not self._connection:
            return

        try:
            self._connection.close()
            logger.info("mongodb_destination_disconnected")
        except Exception as e:
            logger.warning(
                "mongodb_destination_disconnect_failed",
                error=str(e),
                exc_info=True,
            )
        finally:
            self._connection = None

    # =========================================================================
    # TEST CONNECTION
    # =========================================================================
    def test_connection(self) -> ConnectionTestResult:
        logger.info("mongodb_destination_test_requested")

        try:
            with self:
                # just opening connection is enough for test
                pass

            return ConnectionTestResult(True, "Successfully connected to MongoDB.")

        except ConnectionFailure as e:
            logger.error("mongodb_destination_test_failed", error=str(e))
            return ConnectionTestResult(False, f"Connection failed: {e}")

        except OperationFailure as e:
            msg = e.details.get("errmsg", str(e))
            return ConnectionTestResult(False, f"Authentication failed: {msg}")

        except Exception as e:
            return ConnectionTestResult(False, f"Unexpected error: {e}")

    # =========================================================================
    # CREATE STREAM
    # =========================================================================
    def create_stream(self, stream: str, schema: List[Column]) -> None:
        """
        Ensure collection exists. MongoDB collections are schema-less,
        but we still provide this hook for consistency with other connectors.
        """
        if not self.database:
            raise ValueError("MongoDB database must be provided.")

        if not stream:
            raise ValueError("Collection (stream) must be provided.")

        logger.info("mongodb_destination_create_stream_requested", stream=stream)

        with self:
            db = self._connection[self.database]
            if stream not in db.list_collection_names():
                db.create_collection(stream)
                logger.info("mongodb_destination_stream_created", stream=stream)
            else:
                logger.debug("mongodb_destination_stream_exists", stream=stream)

        # Optional: Could infer/create indexes *based on schema* in the future

    # =========================================================================
    # WRITE
    # =========================================================================
    def write(self, records: Iterator[Record]) -> int:
        """
        Insert records into MongoDB in batches.
        Very fast when using insert_many.
        """
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

        inserted_total = 0
        batch: List[dict[str, Any]] = []

        with self:
            db = self._connection[self.database]
            collection = db[self.collection]

            for record in records:
                batch.append(record.data)
                if len(batch) >= self._batch_size:
                    inserted_total += self._flush_batch(collection, batch)
                    batch = []

            # Flush remaining
            if batch:
                inserted_total += self._flush_batch(collection, batch)

        logger.info(
            "mongodb_destination_write_completed",
            records_inserted=inserted_total,
        )
        return inserted_total

    # =========================================================================
    # BATCH FLUSH
    # =========================================================================
    def _flush_batch(self, collection, batch: List[dict[str, Any]]) -> int:
        """Insert batch into MongoDB with error handling."""
        if not batch:
            return 0

        try:
            result = collection.insert_many(batch)
            count = len(result.inserted_ids)

            logger.debug(
                "mongodb_destination_batch_write_success",
                batch_size=len(batch),
            )

            return count

        except errors.BulkWriteError as bwe:
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
