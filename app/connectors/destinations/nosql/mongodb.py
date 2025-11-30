from collections.abc import Iterator
from typing import Any, List

from app.connectors.base import ConnectionTestResult, DestinationConnector, Record, Column
from pymongo import MongoClient, errors
from pymongo.errors import ConnectionFailure, OperationFailure


class MongoDBDestination(DestinationConnector):
    """
    MongoDB Destination Connector.
    Connects to a MongoDB database and loads data into collections.
    """

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.host = config.get("host")
        self.port = config.get("port", 27017)
        self.username = config.get("username")
        self.password = config.get("password")
        self.database = config.get("database")
        self.collection = config.get("collection")
        self.auth_source = config.get("auth_source", self.database)
        self._batch_size = config.get("batch_size", 1000)

    def connect(self) -> None:
        """Establish connection to MongoDB."""
        try:
            self._connection = MongoClient(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                authSource=self.auth_source,
                serverSelectionTimeoutMS=5000, # 5 second timeout
            )
            # The ismaster command is cheap and does not require auth.
            # This will raise an exception if the connection fails.
            self._connection.admin.command('ismaster')
        except ConnectionFailure as e:
            raise ConnectionFailure(f"MongoDB connection failed: {e}")
        except OperationFailure as e:
            raise OperationFailure(f"MongoDB authentication failed: {e.details.get('errmsg', str(e))}")
        except Exception as e:
            raise Exception(f"An unexpected error occurred during MongoDB connection: {e}")

    def disconnect(self) -> None:
        """Close connection to MongoDB."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def test_connection(self) -> ConnectionTestResult:
        """Test if connection is valid and accessible."""
        try:
            with self: # Use context manager to ensure connection is closed
                return ConnectionTestResult(success=True, message="Successfully connected to MongoDB.")
        except ConnectionFailure as e:
            return ConnectionTestResult(success=False, message=f"Failed to connect to MongoDB: {e}")
        except OperationFailure as e:
            return ConnectionTestResult(success=False, message=f"MongoDB authentication failed: {e.details.get('errmsg', str(e))}")
        except Exception as e:
            return ConnectionTestResult(success=False, message=f"An unexpected error occurred: {e}")

    def write(self, records: Iterator[Record]) -> int:
        """Write records to MongoDB collection."""
        if not self.database:
            raise ValueError("Database name must be provided in config to write data.")
        if not self.collection:
            raise ValueError("Collection name must be provided in config to write data.")

        with self:
            db = self._connection[self.database]
            collection = db[self.collection]
            
            inserted_count = 0
            batch: List[dict[str, Any]] = []

            for record in records:
                batch.append(record.data)
                if len(batch) >= self._batch_size:
                    try:
                        result = collection.insert_many(batch)
                        inserted_count += len(result.inserted_ids)
                        batch = []
                    except errors.BulkWriteError as bwe:
                        # Log or handle individual write errors if necessary
                        print(f"Bulk write error: {bwe.details}")
                        inserted_count += bwe.details['nInserted']
                        batch = [] # Clear batch even on error to prevent re-processing
                    except Exception as e:
                        # Handle other potential errors during insert
                        print(f"Error during bulk insert: {e}")
                        batch = []

            if batch: # Insert any remaining records
                try:
                    result = collection.insert_many(batch)
                    inserted_count += len(result.inserted_ids)
                except errors.BulkWriteError as bwe:
                    print(f"Bulk write error on remaining batch: {bwe.details}")
                    inserted_count += bwe.details['nInserted']
                except Exception as e:
                    print(f"Error during bulk insert of remaining batch: {e}")

            return inserted_count

    def create_stream(self, stream: str, schema: List[Column]) -> None:
        """
        In MongoDB, collections are created implicitly on first insert.
        This method can be used for explicit validation or indexing if needed.
        """
        if not self.database or not stream:
            raise ValueError("Database and collection name must be provided.")
        with self:
            db = self._connection[self.database]
            # Ensure the collection exists by a dummy operation if it doesn't already
            # Or add validation based on schema if required.
            if stream not in db.list_collection_names():
                db.create_collection(stream)
            
            # Optionally, create indexes based on schema information
            # For example, if a column is marked as primary_key, create a unique index
            # for col in schema:
            #     if col.primary_key:
            #         db[stream].create_index(col.name, unique=True)
