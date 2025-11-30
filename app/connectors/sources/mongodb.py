from collections.abc import Iterator
from typing import Any

from app.connectors.base import ConnectionTestResult, Record, Schema, SourceConnector, Table, Column, DataType
from app.connectors.utils import map_mongo_type_to_data_type
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure


class MongoDBSource(SourceConnector):
    """
    MongoDB Source Connector.
    Connects to a MongoDB database and extracts data from collections.
    """

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.host = config.get("host")
        self.port = config.get("port", 27017)
        self.username = config.get("username")
        self.password = config.get("password")
        self.database = config.get("database")
        self.auth_source = config.get("auth_source", self.database) # Database to authenticate against

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

    def discover_schema(self) -> Schema:
        """Discover and return schema metadata."""
        if not self.database:
            raise ValueError("Database name must be provided in config to discover schema.")

        with self:
            db = self._connection[self.database]
            tables = [] # In MongoDB, "tables" are collections

            for collection_name in db.list_collection_names():
                # For simplicity, we sample a few documents to infer schema
                # In a real-world scenario, you might want a more robust schema inference library
                sample_document = db[collection_name].find_one()
                columns = []

                if sample_document:
                    for key, value in sample_document.items():
                        # Exclude MongoDB's default _id field for cleaner schema
                        if key == "_id":
                            continue
                        
                        # Infer data type based on Python type
                        data_type = map_mongo_type_to_data_type(type(value))
                        columns.append(Column(name=key, data_type=data_type, nullable=True)) # Assuming nullable by default

                tables.append(Table(name=collection_name, schema=self.database, columns=columns))
            
            return Schema(tables=tables)

    def read(self, stream: str, state: dict | None = None, query: dict | None = None) -> Iterator[Record]:
        """Read data from MongoDB collection."""
        if not self.database:
            raise ValueError("Database name must be provided in config to read data.")
        if not stream:
            raise ValueError("Collection name (stream) must be provided to read data.")

        with self:
            db = self._connection[self.database]
            collection = db[stream]

            # Build query filter (if provided)
            mongo_filter = query if query else {}

            # Apply state for incremental sync (example: using a timestamp or _id)
            if state and self.config.get("replication_key"):
                replication_key = self.config["replication_key"]
                last_replicated_value = state.get("cursor_value")
                if last_replicated_value:
                    mongo_filter[replication_key] = {"$gt": last_replicated_value}
                # TODO: Handle different data types for replication_key and cursor_value comparison

            cursor = collection.find(mongo_filter)
            for doc in cursor:
                # Remove MongoDB's default _id field as it's often not needed downstream
                doc.pop("_id", None) 
                yield Record(stream=stream, data=doc)

    def get_record_count(self, stream: str) -> int:
        """Get total record count for a stream."""
        if not self.database or not stream:
            raise ValueError("Database and collection name must be provided.")
        with self:
            db = self._connection[self.database]
            return db[stream].count_documents({})
