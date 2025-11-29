"""
Base Connector Classes - Plugin Architecture
All source and destination connectors inherit from these
"""
from abc import ABC, abstractmethod
from typing import Iterator, Dict, Any, List, Optional
from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class DataType(str, Enum):
    """Standard data types across all connectors"""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    JSON = "json"
    BINARY = "binary"
    NULL = "null"


@dataclass
class Column:
    """Column metadata"""
    name: str
    data_type: DataType
    nullable: bool = True
    primary_key: bool = False
    foreign_key: Optional[str] = None  # Format: "table_name.column_name"
    default_value: Optional[Any] = None
    description: Optional[str] = None


@dataclass
class Table:
    """Table/Collection metadata"""
    name: str
    schema: Optional[str] = None  # Database schema (for SQL)
    columns: List[Column] = None
    row_count: Optional[int] = None
    description: Optional[str] = None
    
    def __post_init__(self):
        if self.columns is None:
            self.columns = []


@dataclass
class Schema:
    """Complete schema metadata for a connection"""
    tables: List[Table]
    version: Optional[str] = None
    discovered_at: datetime = None
    
    def __post_init__(self):
        if self.discovered_at is None:
            self.discovered_at = datetime.utcnow()


@dataclass
class Record:
    """
    Single data record - standard format across all connectors
    Similar to Singer.io RECORD message
    """
    stream: str  # Table/collection name
    data: Dict[str, Any]
    time_extracted: datetime = None
    
    def __post_init__(self):
        if self.time_extracted is None:
            self.time_extracted = datetime.utcnow()


@dataclass
class State:
    """
    Incremental sync state - tracks position in source data
    Similar to Singer.io STATE message
    """
    stream: str
    cursor_field: Optional[str] = None  # Field used for ordering (timestamp, id)
    cursor_value: Optional[Any] = None  # Last synced value
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class ConnectionTestResult:
    """Result of connection health check"""
    def __init__(self, success: bool, message: str = "", metadata: Dict[str, Any] = None):
        self.success = success
        self.message = message
        self.metadata = metadata or {}
        self.tested_at = datetime.utcnow()


class SourceConnector(ABC):
    """
    Abstract base class for all source connectors
    Implements data extraction from various systems
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize connector with configuration
        
        Args:
            config: Connection configuration (host, port, credentials, etc.)
        """
        self.config = config
        self._connection = None
    
    @abstractmethod
    def test_connection(self) -> ConnectionTestResult:
        """
        Test if connection is valid and accessible
        
        Returns:
            ConnectionTestResult with success status and message
        """
        pass
    
    @abstractmethod
    def discover_schema(self) -> Schema:
        """
        Discover and return schema metadata
        
        Returns:
            Schema object with tables and columns
        """
        pass
    
    @abstractmethod
    def read(
        self,
        stream: str,
        state: Optional[State] = None,
        query: Optional[str] = None
    ) -> Iterator[Record]:
        """
        Read data from source
        
        Args:
            stream: Table/collection name to read from
            state: Optional state for incremental sync
            query: Optional custom query (for SQL sources)
            
        Yields:
            Record objects
        """
        pass
    
    @abstractmethod
    def get_record_count(self, stream: str) -> int:
        """
        Get total record count for a stream
        
        Args:
            stream: Table/collection name
            
        Returns:
            Number of records
        """
        pass
    
    def connect(self) -> None:
        """Establish connection to source (optional override)"""
        pass
    
    def disconnect(self) -> None:
        """Close connection to source"""
        if self._connection:
            if hasattr(self._connection, 'close'):
                self._connection.close()
            self._connection = None
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()


class DestinationConnector(ABC):
    """
    Abstract base class for all destination connectors
    Implements data loading to various systems
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize connector with configuration
        
        Args:
            config: Connection configuration
        """
        self.config = config
        self._connection = None
        self._batch = []
        self._batch_size = config.get("batch_size", 1000)
    
    @abstractmethod
    def test_connection(self) -> ConnectionTestResult:
        """
        Test if connection is valid and accessible
        
        Returns:
            ConnectionTestResult with success status
        """
        pass
    
    @abstractmethod
    def write(self, records: Iterator[Record]) -> int:
        """
        Write records to destination
        
        Args:
            records: Iterator of Record objects
            
        Returns:
            Number of records written
        """
        pass
    
    @abstractmethod
    def create_stream(self, stream: str, schema: List[Column]) -> None:
        """
        Create table/collection if it doesn't exist
        
        Args:
            stream: Table/collection name
            schema: List of Column objects defining structure
        """
        pass
    
    def connect(self) -> None:
        """Establish connection to destination (optional override)"""
        pass
    
    def disconnect(self) -> None:
        """Close connection and flush remaining data"""
        if self._batch:
            self._flush_batch()
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def _flush_batch(self) -> None:
        """Flush batch to destination (optional override for batching)"""
        pass
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()


class ConnectorFactory:
    """
    Factory for creating connector instances
    Dynamically loads connector classes based on type
    """
    
    _source_connectors: Dict[str, type] = {}
    _destination_connectors: Dict[str, type] = {}
    
    @classmethod
    def register_source(cls, connector_type: str, connector_class: type):
        """Register a source connector class"""
        cls._source_connectors[connector_type] = connector_class
    
    @classmethod
    def register_destination(cls, connector_type: str, connector_class: type):
        """Register a destination connector class"""
        cls._destination_connectors[connector_type] = connector_class
    
    @classmethod
    def create_source(cls, connector_type: str, config: Dict[str, Any]) -> SourceConnector:
        """
        Create source connector instance
        
        Args:
            connector_type: Type of connector (postgresql, mysql, etc.)
            config: Connection configuration
            
        Returns:
            SourceConnector instance
        """
        if connector_type not in cls._source_connectors:
            raise ValueError(f"Unknown source connector type: {connector_type}")
        
        connector_class = cls._source_connectors[connector_type]
        return connector_class(config)
    
    @classmethod
    def create_destination(cls, connector_type: str, config: Dict[str, Any]) -> DestinationConnector:
        """
        Create destination connector instance
        
        Args:
            connector_type: Type of connector
            config: Connection configuration
            
        Returns:
            DestinationConnector instance
        """
        if connector_type not in cls._destination_connectors:
            raise ValueError(f"Unknown destination connector type: {connector_type}")
        
        connector_class = cls._destination_connectors[connector_type]
        return connector_class(config)
    
    @classmethod
    def list_sources(cls) -> List[str]:
        """List all registered source connector types"""
        return list(cls._source_connectors.keys())
    
    @classmethod
    def list_destinations(cls) -> List[str]:
        """List all registered destination connector types"""
        return list(cls._destination_connectors.keys())