"""
Universal Connector Base Layer
------------------------------

This layer provides a UNIVERSAL, STREAM-AGNOSTIC interface for all
sources and destinations.

It works for:
✓ SQL
✓ NoSQL
✓ Filesystems
✓ Cloud storage (S3/GCS/Azure)
✓ APIs
✓ Singer taps
✓ Anything else

No assumptions are made at the engine level.
Connectors define their own extraction semantics.
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from abc import ABC, abstractmethod
from typing import Any, Optional, Iterator, Dict, List, Type
from enum import Enum


# ============================================================
# UNIVERSAL DATA MODEL
# ============================================================
class DataType(str, Enum):
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
    name: str
    data_type: DataType
    nullable: bool = True
    primary_key: bool = False
    default_value: Any = None


@dataclass
class Table:
    name: str
    columns: List[Column]
    schema: Optional[str] = None


@dataclass
class Schema:
    tables: List[Table]
    discovered_at: datetime = None

    def __post_init__(self):
        if self.discovered_at is None:
            self.discovered_at = datetime.now(timezone.utc)


@dataclass
class Record:
    """One universal record flowing through the ETL system."""

    data: Dict[str, Any]
    stream: Optional[str] = None
    extracted_at: datetime = None

    def __post_init__(self):
        if self.extracted_at is None:
            self.extracted_at = datetime.now(timezone.utc)


@dataclass
class State:
    """Incremental sync state."""

    stream: Optional[str]
    cursor_field: Optional[str] = None
    cursor_value: Any = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class ConnectionTestResult:
    def __init__(
        self, success: bool, message: str = "", metadata: dict[str, Any] = None
    ):
        self.success = success
        self.message = message
        self.metadata = metadata or {}
        self.tested_at = datetime.now(timezone.utc)


# ============================================================
# UNIVERSAL SOURCE CONNECTOR BASE
# ============================================================
class SourceConnector(ABC):
    """
    Universal Source Connector Contract
    -----------------------------------
    Each source defines:
    ✓ how streams are named (or none)
    ✓ how filtering/query works (or none)
    ✓ how records are extracted
    """

    def __init__(self, config: Any):
        self.config = config
        self._connection = None

    # ------------------------------
    # REQUIRED
    # ------------------------------
    @abstractmethod
    def test_connection(self) -> ConnectionTestResult:
        pass

    @abstractmethod
    def read(
        self,
        stream: Optional[str],
        state: Optional[State],
        query: Optional[Any],
    ) -> Iterator[Record]:
        """Extracts data from the source."""
        pass

    # ------------------------------
    # OPTIONAL CAPABILITIES
    # ------------------------------
    def discover_schema(self) -> Optional[Schema]:
        """Optional. Some connectors don't have a schema (e.g. API, filesystem)."""
        return None

    def get_stream_identifier(
        self, pipeline_source_config: Dict[str, Any]
    ) -> Optional[str]:
        """Each connector decides how to interpret pipeline config."""
        return pipeline_source_config.get("stream")

    def get_query(self, pipeline_source_config: Dict[str, Any]) -> Optional[Any]:
        """Optional filter/query."""
        return pipeline_source_config.get("query")

    def supports_streams(self) -> bool:
        return True

    def supports_query(self) -> bool:
        return False

    # ------------------------------
    # LIFECYCLE
    # ------------------------------
    def connect(self):
        pass

    def disconnect(self):
        if self._connection and hasattr(self._connection, "close"):
            self._connection.close()
            self._connection = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *exc):
        self.disconnect()


# ============================================================
# UNIVERSAL DESTINATION CONNECTOR BASE
# ============================================================
class DestinationConnector(ABC):
    """
    Universal Destination Connector Contract
    """

    def __init__(self, config: Any):
        self.config = config
        self._connection = None

    @abstractmethod
    def test_connection(self) -> ConnectionTestResult:
        pass

    @abstractmethod
    def write(self, records: Iterator[Record]) -> int:
        """Consume an iterator of universal Records."""
        pass

    def create_stream(self, stream: Optional[str], schema: Optional[List[Column]]):
        """Optional for systems without schemas (e.g. file targets)."""
        return

    def supports_schema_creation(self) -> bool:
        return False

    def supports_batching(self) -> bool:
        return False

    # lifecycle
    def connect(self):
        pass

    def disconnect(self):
        if self._connection and hasattr(self._connection, "close"):
            self._connection.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *exc):
        self.disconnect()


# ============================================================
# UNIVERSAL CONNECTOR FACTORY
# ============================================================
class ConnectorFactory:
    """Maps connector_type → (class, config_model)."""

    _sources: Dict[str, tuple[Type[SourceConnector], Type]] = {}
    _destinations: Dict[str, tuple[Type[DestinationConnector], Type]] = {}

    @classmethod
    def register_source(cls, name: str, connector_cls, config_model):
        cls._sources[name] = (connector_cls, config_model)

    @classmethod
    def register_destination(cls, name: str, connector_cls, config_model):
        cls._destinations[name] = (connector_cls, config_model)

    @classmethod
    def create_source(cls, name: str, config: Dict[str, Any]) -> SourceConnector:
        if name not in cls._sources:
            raise ValueError(f"Unknown source connector '{name}'")
        cls_, cfg = cls._sources[name]
        return cls_(cfg(**config))

    @classmethod
    def create_destination(
        cls, name: str, config: Dict[str, Any]
    ) -> DestinationConnector:
        if name not in cls._destinations:
            raise ValueError(f"Unknown destination '{name}'")
        cls_, cfg = cls._destinations[name]
        return cls_(cfg(**config))

    @classmethod
    def list_sources(cls) -> List[str]:
        return list(cls._sources.keys())

    @classmethod
    def list_destinations(cls) -> List[str]:
        return list(cls._destinations.keys())
