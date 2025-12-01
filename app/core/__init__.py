"""Core module initialization.

Exports commonly used core components.
"""

# Import settings first (without triggering validation errors)
from app.core.config import settings

# Only import other modules after settings is loaded
from app.core.exceptions import (
    DataAgentException,
    ConfigurationError,
    EncryptionError,
    InvalidMasterPasswordError,
    ConnectorError,
    ConnectionTestError,
    SchemaDiscoveryError,
    PipelineExecutionError,
    StateManagementError,
    ValidationError,
)

__all__ = [
    "settings",
    "DataAgentException",
    "ConfigurationError",
    "EncryptionError",
    "InvalidMasterPasswordError",
    "ConnectorError",
    "ConnectionTestError",
    "SchemaDiscoveryError",
    "PipelineExecutionError",
    "StateManagementError",
    "ValidationError",
]