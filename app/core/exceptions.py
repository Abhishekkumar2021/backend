"""Custom exception hierarchy for the application."""


class DataAgentException(Exception):
    """Base exception for all application errors."""
    pass


class ConfigurationError(DataAgentException):
    """Configuration-related errors."""
    pass


class EncryptionError(DataAgentException):
    """Encryption/decryption errors."""
    pass


class InvalidMasterPasswordError(EncryptionError):
    """Master password validation failed."""
    pass


class ConnectorError(DataAgentException):
    """Connector-related errors."""
    pass


class ConnectionTestError(ConnectorError):
    """Connection test failed."""
    pass


class SchemaDiscoveryError(ConnectorError):
    """Schema discovery failed."""
    pass


class PipelineExecutionError(DataAgentException):
    """Pipeline execution errors."""
    pass


class StateManagementError(DataAgentException):
    """State persistence errors."""
    pass


class ValidationError(DataAgentException):
    """Input validation errors."""
    pass


class CacheError(DataAgentException):
    """Cache-related errors."""
    pass