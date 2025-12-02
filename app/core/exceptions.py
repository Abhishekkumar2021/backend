"""
Custom exception hierarchy for the Universal ETL platform.
Each exception supports optional trace metadata for debugging.
"""

from typing import Optional


class DataAgentException(Exception):
    """
    Base exception for all application errors.
    Supports binding pipeline/task identifiers for debugging & monitoring.
    """

    def __init__(
        self,
        message: str,
        *,
        pipeline_id: Optional[str] = None,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        operator_id: Optional[str] = None,
    ):
        super().__init__(message)
        self.message = message
        self.pipeline_id = pipeline_id
        self.run_id = run_id
        self.task_id = task_id
        self.operator_id = operator_id

    def to_dict(self):
        """Structured error output for logs/monitoring."""
        return {
            "error": self.__class__.__name__,
            "message": self.message,
            "pipeline_id": self.pipeline_id,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "operator_id": self.operator_id,
        }

    def __str__(self):
        meta = {
            k: v for k, v in self.to_dict().items() if v is not None and k != "error"
        }
        return f"{self.__class__.__name__}: {self.message} | meta={meta}"


# =============================================================
# Configuration / Validation
# =============================================================
class ConfigurationError(DataAgentException):
    pass


class ValidationError(DataAgentException):
    pass


# =============================================================
# Security / Encryption
# =============================================================
class EncryptionError(DataAgentException):
    pass


class InvalidMasterPasswordError(EncryptionError):
    pass


# =============================================================
# Connectors
# =============================================================
class ConnectorError(DataAgentException):
    pass


class ConnectionTestError(ConnectorError):
    pass


class SchemaDiscoveryError(ConnectorError):
    pass


# =============================================================
# ETL Pipeline Errors
# =============================================================
class PipelineExecutionError(DataAgentException):
    """General pipeline-level failure."""
    pass


class ExtractError(PipelineExecutionError):
    """Extraction stage failed."""
    pass


class TransformError(PipelineExecutionError):
    """Transformation stage failed."""
    pass


class LoadError(PipelineExecutionError):
    """Loading stage failed."""
    pass


class StateManagementError(DataAgentException):
    """State persistence errors."""
    pass


# =============================================================
# Caching
# =============================================================
class CacheError(DataAgentException):
    pass


# =============================================================
# Celery-specific
# =============================================================
class RetryableError(DataAgentException):
    """
    Signals Celery that the task may be retried.
    e.g., transient network issues, rate limits.
    """
    pass


class NonRetryableError(DataAgentException):
    """
    Irrecoverable error — Celery should NOT retry.
    """
    pass
