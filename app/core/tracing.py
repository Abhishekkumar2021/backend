"""
Correlation ID & Request Tracing
---------------------------------

Generates and manages correlation IDs that flow through:
✓ API requests → Jobs → Pipeline Runs → Operator Runs
✓ All logs via structlog contextvars
✓ Database records for querying
"""

import uuid
from typing import Optional, Dict, Any
from contextvars import ContextVar

from app.core.logging import bind_trace_ids, clear_trace_ids

# =============================================================================
# CONTEXT STORAGE (thread-safe)
# =============================================================================
_correlation_id: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)


# =============================================================================
# CORRELATION ID GENERATION
# =============================================================================

def generate_correlation_id() -> str:
    """Generate a unique correlation ID (short UUID)."""
    return str(uuid.uuid4())[:8]  # 8 chars for readability


def generate_request_id() -> str:
    """Generate a unique request ID."""
    return f"req_{str(uuid.uuid4())[:12]}"


def generate_task_id() -> str:
    """Generate a unique Celery task ID (if not provided)."""
    return f"task_{str(uuid.uuid4())[:12]}"


# =============================================================================
# CORRELATION ID MANAGEMENT
# =============================================================================

def set_correlation_id(correlation_id: str) -> None:
    """
    Set correlation ID in context and bind to structlog.
    
    Args:
        correlation_id: Correlation ID to set
    """
    _correlation_id.set(correlation_id)
    bind_trace_ids(correlation_id=correlation_id)


def get_correlation_id() -> Optional[str]:
    """Get current correlation ID from context."""
    return _correlation_id.get()


def ensure_correlation_id() -> str:
    """
    Get existing correlation ID or generate a new one.
    
    Returns:
        Correlation ID (existing or new)
    """
    corr_id = get_correlation_id()
    if not corr_id:
        corr_id = generate_correlation_id()
        set_correlation_id(corr_id)
    return corr_id


def clear_correlation_context() -> None:
    """Clear correlation context (call at end of request/task)."""
    _correlation_id.set(None)
    clear_trace_ids()


# =============================================================================
# TRACE CONTEXT BUILDER
# =============================================================================

class TraceContext:
    """
    Builder for trace context that flows through the system.
    
    Usage:
        ctx = TraceContext.from_api_request(request_id="req_abc123")
        ctx.add_job(job_id=42)
        ctx.add_pipeline_run(run_id=100)
        ctx.bind()  # Apply to structlog
    """
    
    def __init__(self, correlation_id: Optional[str] = None):
        self.correlation_id = correlation_id or generate_correlation_id()
        self.trace_data: Dict[str, Any] = {
            "correlation_id": self.correlation_id
        }
    
    @classmethod
    def from_api_request(cls, request_id: Optional[str] = None) -> "TraceContext":
        """Create context from API request."""
        ctx = cls()
        ctx.trace_data["request_id"] = request_id or generate_request_id()
        return ctx
    
    @classmethod
    def from_celery_task(cls, task_id: str, correlation_id: Optional[str] = None) -> "TraceContext":
        """Create context from Celery task."""
        ctx = cls(correlation_id=correlation_id)
        ctx.trace_data["task_id"] = task_id
        return ctx
    
    def add_job(self, job_id: int) -> "TraceContext":
        """Add job ID to context."""
        self.trace_data["job_id"] = job_id
        return self
    
    def add_pipeline(self, pipeline_id: int) -> "TraceContext":
        """Add pipeline ID to context."""
        self.trace_data["pipeline_id"] = pipeline_id
        return self
    
    def add_pipeline_run(self, pipeline_run_id: int) -> "TraceContext":
        """Add pipeline run ID to context."""
        self.trace_data["pipeline_run_id"] = pipeline_run_id
        return self
    
    def add_operator_run(self, operator_run_id: int) -> "TraceContext":
        """Add operator run ID to context."""
        self.trace_data["operator_run_id"] = operator_run_id
        return self
    
    def bind(self) -> None:
        """Apply trace context to structlog."""
        set_correlation_id(self.correlation_id)
        bind_trace_ids(**self.trace_data)
    
    def to_dict(self) -> Dict[str, Any]:
        """Export trace data as dict (for storing in DB)."""
        return self.trace_data.copy()
    
    def __repr__(self) -> str:
        return f"TraceContext({self.trace_data})"


# =============================================================================
# HELPERS FOR COMMON PATTERNS
# =============================================================================

def start_api_request(request_id: Optional[str] = None) -> TraceContext:
    """
    Start tracking an API request.
    
    Usage:
        ctx = start_api_request()
        ctx.bind()
    """
    return TraceContext.from_api_request(request_id)


def start_pipeline_job(
    task_id: str,
    job_id: int,
    pipeline_id: int,
    correlation_id: Optional[str] = None
) -> TraceContext:
    """
    Start tracking a pipeline job execution.
    
    Usage:
        ctx = start_pipeline_job(task_id, job_id, pipeline_id)
        ctx.bind()
    """
    return (
        TraceContext.from_celery_task(task_id, correlation_id)
        .add_job(job_id)
        .add_pipeline(pipeline_id)
    )