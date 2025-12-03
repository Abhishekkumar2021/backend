"""
FastAPI Middleware for Request Tracing
---------------------------------------

Injects correlation IDs into:
✓ Request context
✓ Response headers
✓ Structured logs
"""

from typing import Callable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from app.core.tracing import (
    start_api_request,
    clear_correlation_context,
    get_correlation_id,
)
from app.core.logging import get_logger

logger = get_logger(__name__)


class CorrelationMiddleware(BaseHTTPMiddleware):
    """
    Middleware that:
    1. Extracts or generates correlation ID
    2. Binds to structlog context
    3. Adds to response headers
    4. Clears context after request
    """
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Extract correlation ID from header or generate new
        correlation_id = request.headers.get("X-Correlation-ID")
        request_id = request.headers.get("X-Request-ID")
        
        # Create trace context
        ctx = start_api_request(request_id=request_id)
        if correlation_id:
            ctx.correlation_id = correlation_id
            ctx.trace_data["correlation_id"] = correlation_id
        
        ctx.bind()
        
        # Log request
        logger.info(
            "request_started",
            method=request.method,
            path=request.url.path,
            client=request.client.host if request.client else None,
        )
        
        try:
            # Process request
            response = await call_next(request)
            
            # Add correlation ID to response headers
            response.headers["X-Correlation-ID"] = get_correlation_id() or "unknown"
            response.headers["X-Request-ID"] = ctx.trace_data.get("request_id", "unknown")
            
            # Log response
            logger.info(
                "request_completed",
                method=request.method,
                path=request.url.path,
                status_code=response.status_code,
            )
            
            return response
        
        except Exception as e:
            logger.exception(
                "request_failed",
                method=request.method,
                path=request.url.path,
                error=str(e),
            )
            raise
        
        finally:
            # Always clear context
            clear_correlation_context()


# =============================================================================
# Add to main.py:
# =============================================================================
"""
from app.core.middleware import CorrelationMiddleware

app = FastAPI(...)
app.add_middleware(CorrelationMiddleware)
"""