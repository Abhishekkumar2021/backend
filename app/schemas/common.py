from datetime import datetime
from typing import Generic, Optional, TypeVar, List

from pydantic import BaseModel, Field

T = TypeVar("T")


# =============================================================================
# BASE RESPONSE
# =============================================================================

class ResponseBase(BaseModel):
    """
    Universal base API response.
    Works for both success + failure responses.
    """

    success: bool = Field(True, description="Indicates whether the operation succeeded")
    message: Optional[str] = Field(
        None,
        description="Human-friendly message describing the result"
    )
    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="UTC timestamp of the response"
    )
    errors: Optional[dict] = Field(
        default=None,
        description="Optional dictionary of error details (validation, business logic, etc.)"
    )


# =============================================================================
# SINGLE OBJECT RESPONSE
# =============================================================================

class DataResponse(ResponseBase, Generic[T]):
    """
    Response wrapper for a single object.
    Auto-documents beautifully in FastAPI / OpenAPI.
    """
    data: Optional[T] = Field(
        None,
        description="Payload returned from the request"
    )


# =============================================================================
# LIST + PAGINATION RESPONSE
# =============================================================================

class ListResponse(ResponseBase, Generic[T]):
    """
    Response for returning list results.
    Supports pagination metadata.
    """
    data: List[T] = Field(default_factory=list)
    total: Optional[int] = Field(
        None,
        description="Total number of items available (for pagination)"
    )
    page: Optional[int] = Field(None, description="Current page number")
    page_size: Optional[int] = Field(None, description="Size of each page")


# =============================================================================
# SIMPLE ACK RESPONSE (no data)
# =============================================================================

class AckResponse(ResponseBase):
    """
    Used for simple success/failure acknowledgements.
    Example: DELETE endpoints or operations with no return payload.
    """
    pass
