from pydantic import BaseModel, Field

class InitSystemRequest(BaseModel):
    """Request model for system initialization."""
    master_password: str = Field(
        ...,
        min_length=8,
        description="Master password for encryption (minimum 8 characters)",
    )

class InitSystemResponse(BaseModel):
    """Generic simple response."""
    message: str
    status: str

class SystemStatusResponse(BaseModel):
    """Response model for system status."""
    initialized: bool
    unlocked: bool
    message: str
