from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field

from app.models.database import (
    AlertDeliveryMethod,
    AlertLevel,
    AlertStatus,
    AlertType,
)


# ============================================================================
# ALERT CONFIG (TEMPLATE FOR AUTOMATED ALERTS)
# ============================================================================

class AlertConfigBase(BaseModel):
    """Base fields for alert configuration templates."""
    
    name: str = Field(..., description="Human-readable name of the alert configuration")
    description: Optional[str] = Field(None, description="Description of what this alert monitors")
    alert_type: AlertType = Field(..., description="Trigger type (job_failed, pipeline_timeout, etc.)")
    delivery_method: AlertDeliveryMethod = Field(..., description="How alerts will be delivered (email, webhook)")
    recipient: str = Field(..., description="Email address, webhook URL, Slack channel, ...")
    
    threshold_value: Optional[int] = Field(
        1, description="Minimum threshold before the alert triggers (e.g. failure count)"
    )
    threshold_unit: Optional[str] = Field(
        None, description="Optional unit for threshold (minutes, % etc.)"
    )
    
    enabled: bool = Field(True, description="Whether this alert configuration is active")


class AlertConfigCreate(AlertConfigBase):
    """Schema used when creating a new alert config."""
    pass


class AlertConfigUpdate(BaseModel):
    """
    Safe update schema — all fields optional.
    Does NOT inherit from AlertConfigBase to avoid making fields required.
    """
    
    name: Optional[str] = Field(None, description="Updated name")
    description: Optional[str] = None
    alert_type: Optional[AlertType] = None
    delivery_method: Optional[AlertDeliveryMethod] = None
    recipient: Optional[str] = None
    threshold_value: Optional[int] = None
    threshold_unit: Optional[str] = None
    enabled: Optional[bool] = None


class AlertConfigInDBBase(AlertConfigBase):
    """Fields stored in DB."""
    
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class AlertConfig(AlertConfigInDBBase):
    """Returned via API."""
    pass


# ============================================================================
# ALERT INSTANCE (ACTUAL EVENT)
# ============================================================================

class AlertBase(BaseModel):
    """
    Base fields for actual alert events.
    These are concrete instances created when rules trigger.
    """
    
    message: str = Field(..., description="Human-readable alert message")
    level: AlertLevel = Field(AlertLevel.INFO, description="Severity level (info/warning/error)")
    
    job_id: Optional[int] = Field(
        None,
        description="If alert was triggered for a job run",
    )
    pipeline_id: Optional[int] = Field(
        None,
        description="If alert was triggered for a pipeline",
    )
    
    delivery_method: AlertDeliveryMethod
    recipient: str


class AlertCreate(AlertBase):
    """
    Schema for creating alerts programmatically (from worker or API).
    """
    
    status: Optional[AlertStatus] = Field(
        AlertStatus.PENDING,
        description="Initial alert delivery status"
    )
    alert_config_id: Optional[int] = Field(
        None, description="Link to alert configuration if applicable"
    )


class AlertUpdate(BaseModel):
    """
    Safe update schema for alerts.
    Only modifiable fields allowed.
    """
    
    message: Optional[str] = None
    level: Optional[AlertLevel] = None
    status: Optional[AlertStatus] = None
    sent_at: Optional[datetime] = None
    delivery_method: Optional[AlertDeliveryMethod] = None
    recipient: Optional[str] = None


class AlertInDBBase(AlertBase):
    """Fields stored in DB."""
    
    id: int
    status: AlertStatus
    timestamp: datetime
    sent_at: Optional[datetime] = None
    alert_config_id: Optional[int] = None

    class Config:
        from_attributes = True


class Alert(AlertInDBBase):
    """Returned via API."""
    pass
