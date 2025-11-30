from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel

from app.models.db_models import AlertDeliveryMethod, AlertLevel, AlertStatus, AlertType


# Shared properties
class AlertConfigBase(BaseModel):
    name: str
    description: Optional[str] = None
    alert_type: AlertType
    delivery_method: AlertDeliveryMethod
    recipient: str
    threshold_value: Optional[int] = 1
    threshold_unit: Optional[str] = None
    enabled: bool = True


# Properties to receive via API on creation
class AlertConfigCreate(AlertConfigBase):
    pass


# Properties to receive via API on update
class AlertConfigUpdate(AlertConfigBase):
    name: Optional[str] = None
    alert_type: Optional[AlertType] = None
    delivery_method: Optional[AlertDeliveryMethod] = None
    recipient: Optional[str] = None


# Properties shared by models stored in DB
class AlertConfigInDBBase(AlertConfigBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# Properties to return via API
class AlertConfig(AlertConfigInDBBase):
    pass


# Shared properties for Alert instances
class AlertBase(BaseModel):
    message: str
    level: Optional[AlertLevel] = AlertLevel.INFO
    job_id: Optional[int] = None
    pipeline_id: Optional[int] = None
    delivery_method: AlertDeliveryMethod
    recipient: str


# Properties to receive via API on creation
class AlertCreate(AlertBase):
    status: Optional[AlertStatus] = AlertStatus.PENDING
    alert_config_id: Optional[int] = None


# Properties to receive via API on update
class AlertUpdate(AlertBase):
    status: Optional[AlertStatus] = None
    sent_at: Optional[datetime] = None


# Properties shared by models stored in DB
class AlertInDBBase(AlertBase):
    id: int
    status: AlertStatus
    timestamp: datetime
    sent_at: Optional[datetime] = None
    alert_config_id: Optional[int] = None

    class Config:
        from_attributes = True


# Properties to return via API
class Alert(AlertInDBBase):
    pass
