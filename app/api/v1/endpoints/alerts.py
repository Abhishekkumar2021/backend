from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.alert import Alert, AlertConfig, AlertConfigCreate, AlertConfigUpdate, AlertCreate, AlertUpdate
from app.services.alert_service import AlertService
from app.models.db_models import AlertType, AlertStatus, AlertLevel

router = APIRouter()


# --- Alert Configuration Endpoints ---
@router.post("/configs/", response_model=AlertConfig, status_code=status.HTTP_201_CREATED)
def create_alert_config(
    *,
    db: Session = Depends(get_db),
    alert_config_in: AlertConfigCreate,
) -> Any:
    """Create a new alert configuration."""
    existing_config = AlertService.get_alert_config_by_name(db, name=alert_config_in.name)
    if existing_config:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="An alert configuration with this name already exists.",
        )
    alert_config = AlertService.create_alert_config(db, alert_config_in=alert_config_in)
    return alert_config


@router.get("/configs/", response_model=List[AlertConfig])
def list_alert_configs(
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100,
    alert_type: AlertType | None = None,
) -> Any:
    """Retrieve a list of alert configurations."""
    alert_configs = AlertService.get_alert_configs(db, skip=skip, limit=limit, alert_type=alert_type)
    return alert_configs


@router.get("/configs/{config_id}", response_model=AlertConfig)
def get_alert_config(
    *,
    db: Session = Depends(get_db),
    config_id: int,
) -> Any:
    """Get a specific alert configuration by ID."""
    alert_config = AlertService.get_alert_config(db, alert_config_id=config_id)
    if not alert_config:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert configuration not found")
    return alert_config


@router.put("/configs/{config_id}", response_model=AlertConfig)
def update_alert_config(
    *,
    db: Session = Depends(get_db),
    config_id: int,
    alert_config_in: AlertConfigUpdate,
) -> Any:
    """Update an alert configuration."""
    alert_config = AlertService.get_alert_config(db, alert_config_id=config_id)
    if not alert_config:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert configuration not found")

    if alert_config_in.name:
        existing_config = AlertService.get_alert_config_by_name(db, name=alert_config_in.name)
        if existing_config and existing_config.id != config_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="An alert configuration with this name already exists.",
            )

    alert_config = AlertService.update_alert_config(db, alert_config_id=config_id, alert_config_in=alert_config_in)
    return alert_config


@router.delete("/configs/{config_id}", response_model=AlertConfig)
def delete_alert_config(
    *,
    db: Session = Depends(get_db),
    config_id: int,
) -> Any:
    """Delete an alert configuration."""
    alert_config = AlertService.get_alert_config(db, alert_config_id=config_id)
    if not alert_config:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert configuration not found")
    alert_config = AlertService.delete_alert_config(db, alert_config_id=config_id)
    return alert_config


# --- Alert Instance Endpoints ---
@router.post("/", response_model=Alert, status_code=status.HTTP_201_CREATED)
def create_alert(
    *,
    db: Session = Depends(get_db),
    alert_in: AlertCreate,
) -> Any:
    """Create a new alert instance (e.g., triggered by a system event)."""
    alert = AlertService.create_alert(db, alert_in=alert_in)
    return alert


@router.get("/", response_model=List[Alert])
def list_alerts(
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100,
    status: AlertStatus | None = None,
    level: AlertLevel | None = None,
    pipeline_id: int | None = None,
    job_id: int | None = None,
) -> Any:
    """Retrieve a list of alert instances."""
    alerts = AlertService.get_alerts(db, skip=skip, limit=limit, status=status, level=level, pipeline_id=pipeline_id, job_id=job_id)
    return alerts


@router.get("/{alert_id}", response_model=Alert)
def get_alert(
    *,
    db: Session = Depends(get_db),
    alert_id: int,
) -> Any:
    """Get a specific alert instance by ID."""
    alert = AlertService.get_alert(db, alert_id=alert_id)
    if not alert:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert instance not found")
    return alert


@router.put("/{alert_id}", response_model=Alert)
def update_alert(
    *,
    db: Session = Depends(get_db),
    alert_id: int,
    alert_in: AlertUpdate,
) -> Any:
    """Update an alert instance."""
    alert = AlertService.get_alert(db, alert_id=alert_id)
    if not alert:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert instance not found")
    alert = AlertService.update_alert(db, alert_id=alert_id, alert_in=alert_in)
    return alert


@router.delete("/{alert_id}", response_model=Alert)
def delete_alert(
    *,
    db: Session = Depends(get_db),
    alert_id: int,
) -> Any:
    """Delete an alert instance."""
    alert = AlertService.get_alert(db, alert_id=alert_id)
    if not alert:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert instance not found")
    alert = AlertService.delete_alert(db, alert_id=alert_id)
    return alert
