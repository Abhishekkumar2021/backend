from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.alert import (
    Alert, AlertConfig, AlertConfigCreate, AlertConfigUpdate,
    AlertCreate, AlertUpdate
)
from app.services.alert import AlertService
from app.models.database import AlertType, AlertStatus, AlertLevel
from app.core.logging import get_logger

router = APIRouter()
logger = get_logger(__name__)


# ===================================================================
# Alert Configuration Endpoints
# ===================================================================

@router.post(
    "/configs/",
    response_model=AlertConfig,
    status_code=status.HTTP_201_CREATED,
)
def create_alert_config(
    *,
    db: Session = Depends(get_db),
    alert_config_in: AlertConfigCreate,
) -> Any:
    """Create a new alert configuration."""
    logger.info(
        "alert_config_creation_requested",
        name=alert_config_in.name,
        alert_type=alert_config_in.alert_type,
    )

    existing_config = AlertService.get_alert_config_by_name(
        db, name=alert_config_in.name
    )

    if existing_config:
        logger.warning(
            "alert_config_name_conflict",
            name=alert_config_in.name,
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="An alert configuration with this name already exists.",
        )

    alert_config = AlertService.create_alert_config(
        db, alert_config_in=alert_config_in
    )

    logger.info(
        "alert_config_created",
        config_id=alert_config.id,
        name=alert_config.name,
    )

    return alert_config


@router.get("/configs/", response_model=List[AlertConfig])
def list_alert_configs(
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100,
    alert_type: AlertType | None = None,
) -> Any:
    """Retrieve a list of alert configurations."""
    logger.info(
        "alert_config_list_requested",
        skip=skip,
        limit=limit,
        alert_type=str(alert_type) if alert_type else None,
    )

    alert_configs = AlertService.get_alert_configs(
        db, skip=skip, limit=limit, alert_type=alert_type
    )

    logger.info(
        "alert_configs_listed",
        count=len(alert_configs),
    )

    return alert_configs


@router.get("/configs/{config_id}", response_model=AlertConfig)
def get_alert_config(
    *,
    db: Session = Depends(get_db),
    config_id: int,
) -> Any:
    """Get a specific alert configuration by ID."""
    logger.info(
        "alert_config_fetch_requested",
        config_id=config_id,
    )

    alert_config = AlertService.get_alert_config(
        db, alert_config_id=config_id
    )

    if not alert_config:
        logger.warning(
            "alert_config_not_found",
            config_id=config_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert configuration not found",
        )

    logger.debug(
        "alert_config_retrieved",
        config_id=config_id,
    )

    return alert_config


@router.put("/configs/{config_id}", response_model=AlertConfig)
def update_alert_config(
    *,
    db: Session = Depends(get_db),
    config_id: int,
    alert_config_in: AlertConfigUpdate,
) -> Any:
    """Update an alert configuration."""
    logger.info(
        "alert_config_update_requested",
        config_id=config_id,
    )

    alert_config = AlertService.get_alert_config(
        db, alert_config_id=config_id
    )
    if not alert_config:
        logger.warning(
            "alert_config_not_found",
            config_id=config_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert configuration not found",
        )

    if alert_config_in.name:
        existing_config = AlertService.get_alert_config_by_name(
            db, name=alert_config_in.name
        )
        if existing_config and existing_config.id != config_id:
            logger.warning(
                "alert_config_name_conflict",
                name=alert_config_in.name,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="An alert configuration with this name already exists.",
            )

    updated_config = AlertService.update_alert_config(
        db, alert_config_id=config_id, alert_config_in=alert_config_in
    )

    logger.info(
        "alert_config_updated",
        config_id=config_id,
    )

    return updated_config


@router.delete("/configs/{config_id}", response_model=AlertConfig)
def delete_alert_config(
    *,
    db: Session = Depends(get_db),
    config_id: int,
) -> Any:
    """Delete an alert configuration."""
    logger.info(
        "alert_config_deletion_requested",
        config_id=config_id,
    )

    alert_config = AlertService.get_alert_config(
        db, alert_config_id=config_id
    )

    if not alert_config:
        logger.warning(
            "alert_config_not_found",
            config_id=config_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert configuration not found",
        )

    deleted = AlertService.delete_alert_config(
        db, alert_config_id=config_id
    )

    logger.info(
        "alert_config_deleted",
        config_id=config_id,
    )

    return deleted


# ===================================================================
# Alert Instance Endpoints
# ===================================================================

@router.post(
    "/",
    response_model=Alert,
    status_code=status.HTTP_201_CREATED,
)
def create_alert(
    *,
    db: Session = Depends(get_db),
    alert_in: AlertCreate,
) -> Any:
    """Create a new alert instance."""
    logger.info(
        "alert_creation_requested",
        level=alert_in.level,
        status=alert_in.status,
        pipeline_id=alert_in.pipeline_id,
        job_id=alert_in.job_id,
    )

    alert = AlertService.create_alert(db, alert_in=alert_in)

    logger.info(
        "alert_created",
        alert_id=alert.id,
        level=alert.level,
        status=alert.status,
    )

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
    logger.info(
        "alerts_list_requested",
        skip=skip,
        limit=limit,
        status=str(status) if status else None,
        level=str(level) if level else None,
        pipeline_id=pipeline_id,
        job_id=job_id,
    )

    alerts = AlertService.get_alerts(
        db,
        skip=skip,
        limit=limit,
        status=status,
        level=level,
        pipeline_id=pipeline_id,
        job_id=job_id,
    )

    logger.info(
        "alerts_listed",
        count=len(alerts),
    )

    return alerts


@router.get("/{alert_id}", response_model=Alert)
def get_alert(
    *,
    db: Session = Depends(get_db),
    alert_id: int,
) -> Any:
    """Get a specific alert instance by ID."""
    logger.info(
        "alert_fetch_requested",
        alert_id=alert_id,
    )

    alert = AlertService.get_alert(db, alert_id=alert_id)

    if not alert:
        logger.warning(
            "alert_not_found",
            alert_id=alert_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert instance not found",
        )

    logger.debug(
        "alert_retrieved",
        alert_id=alert_id,
    )

    return alert


@router.put("/{alert_id}", response_model=Alert)
def update_alert(
    *,
    db: Session = Depends(get_db),
    alert_id: int,
    alert_in: AlertUpdate,
) -> Any:
    """Update an alert instance."""
    logger.info(
        "alert_update_requested",
        alert_id=alert_id,
    )

    alert = AlertService.get_alert(db, alert_id=alert_id)
    if not alert:
        logger.warning(
            "alert_not_found",
            alert_id=alert_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert instance not found",
        )

    updated = AlertService.update_alert(
        db, alert_id=alert_id, alert_in=alert_in
    )

    logger.info(
        "alert_updated",
        alert_id=alert_id,
    )

    return updated


@router.delete("/{alert_id}", response_model=Alert)
def delete_alert(
    *,
    db: Session = Depends(get_db),
    alert_id: int,
) -> Any:
    """Delete an alert instance."""
    logger.info(
        "alert_deletion_requested",
        alert_id=alert_id,
    )

    alert = AlertService.get_alert(db, alert_id=alert_id)
    if not alert:
        logger.warning(
            "alert_not_found",
            alert_id=alert_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert instance not found",
        )

    deleted = AlertService.delete_alert(
        db, alert_id=alert_id
    )

    logger.info(
        "alert_deleted",
        alert_id=alert_id,
    )

    return deleted
