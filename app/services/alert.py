from typing import Any, List, Optional
import os

from sqlalchemy.orm import Session

from app.models.database import (
    Alert,
    AlertConfig,
    AlertStatus,
    AlertType,
    AlertDeliveryMethod,
    AlertLevel,
)
from app.schemas.alert import (
    AlertCreate,
    AlertConfigCreate,
    AlertConfigUpdate,
    AlertUpdate,
)
from app.core.logging import get_logger

logger = get_logger(__name__)


class AlertService:
    """
    High-level Alert Manager.
    Handles both:
    • AlertConfig CRUD (templates)
    • Alert instance creation (events)
    • Business logic triggers (job failure, system errors)
    """

    # ===================================================================
    # Helper
    # ===================================================================

    @staticmethod
    def _apply_updates(db_obj, update_data: dict):
        """Safely apply partial updates to SQLAlchemy model."""
        for key, value in update_data.items():
            setattr(db_obj, key, value)
            logger.debug("alert_field_updated", field=key)

    # ===================================================================
    # Alert Config CRUD
    # ===================================================================

    @staticmethod
    def create_alert_config(db: Session, alert_config_in: AlertConfigCreate) -> AlertConfig:
        logger.info("alert_config_create_requested", name=alert_config_in.name)

        db_obj = AlertConfig(**alert_config_in.model_dump())
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)

        logger.info("alert_config_created", config_id=db_obj.id)
        return db_obj

    @staticmethod
    def get_alert_config(db: Session, alert_config_id: int) -> Optional[AlertConfig]:
        cfg = db.query(AlertConfig).filter_by(id=alert_config_id).first()
        if not cfg:
            logger.warning("alert_config_not_found", config_id=alert_config_id)
        return cfg

    @staticmethod
    def get_alert_config_by_name(db: Session, name: str) -> Optional[AlertConfig]:
        return db.query(AlertConfig).filter_by(name=name).first()

    @staticmethod
    def get_alert_configs(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        alert_type: Optional[AlertType] = None,
    ) -> List[AlertConfig]:

        logger.info(
            "alert_config_list_requested",
            skip=skip,
            limit=limit,
            alert_type=str(alert_type) if alert_type else None,
        )

        query = db.query(AlertConfig)
        if alert_type:
            query = query.filter(AlertConfig.alert_type == alert_type)

        configs = query.offset(skip).limit(limit).all()

        logger.info("alert_config_list_completed", count=len(configs))
        return configs

    @staticmethod
    def update_alert_config(
        db: Session, alert_config_id: int, alert_config_in: AlertConfigUpdate
    ) -> Optional[AlertConfig]:

        logger.info("alert_config_update_requested", alert_config_id=alert_config_id)

        db_obj = db.query(AlertConfig).filter_by(id=alert_config_id).first()
        if not db_obj:
            logger.warning("alert_config_update_not_found", alert_config_id=alert_config_id)
            return None

        update_data = alert_config_in.model_dump(exclude_unset=True)
        AlertService._apply_updates(db_obj, update_data)

        db.commit()
        db.refresh(db_obj)

        logger.info("alert_config_updated", alert_config_id=db_obj.id)
        return db_obj

    @staticmethod
    def delete_alert_config(db: Session, alert_config_id: int) -> bool:
        logger.info("alert_config_delete_requested", alert_config_id=alert_config_id)

        db_obj = db.query(AlertConfig).filter_by(id=alert_config_id).first()
        if not db_obj:
            logger.warning("alert_config_delete_not_found", alert_config_id=alert_config_id)
            return False

        db.delete(db_obj)
        db.commit()

        logger.info("alert_config_deleted", alert_config_id=alert_config_id)
        return True

    # ===================================================================
    # Alert Instance CRUD
    # ===================================================================

    @staticmethod
    def create_alert(db: Session, alert_in: AlertCreate) -> Alert:
        logger.info(
            "alert_create_requested",
            level=alert_in.level,
            pipeline_id=alert_in.pipeline_id,
            job_id=alert_in.job_id,
        )

        db_obj = Alert(**alert_in.model_dump())
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)

        logger.info("alert_created", alert_id=db_obj.id)
        return db_obj

    @staticmethod
    def get_alert(db: Session, alert_id: int) -> Optional[Alert]:
        alert = db.query(Alert).filter_by(id=alert_id).first()
        if not alert:
            logger.warning("alert_not_found", alert_id=alert_id)
        return alert

    @staticmethod
    def get_alerts(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        status: Optional[AlertStatus] = None,
        level: Optional[AlertLevel] = None,
        pipeline_id: Optional[int] = None,
        job_id: Optional[int] = None,
    ) -> List[Alert]:

        logger.info(
            "alert_list_requested",
            skip=skip,
            limit=limit,
            status=status.value if status else None,
            level=level.value if level else None,
            pipeline_id=pipeline_id,
            job_id=job_id,
        )

        query = db.query(Alert)

        if status:
            query = query.filter(Alert.status == status)
        if level:
            query = query.filter(Alert.level == level)
        if pipeline_id:
            query = query.filter(Alert.pipeline_id == pipeline_id)
        if job_id:
            query = query.filter(Alert.job_id == job_id)

        alerts = query.order_by(Alert.timestamp.desc()).offset(skip).limit(limit).all()

        logger.info("alert_list_completed", count=len(alerts))
        return alerts

    @staticmethod
    def update_alert(db: Session, alert_id: int, alert_in: AlertUpdate) -> Optional[Alert]:
        logger.info("alert_update_requested", alert_id=alert_id)

        db_obj = db.query(Alert).filter_by(id=alert_id).first()
        if not db_obj:
            logger.warning("alert_update_not_found", alert_id=alert_id)
            return None

        update_data = alert_in.model_dump(exclude_unset=True)
        AlertService._apply_updates(db_obj, update_data)

        db.commit()
        db.refresh(db_obj)

        logger.info("alert_updated", alert_id=alert_id)
        return db_obj

    @staticmethod
    def delete_alert(db: Session, alert_id: int) -> bool:
        logger.info("alert_delete_requested", alert_id=alert_id)

        db_obj = db.query(Alert).filter_by(id=alert_id).first()
        if not db_obj:
            logger.warning("alert_delete_not_found", alert_id=alert_id)
            return False

        db.delete(db_obj)
        db.commit()

        logger.info("alert_deleted", alert_id=alert_id)
        return True

    # ===================================================================
    # Business Logic: Job Failure Alerts
    # ===================================================================

    @staticmethod
    def trigger_job_failure_alert(db: Session, job_id: int, pipeline_id: int, error_message: str):
        logger.info(
            "job_failure_alert_trigger_requested",
            job_id=job_id,
            pipeline_id=pipeline_id,
        )

        configs = (
            db.query(AlertConfig)
            .filter(AlertConfig.alert_type == AlertType.JOB_FAILURE, AlertConfig.enabled == True)
            .all()
        )

        logger.info("job_failure_alert_configs_loaded", count=len(configs))

        for cfg in configs:
            alert_in = AlertCreate(
                message=f"Job {job_id} for Pipeline {pipeline_id} failed: {error_message}",
                level=AlertLevel.ERROR,
                job_id=job_id,
                pipeline_id=pipeline_id,
                delivery_method=cfg.delivery_method,
                recipient=cfg.recipient,
                alert_config_id=cfg.id,
            )

            AlertService.create_alert(db, alert_in)

            logger.info(
                "job_failure_alert_triggered",
                job_id=job_id,
                pipeline_id=pipeline_id,
                delivery_method=cfg.delivery_method,
                recipient=cfg.recipient,
            )

    # ===================================================================
    # Business Logic: Generic System Alerts
    # ===================================================================

    @staticmethod
    def trigger_generic_alert(db: Session, job_id: Optional[int], error_message: str):
        logger.info("generic_alert_trigger_requested", job_id=job_id)

        default_recipient = os.getenv("DEFAULT_ALERT_RECIPIENT", "admin@example.com")
        default_method = os.getenv(
            "DEFAULT_ALERT_DELIVERY_METHOD", AlertDeliveryMethod.EMAIL.value
        )

        alert_in = AlertCreate(
            message=f"Unexpected system error for Job {job_id}: {error_message}",
            level=AlertLevel.CRITICAL,
            job_id=job_id,
            pipeline_id=None,
            delivery_method=AlertDeliveryMethod(default_method),
            recipient=default_recipient,
            alert_config_id=None,
        )

        AlertService.create_alert(db, alert_in)

        logger.info(
            "generic_alert_triggered",
            job_id=job_id,
            recipient=default_recipient,
            method=default_method,
        )
