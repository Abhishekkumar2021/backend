from typing import Any, List, Optional
import os # Import os for environment variables in case needed for recipients or secure data

from sqlalchemy.orm import Session

from app.models.db_models import Alert, AlertConfig, AlertStatus, AlertType, AlertDeliveryMethod, AlertLevel
from app.schemas.alert import AlertCreate, AlertConfigCreate, AlertConfigUpdate, AlertUpdate


class AlertService:
    """Service for managing alert-related operations."""

    @staticmethod
    def create_alert_config(db: Session, alert_config_in: AlertConfigCreate) -> AlertConfig:
        """Create a new alert configuration."""
        db_obj = AlertConfig(**alert_config_in.model_dump())
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    @staticmethod
    def get_alert_config(db: Session, alert_config_id: int) -> Optional[AlertConfig]:
        """Get an alert configuration by ID."""
        return db.query(AlertConfig).filter(AlertConfig.id == alert_config_id).first()

    @staticmethod
    def get_alert_config_by_name(db: Session, name: str) -> Optional[AlertConfig]:
        """Get an alert configuration by name."""
        return db.query(AlertConfig).filter(AlertConfig.name == name).first()

    @staticmethod
    def get_alert_configs(
        db: Session, skip: int = 0, limit: int = 100, alert_type: Optional[AlertType] = None
    ) -> List[AlertConfig]:
        """Get a list of alert configurations."""
        query = db.query(AlertConfig)
        if alert_type:
            query = query.filter(AlertConfig.alert_type == alert_type)
        return query.offset(skip).limit(limit).all()

    @staticmethod
    def update_alert_config(
        db: Session, alert_config_id: int, alert_config_in: AlertConfigUpdate
    ) -> Optional[AlertConfig]:
        """Update an existing alert configuration."""
        db_obj = db.query(AlertConfig).filter(AlertConfig.id == alert_config_id).first()
        if db_obj:
            update_data = alert_config_in.model_dump(exclude_unset=True)
            for key, value in update_data.items():
                setattr(db_obj, key, value)
            db.add(db_obj)
            db.commit()
            db.refresh(db_obj)
        return db_obj

    @staticmethod
    def delete_alert_config(db: Session, alert_config_id: int) -> Optional[AlertConfig]:
        """Delete an alert configuration."""
        db_obj = db.query(AlertConfig).filter(AlertConfig.id == alert_config_id).first()
        if db_obj:
            db.delete(db_obj)
            db.commit()
        return db_obj

    @staticmethod
    def create_alert(db: Session, alert_in: AlertCreate) -> Alert:
        """Create a new alert instance."""
        db_obj = Alert(**alert_in.model_dump())
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    @staticmethod
    def get_alert(db: Session, alert_id: int) -> Optional[Alert]:
        """Get an alert instance by ID."""
        return db.query(Alert).filter(Alert.id == alert_id).first()

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
        """Get a list of alert instances."""
        query = db.query(Alert)
        if status:
            query = query.filter(Alert.status == status)
        if level:
            query = query.filter(Alert.level == level)
        if pipeline_id:
            query = query.filter(Alert.pipeline_id == pipeline_id)
        if job_id:
            query = query.filter(Alert.job_id == job_id)
        return query.order_by(Alert.timestamp.desc()).offset(skip).limit(limit).all()

    @staticmethod
    def update_alert(db: Session, alert_id: int, alert_in: AlertUpdate) -> Optional[Alert]:
        """Update an existing alert instance."""
        db_obj = db.query(Alert).filter(Alert.id == alert_id).first()
        if db_obj:
            update_data = alert_in.model_dump(exclude_unset=True)
            for key, value in update_data.items():
                setattr(db_obj, key, value)
            db.add(db_obj)
            db.commit()
            db.refresh(db_obj)
        return db_obj

    @staticmethod
    def delete_alert(db: Session, alert_id: int) -> Optional[Alert]:
        """Delete an alert instance."""
        db_obj = db.query(Alert).filter(Alert.id == alert_id).first()
        if db_obj:
            db.delete(db_obj)
            db.commit()
        return db_obj

    @staticmethod
    def trigger_job_failure_alert(db: Session, job_id: int, pipeline_id: int, error_message: str):
        """
        Triggers alerts for job failures based on configured alert_configs.
        """
        alert_configs = db.query(AlertConfig).filter(
            AlertConfig.alert_type == AlertType.JOB_FAILURE,
            AlertConfig.enabled == True
        ).all()

        for config in alert_configs:
            alert_message = f"Job {job_id} for Pipeline {pipeline_id} failed: {error_message}"
            alert_in = AlertCreate(
                message=alert_message,
                level=AlertLevel.ERROR,
                job_id=job_id,
                pipeline_id=pipeline_id,
                delivery_method=config.delivery_method,
                recipient=config.recipient,
                alert_config_id=config.id,
            )
            AlertService.create_alert(db, alert_in)
            print(f"Alert triggered for job {job_id} to {config.recipient} via {config.delivery_method}: {alert_message}") # Placeholder for actual dispatch

    @staticmethod
    def trigger_generic_alert(db: Session, job_id: int | None, error_message: str):
        """
        Triggers a generic alert for unexpected system or worker errors
        that might not be tied to a specific pipeline alert configuration.
        """
        # For generic alerts, we might have a default recipient or look for specific configs.
        # For now, we'll create a basic alert without linking to AlertConfig.
        alert_message = f"Unexpected system error during job processing (Job ID: {job_id if job_id else 'N/A'}): {error_message}"
        
        # This part assumes there's a default way to send generic alerts, e.g., to an admin email
        # or a default webhook. For simplicity, we'll use a placeholder recipient/method.
        default_recipient = os.getenv("DEFAULT_ALERT_RECIPIENT", "admin@example.com")
        default_delivery_method = os.getenv("DEFAULT_ALERT_DELIVERY_METHOD", AlertDeliveryMethod.EMAIL.value)

        alert_in = AlertCreate(
            message=alert_message,
            level=AlertLevel.CRITICAL,
            job_id=job_id,
            pipeline_id=None, # No specific pipeline for generic errors
            delivery_method=AlertDeliveryMethod(default_delivery_method),
            recipient=default_recipient,
            alert_config_id=None,
        )
        AlertService.create_alert(db, alert_in)
        print(f"Generic alert triggered: {alert_message}") # Placeholder for actual dispatch
