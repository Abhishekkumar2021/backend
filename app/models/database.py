from datetime import UTC, datetime
from sqlalchemy import (
    JSON, Boolean, Column, DateTime, Float, ForeignKey, Integer, String,
    Text, UniqueConstraint, Enum as SQLEnum, Index
)
from sqlalchemy.orm import declarative_base, relationship
from app.models.enums import *

Base = declarative_base()

class Connection(Base):
    """Stores source/destination connection definitions."""

    __tablename__ = "connections"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True, index=True)
    connector_type = Column(SQLEnum(ConnectorType), nullable=False)

    config_encrypted = Column(Text, nullable=False)

    description = Column(Text)
    is_source = Column(Boolean, default=True)

    last_test_at = Column(DateTime(timezone=True))
    last_test_success = Column(Boolean)
    last_test_error = Column(Text)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))

    # Relationships
    source_pipelines = relationship(
        "Pipeline",
        foreign_keys="Pipeline.source_connection_id",
        back_populates="source_connection",
        cascade="all, delete"
    )
    destination_pipelines = relationship(
        "Pipeline",
        foreign_keys="Pipeline.destination_connection_id",
        back_populates="destination_connection",
        cascade="all, delete"
    )
    metadata_cache = relationship(
        "MetadataCache",
        back_populates="connection",
        cascade="all, delete-orphan"
    )


class MetadataCache(Base):
    """Caches schemas for fast metadata browsing / ERD rendering."""

    __tablename__ = "metadata_cache"

    id = Column(Integer, primary_key=True)
    connection_id = Column(
        Integer, ForeignKey("connections.id", ondelete="CASCADE"), nullable=False, index=True
    )

    schema_data = Column(JSON, nullable=False)
    table_count = Column(Integer, default=0)
    column_count = Column(Integer, default=0)

    last_scanned_at = Column(DateTime, default=lambda: datetime.now(UTC))
    scan_duration_seconds = Column(Float)

    connection = relationship("Connection", back_populates="metadata_cache")


class Pipeline(Base):
    """Defines an end-to-end ETL/ELT pipeline."""

    __tablename__ = "pipelines"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True, index=True)
    description = Column(Text)

    source_connection_id = Column(Integer, ForeignKey("connections.id"), nullable=False, index=True)
    destination_connection_id = Column(Integer, ForeignKey("connections.id"), nullable=False, index=True)

    source_config = Column(JSON, nullable=False)
    destination_config = Column(JSON, nullable=False)
    transform_config = Column(JSON)

    schedule_cron = Column(String(100))
    schedule_enabled = Column(Boolean, default=False)

    sync_mode = Column(String(50), default="full_refresh")
    status = Column(SQLEnum(PipelineStatus), default=PipelineStatus.DRAFT)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))
    last_run_at = Column(DateTime(timezone=True))
    next_run_at = Column(DateTime(timezone=True))

    # Relationships
    source_connection = relationship("Connection", foreign_keys=[source_connection_id], back_populates="source_pipelines")
    destination_connection = relationship("Connection", foreign_keys=[destination_connection_id], back_populates="destination_pipelines")

    jobs = relationship("Job", back_populates="pipeline", cascade="all, delete-orphan")
    state = relationship("PipelineState", back_populates="pipeline", uselist=False, cascade="all, delete-orphan")
    alerts = relationship("Alert", back_populates="pipeline", cascade="all, delete-orphan")
    versions = relationship("PipelineVersion", back_populates="pipeline", cascade="all, delete-orphan")


class PipelineVersion(Base):
    __tablename__ = "pipeline_versions"

    id = Column(Integer, primary_key=True)
    pipeline_id = Column(Integer, ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)

    version_number = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    created_by = Column(String(255))
    description = Column(Text)

    config_snapshot = Column(JSON, nullable=False)

    __table_args__ = (
        UniqueConstraint("pipeline_id", "version_number", name="uq_pipeline_version_number"),
    )

    pipeline = relationship("Pipeline", back_populates="versions")


class PipelineState(Base):
    """Stores incremental state for each pipeline."""

    __tablename__ = "pipeline_states"

    id = Column(Integer, primary_key=True)
    pipeline_id = Column(
        Integer, ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False, unique=True
    )

    state_data = Column(JSON, nullable=False, default=dict)  # FIX: no shared dict

    last_record_count = Column(Integer, default=0)
    total_records_synced = Column(Integer, default=0)

    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))

    pipeline = relationship("Pipeline", back_populates="state")


class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True)
    pipeline_id = Column(Integer, ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False, index=True)

    status = Column(SQLEnum(JobStatus), default=JobStatus.PENDING)
    celery_task_id = Column(String(255), unique=True, index=True)

    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    duration_seconds = Column(Float)

    records_extracted = Column(Integer, default=0)
    records_loaded = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)

    error_message = Column(Text)
    error_traceback = Column(Text)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC))

    pipeline = relationship("Pipeline", back_populates="jobs")
    logs = relationship("JobLog", back_populates="job", cascade="all, delete-orphan")
    alerts = relationship("Alert", back_populates="job", cascade="all, delete-orphan")


class JobLog(Base):
    __tablename__ = "job_logs"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)

    level = Column(String(20), nullable=False)
    message = Column(Text, nullable=False)
    timestamp = Column(DateTime, default=lambda: datetime.now(UTC))

    log_metadata = Column(JSON)

    job = relationship("Job", back_populates="logs")


class SystemConfig(Base):
    __tablename__ = "system_config"

    id = Column(Integer, primary_key=True)
    key = Column(String(255), nullable=False, unique=True)
    value = Column(Text, nullable=False)
    description = Column(Text)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))


class AlertConfig(Base):
    __tablename__ = "alert_configs"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)
    description = Column(Text)

    alert_type = Column(SQLEnum(AlertType), nullable=False)
    delivery_method = Column(SQLEnum(AlertDeliveryMethod), nullable=False)
    recipient = Column(String(255), nullable=False)

    threshold_value = Column(Integer, default=1)
    threshold_unit = Column(String(50))

    enabled = Column(Boolean, default=True)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))


class Alert(Base):
    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True)
    alert_config_id = Column(Integer, ForeignKey("alert_configs.id", ondelete="SET NULL"))

    message = Column(Text, nullable=False)
    level = Column(SQLEnum(AlertLevel), default=AlertLevel.INFO)

    job_id = Column(Integer, ForeignKey("jobs.id", ondelete="SET NULL"))
    pipeline_id = Column(Integer, ForeignKey("pipelines.id", ondelete="SET NULL"))

    status = Column(SQLEnum(AlertStatus), default=AlertStatus.PENDING)
    delivery_method = Column(SQLEnum(AlertDeliveryMethod), nullable=False)
    recipient = Column(String(255), nullable=False)

    timestamp = Column(DateTime, default=lambda: datetime.now(UTC))
    sent_at = Column(DateTime(timezone=True))

    alert_config = relationship("AlertConfig")
    job = relationship("Job", back_populates="alerts")
    pipeline = relationship("Pipeline", back_populates="alerts")
