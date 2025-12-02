from datetime import UTC, datetime
from sqlalchemy import (
    JSON, Boolean, Column, DateTime, Float, ForeignKey, Integer, String,
    Text, UniqueConstraint, Enum as SQLEnum, Index
)
from sqlalchemy.orm import declarative_base, relationship

from app.models.enums import (
    ConnectorType,
    PipelineStatus,
    JobStatus,
    AlertLevel,
    AlertStatus,
    AlertType,
    AlertDeliveryMethod,
    OperatorType,
    PipelineRunStatus,
    OperatorRunStatus,
)

Base = declarative_base()


# =============================================================
# CONNECTIONS & METADATA CACHE
# =============================================================
class Connection(Base):
    """
    Stores source/destination connection definitions.
    Includes encrypted config, type, and test status.
    """

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
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC),
                        onupdate=lambda: datetime.now(UTC))

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
    """
    Caches schemas from connectors for exploration + ERD UI.
    """

    __tablename__ = "metadata_cache"

    id = Column(Integer, primary_key=True)
    connection_id = Column(
        Integer, ForeignKey("connections.id", ondelete="CASCADE"),
        nullable=False, index=True
    )

    schema_data = Column(JSON, nullable=False)
    table_count = Column(Integer, default=0)
    column_count = Column(Integer, default=0)

    last_scanned_at = Column(DateTime, default=lambda: datetime.now(UTC))
    scan_duration_seconds = Column(Float)

    connection = relationship("Connection", back_populates="metadata_cache")


# =============================================================
# PIPELINE DEFINITIONS
# =============================================================
class Pipeline(Base):
    """
    Defines an ETL/ELT pipeline, its schedule, connectors,
    and transformation config.
    """

    __tablename__ = "pipelines"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True, index=True)
    description = Column(Text)

    source_connection_id = Column(
        Integer,
        ForeignKey("connections.id"),
        nullable=False,
        index=True
    )
    destination_connection_id = Column(
        Integer,
        ForeignKey("connections.id"),
        nullable=False,
        index=True
    )

    source_config = Column(JSON, nullable=False)
    destination_config = Column(JSON, nullable=False)
    transform_config = Column(JSON)  # list of transformations

    schedule_cron = Column(String(100))
    schedule_enabled = Column(Boolean, default=False)

    status = Column(SQLEnum(PipelineStatus), default=PipelineStatus.DRAFT)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC),
                        onupdate=lambda: datetime.now(UTC))
    last_run_at = Column(DateTime(timezone=True))
    next_run_at = Column(DateTime(timezone=True))

    # Relationships
    source_connection = relationship(
        "Connection",
        foreign_keys=[source_connection_id],
        back_populates="source_pipelines"
    )
    destination_connection = relationship(
        "Connection",
        foreign_keys=[destination_connection_id],
        back_populates="destination_pipelines"
    )

    jobs = relationship("Job", back_populates="pipeline",
                        cascade="all, delete-orphan")
    state = relationship("PipelineState", back_populates="pipeline",
                         uselist=False, cascade="all, delete-orphan")
    alerts = relationship("Alert", back_populates="pipeline",
                          cascade="all, delete-orphan")
    versions = relationship("PipelineVersion", back_populates="pipeline",
                            cascade="all, delete-orphan")

    runs = relationship(
        "PipelineRun",
        back_populates="pipeline",
        cascade="all, delete-orphan"
    )


class PipelineVersion(Base):
    """
    Version history of pipeline definition/config snapshots.
    """

    __tablename__ = "pipeline_versions"

    id = Column(Integer, primary_key=True)
    pipeline_id = Column(
        Integer, ForeignKey("pipelines.id", ondelete="CASCADE"),
        nullable=False
    )

    version_number = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    created_by = Column(String(255))
    description = Column(Text)

    config_snapshot = Column(JSON, nullable=False)

    __table_args__ = (
        UniqueConstraint("pipeline_id", "version_number",
                         name="uq_pipeline_version_number"),
    )

    pipeline = relationship("Pipeline", back_populates="versions")


class PipelineState(Base):
    """
    Stores incremental sync state per pipeline.
    """

    __tablename__ = "pipeline_states"

    id = Column(Integer, primary_key=True)
    pipeline_id = Column(
        Integer, ForeignKey("pipelines.id", ondelete="CASCADE"),
        nullable=False, unique=True
    )

    state_data = Column(JSON, nullable=False, default=dict)
    last_record_count = Column(Integer, default=0)
    total_records_synced = Column(Integer, default=0)

    updated_at = Column(DateTime, default=lambda: datetime.now(UTC),
                        onupdate=lambda: datetime.now(UTC))

    pipeline = relationship("Pipeline", back_populates="state")


# =============================================================
# JOB SYSTEM (Celery Tasks & High-Level Execution)
# =============================================================
class Job(Base):
    """
    Represents a task execution triggered for a pipeline.
    Maps to a Celery task.
    """

    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True)
    pipeline_id = Column(
        Integer, ForeignKey("pipelines.id", ondelete="CASCADE"),
        nullable=False, index=True
    )

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
    logs = relationship("JobLog", back_populates="job",
                        cascade="all, delete-orphan")
    alerts = relationship("Alert", back_populates="job",
                          cascade="all, delete-orphan")

    runs = relationship(
        "PipelineRun",
        back_populates="job",
        cascade="all, delete-orphan"
    )


class JobLog(Base):
    """
    Log entries for a job (general, not operator-specific).
    """

    __tablename__ = "job_logs"

    id = Column(Integer, primary_key=True)
    job_id = Column(
        Integer, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )

    level = Column(String(20), nullable=False)
    message = Column(Text, nullable=False)
    timestamp = Column(DateTime, default=lambda: datetime.now(UTC))

    log_metadata = Column(JSON)

    job = relationship("Job", back_populates="logs")


# =============================================================
# PIPELINE RUN (Logical Execution)
# =============================================================
class PipelineRun(Base):
    """
    A full pipeline execution instance.
    Parent of operator runs.
    """

    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True)
    pipeline_id = Column(
        Integer, ForeignKey("pipelines.id", ondelete="CASCADE"),
        nullable=False, index=True
    )
    job_id = Column(Integer, ForeignKey("jobs.id", ondelete="SET NULL"))

    run_number = Column(Integer, nullable=False)

    status = Column(SQLEnum(PipelineRunStatus),
                    default=PipelineRunStatus.PENDING)

    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    duration_seconds = Column(Float)

    total_extracted = Column(Integer, default=0)
    total_transformed = Column(Integer, default=0)
    total_loaded = Column(Integer, default=0)

    error_message = Column(Text)
    error_traceback = Column(Text)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC))

    pipeline = relationship("Pipeline", back_populates="runs")
    job = relationship("Job", back_populates="runs")

    operator_runs = relationship(
        "OperatorRun",
        back_populates="pipeline_run",
        cascade="all, delete-orphan"
    )


# =============================================================
# OPERATOR RUN (Extract/Transform/Load)
# =============================================================
class OperatorRun(Base):
    """
    Execution of a specific operator inside a pipeline run.
    """

    __tablename__ = "operator_runs"

    id = Column(Integer, primary_key=True)
    pipeline_run_id = Column(
        Integer,
        ForeignKey("pipeline_runs.id", ondelete="CASCADE"),
        nullable=False, index=True
    )

    operator_type = Column(SQLEnum(OperatorType), nullable=False)
    operator_name = Column(String(255), nullable=False)

    status = Column(SQLEnum(OperatorRunStatus),
                    default=OperatorRunStatus.PENDING)

    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    duration_seconds = Column(Float)

    records_in = Column(Integer, default=0)
    records_out = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)

    error_message = Column(Text)
    error_traceback = Column(Text)

    pipeline_run = relationship("PipelineRun", back_populates="operator_runs")

    logs = relationship(
        "OperatorRunLog",
        back_populates="operator_run",
        cascade="all, delete-orphan"
    )


# =============================================================
# OPERATOR RUN LOGS
# =============================================================
class OperatorRunLog(Base):
    """
    Logs tied to a specific operator in a pipeline run.
    """

    __tablename__ = "operator_run_logs"

    id = Column(Integer, primary_key=True)
    operator_run_id = Column(
        Integer,
        ForeignKey("operator_runs.id", ondelete="CASCADE"),
        nullable=False
    )

    level = Column(String(20), nullable=False)
    message = Column(Text, nullable=False)
    timestamp = Column(DateTime, default=lambda: datetime.now(UTC))

    log_metadata = Column(JSON)

    operator_run = relationship("OperatorRun", back_populates="logs")


# =============================================================
# SYSTEM CONFIG
# =============================================================
class SystemConfig(Base):
    __tablename__ = "system_config"

    id = Column(Integer, primary_key=True)
    key = Column(String(255), nullable=False, unique=True)
    value = Column(Text, nullable=False)
    description = Column(Text)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC),
                        onupdate=lambda: datetime.now(UTC))


# =============================================================
# ALERTING SYSTEM
# =============================================================
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
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC),
                        onupdate=lambda: datetime.now(UTC))


class Alert(Base):
    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True)
    alert_config_id = Column(
        Integer, ForeignKey("alert_configs.id", ondelete="SET NULL")
    )

    message = Column(Text, nullable=False)
    level = Column(SQLEnum(AlertLevel), default=AlertLevel.INFO)

    job_id = Column(Integer, ForeignKey("jobs.id", ondelete="SET NULL"))
    pipeline_id = Column(
        Integer, ForeignKey("pipelines.id", ondelete="SET NULL")
    )

    status = Column(SQLEnum(AlertStatus), default=AlertStatus.PENDING)
    delivery_method = Column(SQLEnum(AlertDeliveryMethod), nullable=False)
    recipient = Column(String(255), nullable=False)

    timestamp = Column(DateTime, default=lambda: datetime.now(UTC))
    sent_at = Column(DateTime(timezone=True))

    alert_config = relationship("AlertConfig")
    job = relationship("Job", back_populates="alerts")
    pipeline = relationship("Pipeline", back_populates="alerts")


# =============================================================
# TRANSFORMER DEFINITIONS
# =============================================================
class Transformer(Base):
    """
    Stores custom transformer definitions.
    """

    __tablename__ = "transformers"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True, index=True)
    type = Column(String(100), nullable=False)  # e.g., "noop", "filter"
    config = Column(JSON, nullable=False, default=dict)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC),
                        onupdate=lambda: datetime.now(UTC))
