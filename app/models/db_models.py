import enum
from datetime import UTC, datetime

from sqlalchemy import JSON, Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class ConnectorType(str, enum.Enum):
    """Supported connector types"""

    # Databases
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    MSSQL = "mssql"
    ORACLE = "oracle"
    MONGODB = "mongodb"
    REDIS = "redis"

    # Cloud Data Warehouses
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    REDSHIFT = "redshift"

    # File Systems
    LOCAL_FILE = "local_file"
    S3 = "s3"
    GCS = "gcs"
    AZURE_BLOB = "azure_blob"

    # APIs
    REST_API = "rest_api"
    GRAPHQL = "graphql"

    # SaaS
    GOOGLE_SHEETS = "google_sheets"
    AIRTABLE = "airtable"


class PipelineStatus(str, enum.Enum):
    """Pipeline execution states"""

    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    ARCHIVED = "archived"


class JobStatus(str, enum.Enum):
    """Job execution states"""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AlertLevel(str, enum.Enum):
    """Severity level of an alert"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertStatus(str, enum.Enum):
    """Current status of an alert instance"""
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"


class AlertType(str, enum.Enum):
    """Type of event that triggers an alert"""
    JOB_FAILURE = "job_failure"
    PIPELINE_STATUS_CHANGE = "pipeline_status_change"
    CUSTOM_METRIC_THRESHOLD = "custom_metric_threshold"


class AlertDeliveryMethod(str, enum.Enum):
    """Method used to deliver an alert"""
    EMAIL = "email"
    WEBHOOK = "webhook"
    SLACK = "slack"


class Connection(Base):
    """Stores connection details for sources and destinations
    Credentials are encrypted before storage
    """

    __tablename__ = "connections"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, unique=True)
    connector_type = Column(SQLEnum(ConnectorType), nullable=False)

    # Encrypted configuration (host, port, credentials, etc.)
    config_encrypted = Column(Text, nullable=False)

    # Metadata
    description = Column(Text)
    is_source = Column(Boolean, default=True)  # True=Source, False=Destination

    # Connection health
    last_test_at = Column(DateTime)
    last_test_success = Column(Boolean)
    last_test_error = Column(Text)

    # Timestamps - FIXED: Use lambda to avoid single timestamp
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    updated_at = Column(
        DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )

    # Relationships
    source_pipelines = relationship(
        "Pipeline", foreign_keys="Pipeline.source_connection_id", back_populates="source_connection"
    )
    destination_pipelines = relationship(
        "Pipeline", foreign_keys="Pipeline.destination_connection_id", back_populates="destination_connection"
    )
    metadata_cache = relationship("MetadataCache", back_populates="connection", cascade="all, delete-orphan")


class MetadataCache(Base):
    """Caches discovered schemas from connections
    Stores tables, columns, relationships for fast ERD rendering
    """

    __tablename__ = "metadata_cache"

    id = Column(Integer, primary_key=True, index=True)
    connection_id = Column(Integer, ForeignKey("connections.id", ondelete="CASCADE"), nullable=False)

    # Schema discovery data
    schema_data = Column(JSON, nullable=False)  # Full schema structure
    table_count = Column(Integer, default=0)
    column_count = Column(Integer, default=0)

    # Scan metadata
    last_scanned_at = Column(DateTime, default=lambda: datetime.now(UTC))
    scan_duration_seconds = Column(Float)

    # Relationships
    connection = relationship("Connection", back_populates="connection_metadata_cache")


class Pipeline(Base):
    """Defines a data pipeline from source to destination
    Includes schedule, transformation config, and sync settings
    """

    __tablename__ = "pipelines"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, unique=True)
    description = Column(Text)

    # Source and Destination
    source_connection_id = Column(Integer, ForeignKey("connections.id"), nullable=False)
    destination_connection_id = Column(Integer, ForeignKey("connections.id"), nullable=False)

    # Pipeline Configuration
    source_config = Column(JSON, nullable=False)  # Query, tables, filters
    destination_config = Column(JSON, nullable=False)  # Target table, write mode

    # Transformation (Optional)
    transform_config = Column(JSON)  # SQL transformations, mappings

    # Scheduling
    schedule_cron = Column(String(100))  # CRON expression
    schedule_enabled = Column(Boolean, default=False)

    # Sync Strategy
    sync_mode = Column(String(50), default="full_refresh")  # full_refresh, incremental, append

    # State
    status = Column(SQLEnum(PipelineStatus), default=PipelineStatus.DRAFT)

    # Timestamps - FIXED
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    updated_at = Column(
        DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )
    last_run_at = Column(DateTime)
    next_run_at = Column(DateTime)

    # Relationships
    source_connection = relationship(
        "Connection", foreign_keys=[source_connection_id], back_populates="source_pipelines"
    )
    destination_connection = relationship(
        "Connection", foreign_keys=[destination_connection_id], back_populates="destination_pipelines"
    )
    jobs = relationship("Job", back_populates="pipeline", cascade="all, delete-orphan")
    state = relationship("PipelineState", back_populates="pipeline", uselist=False, cascade="all, delete-orphan")
    alerts = relationship("Alert", back_populates="pipeline", cascade="all, delete-orphan")
    versions = relationship("PipelineVersion", back_populates="pipeline", cascade="all, delete-orphan")


class PipelineVersion(Base):
    """Stores historical versions of pipeline configurations."""
    __tablename__ = "pipeline_versions"

    id = Column(Integer, primary_key=True, index=True)
    pipeline_id = Column(Integer, ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)
    version_number = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    created_by = Column(String(255), nullable=True) # e.g., user ID or system
    description = Column(Text, nullable=True)

    # Full snapshot of the pipeline's configuration
    config_snapshot = Column(JSON, nullable=False)

    __table_args__ = (
        UniqueConstraint("pipeline_id", "version_number", name="uq_pipeline_version_number"),
    )

    # Relationships
    pipeline = relationship("Pipeline", back_populates="versions")




class PipelineState(Base):
    """Tracks incremental sync state for pipelines
    Stores last synced position (timestamp, ID, cursor)
    """

    __tablename__ = "pipeline_states"

    id = Column(Integer, primary_key=True, index=True)
    pipeline_id = Column(Integer, ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False, unique=True)

    # State data (Singer-compatible)
    state_data = Column(JSON, nullable=False, default={})

    # Metrics
    last_record_count = Column(Integer, default=0)
    total_records_synced = Column(Integer, default=0)

    # Timestamps - FIXED
    updated_at = Column(
        DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )

    # Relationships
    pipeline = relationship("Pipeline", back_populates="state")


class Job(Base):
    """Individual job execution instance
    Tracks progress, logs, and metrics for each pipeline run
    """

    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, index=True)
    pipeline_id = Column(Integer, ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)

    # Execution metadata
    status = Column(SQLEnum(JobStatus), default=JobStatus.PENDING)
    celery_task_id = Column(String(255), unique=True, index=True)  # For tracking

    # Timing
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    duration_seconds = Column(Float)

    # Progress tracking
    records_extracted = Column(Integer, default=0)
    records_loaded = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)

    # Error handling
    error_message = Column(Text)
    error_traceback = Column(Text)

    # Timestamps - FIXED
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))

    # Relationships
    pipeline = relationship("Pipeline", back_populates="jobs")
    logs = relationship("JobLog", back_populates="job", cascade="all, delete-orphan")
    alerts = relationship("Alert", back_populates="job", cascade="all, delete-orphan")


class JobLog(Base):
    """Detailed logs for job execution
    Stores stdout, errors, warnings for debugging
    """

    __tablename__ = "job_logs"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)

    # Log details
    level = Column(String(20), nullable=False)  # INFO, WARNING, ERROR
    message = Column(Text, nullable=False)
    timestamp = Column(DateTime, default=lambda: datetime.now(UTC))

    # Optional metadata
    log_metadata = Column(JSON)  # Extra context (row numbers, etc.)

    # Relationships
    job = relationship("Job", back_populates="logs")


class SystemConfig(Base):
    """Stores system-wide configuration
    Including encryption keys, feature flags, etc.
    """

    __tablename__ = "system_config"

    id = Column(Integer, primary_key=True, index=True)
    key = Column(String(255), nullable=False, unique=True)
    value = Column(Text, nullable=False)
    description = Column(Text)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    updated_at = Column(
        DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )

class AlertConfig(Base):
    """Defines a reusable alert configuration
    e.g., "Email on pipeline failure to admin@example.com"
    """
    __tablename__ = "alert_configs"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, unique=True)
    description = Column(Text)

    alert_type = Column(SQLEnum(AlertType), nullable=False)
    delivery_method = Column(SQLEnum(AlertDeliveryMethod), nullable=False)
    recipient = Column(String(255), nullable=False) # e.g., email address, webhook URL

    # Optional: thresholds for triggering
    threshold_value = Column(Integer, default=1) # e.g., 1 for immediate failure, 3 for 3 consecutive failures
    threshold_unit = Column(String(50)) # e.g., "consecutive_failures", "minutes"

    enabled = Column(Boolean, default=True)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    updated_at = Column(
        DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )


class Alert(Base):
    """Instance of an alert triggered by a specific event"""
    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True, index=True)
    alert_config_id = Column(Integer, ForeignKey("alert_configs.id"), nullable=True) # Optional: if triggered by a config
    message = Column(Text, nullable=False)
    level = Column(SQLEnum(AlertLevel), default=AlertLevel.INFO)

    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=True)
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"), nullable=True)

    status = Column(SQLEnum(AlertStatus), default=AlertStatus.PENDING)
    delivery_method = Column(SQLEnum(AlertDeliveryMethod), nullable=False)
    recipient = Column(String(255), nullable=False)

    timestamp = Column(DateTime, default=lambda: datetime.now(UTC))
    sent_at = Column(DateTime, nullable=True)

    # Relationships
    alert_config = relationship("AlertConfig")
    job = relationship("Job", back_populates="alerts")
    pipeline = relationship("Pipeline", back_populates="alerts")

