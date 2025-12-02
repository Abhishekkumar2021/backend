import enum


# =============================================================
# Connectors
# =============================================================
class ConnectorType(str, enum.Enum):
    # SQL
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    MSSQL = "mssql"
    ORACLE = "oracle"
    SQLITE = "sqlite"

    # NoSQL
    MONGODB = "mongodb"
    REDIS = "redis"

    # Cloud warehouses
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    REDSHIFT = "redshift"

    # File / Storage
    LOCAL_FILE = "local_file"
    S3 = "s3"
    GCS = "gcs"
    AZURE_BLOB = "azure_blob"

    # APIs / Integrations
    REST_API = "rest_api"
    GRAPHQL = "graphql"
    SINGER = "singer"

    # SaaS
    GOOGLE_SHEETS = "google_sheets"
    AIRTABLE = "airtable"


# =============================================================
# Pipeline-level definitions
# =============================================================
class PipelineStatus(str, enum.Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    ARCHIVED = "archived"


class PipelineRunStatus(str, enum.Enum):
    PENDING = "pending"
    INITIALIZING = "initializing"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class OperatorRunStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class OperatorType(str, enum.Enum):
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"
    CUSTOM = "custom"
    NOOP = "noop"


class DataDirection(str, enum.Enum):
    SOURCE = "source"
    DESTINATION = "destination"
    STAGE = "stage"


# =============================================================
# Jobs
# =============================================================
class JobStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RetryStrategy(str, enum.Enum):
    NONE = "none"
    FIXED_INTERVAL = "fixed_interval"
    EXPONENTIAL = "exponential"
    CUSTOM = "custom"


# =============================================================
# Alerts
# =============================================================
class AlertLevel(str, enum.Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertStatus(str, enum.Enum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"


class AlertType(str, enum.Enum):
    JOB_FAILURE = "job_failure"
    PIPELINE_STATUS_CHANGE = "pipeline_status_change"
    CUSTOM_METRIC_THRESHOLD = "custom_metric_threshold"


class AlertDeliveryMethod(str, enum.Enum):
    EMAIL = "email"
    WEBHOOK = "webhook"
    SLACK = "slack"
