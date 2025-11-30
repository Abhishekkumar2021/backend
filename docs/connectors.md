# Connectors Guide

## Overview
The platform uses a plugin-based architecture for connectors. This allows for easy integration of new sources and destinations.

### Core Design Principles
-   **Plugin Everything:** New connectors should be 1 class + 5 methods.
-   **Abstract Base Classes:** All sources inherit from `SourceConnector`, destinations from `DestinationConnector`.

## Supported Connectors

### Database Connectors
-   **PostgreSQL:** Source & Destination
-   **MySQL:** Source & Destination
-   **MSSQL:** Source & Destination
-   **Oracle:** Source & Destination
-   **SQLite:** Source & Destination (Supports full refresh & incremental)

### File & Cloud Connectors
-   **Local File System:** Source & Destination
    -   Formats: Parquet, JSON, JSONL, CSV, Avro
-   **AWS S3:** Source & Destination
    -   Formats: Parquet, JSON, JSONL, CSV

## Connector Configuration Reference

### PostgreSQL
```json
{
  "connector_type": "postgresql",
  "is_source": true,
  "config": {
    "host": "localhost",
    "port": 5432,
    "database": "mydb",
    "user": "user",
    "password": "password",
    "schema": "public"
  }
}
```

### MySQL
```json
{
  "connector_type": "mysql",
  "is_source": true,
  "config": {
    "host": "localhost",
    "port": 3306,
    "database": "mydb",
    "user": "user",
    "password": "password"
  }
}
```

### MSSQL
```json
{
  "connector_type": "mssql",
  "is_source": true,
  "config": {
    "host": "localhost",
    "port": 1433,
    "database": "mydb",
    "user": "user",
    "password": "password"
  }
}
```

### Oracle
```json
{
  "connector_type": "oracle",
  "is_source": true,
  "config": {
    "dsn": "host:port/service_name",
    "user": "user",
    "password": "password"
  }
}
```

### SQLite
```json
{
  "connector_type": "sqlite",
  "is_source": true,
  "config": {
    "database_path": "/path/to/database.db",
    "batch_size": 1000
  }
}
```

### File System
**Destination:**
```json
{
  "connector_type": "local_file",
  "is_source": false,
  "config": {
    "base_path": "/data/output",
    "format": "parquet",
    "compression": "snappy",
    "overwrite": false
  }
}
```

**Source:**
```json
{
  "connector_type": "local_file",
  "is_source": true,
  "config": {
    "file_path": "/data/input",
    "format": "parquet",
    "glob_pattern": "*.parquet"
  }
}
```

### AWS S3
**Destination:**
```json
{
  "connector_type": "s3",
  "is_source": false,
  "config": {
    "bucket": "my-bucket",
    "prefix": "data/",
    "format": "parquet",
    "aws_access_key_id": "...",
    "aws_secret_access_key": "...",
    "region": "us-east-1"
  }
}
```

## Supported Formats Comparison

| Format | Compression | Speed | Size | Best For |
|--------|------------|-------|------|----------|
| **Parquet** | Snappy/Gzip | ⚡⚡⚡ Fast | 🗜️ Smallest | Analytics, data warehouses |
| **JSON** | None | ⚡ Moderate | 📦 Large | APIs, config files |
| **JSON Lines** | None | ⚡⚡ Fast | 📦 Large | Streaming, logs |
| **CSV** | None | ⚡⚡ Fast | 📦 Medium | Spreadsheets, legacy systems |
| **Avro** | Snappy | ⚡⚡⚡ Fast | 🗜️ Small | Schema evolution, Kafka |

## Adding a New Connector

1.  Create a source file in `app/connectors/sources/<name>.py` inheriting from `SourceConnector`.
2.  Create a destination file in `app/connectors/destinations/<name>.py` inheriting from `DestinationConnector`.
3.  Implement required methods:
    -   `test_connection()`
    -   `discover_schema()` (Source only)
    -   `read()` (Source only)
    -   `write()` (Destination only)
4.  Register the connector in `app/connectors/factory.py`.
