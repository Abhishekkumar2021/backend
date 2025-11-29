# Data Agent Backend

## Overview
This is the backend service for the Universal Data Agent & Orchestrator. It's built with Python using FastAPI, SQLAlchemy for database interactions, and designed to be scalable and extensible.

## Features
- **API Layer (FastAPI):** Exposes RESTful APIs for managing connections, pipelines, jobs, and system status.
- **Security:** Implements a Master Password + DEK (Data Encryption Key) model for secure credential storage.
- **Database Management:** Uses SQLAlchemy for ORM and Alembic for database migrations against a PostgreSQL database.
- **Plugin Architecture:** Designed for easy integration of new source and destination connectors.

## Technology Stack
- **Python 3.11+**
- **FastAPI:** High-performance async API framework.
- **SQLAlchemy:** ORM for system database.
- **Alembic:** Database migrations.
- **Pydantic:** Data validation and settings management.
- **Cryptography (Fernet):** Credential encryption.
- **Uvicorn:** ASGI server for FastAPI.

## Setup and Run

### 1. Prerequisites
- Python 3.11+
- PostgreSQL database
- Redis (for future Celery integration)

### 2. Install Dependencies
Navigate to the `data-agent/backend` directory and install the required packages:
```bash
cd data-agent/backend
pip install -r requirements.txt
```

### 3. Environment Configuration
Create a `.env` file in the `data-agent/backend` directory based on `.env.example` (if available, otherwise refer to `app/core/config.py` for variables).
Example `.env` content:
```env
DATABASE_URL="postgresql://user:password@localhost:5432/data_agent_system"
REDIS_URL="redis://localhost:6379/0"
```
**Important:** Replace `user`, `password`, and `database_name` with your actual PostgreSQL credentials.

### 4. Database Migrations
Initialize and apply the database schema. This creates all necessary tables based on the SQLAlchemy models.
```bash
cd data-agent/backend
alembic revision --autogenerate -m "Initial migration" # Only run once for first migration
alembic upgrade head
```

### 5. Run the Application
```bash
cd data-agent/backend
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```
The API will be available at `http://localhost:8000`. You can access the API documentation at `http://localhost:8000/docs`.

## API Endpoints (Initial)

### System
- `POST /api/v1/system/init`: Initialize the system with a master password. (Requires `master_password` in body)
- `POST /api/v1/system/unlock`: Unlock the encryption service using the master password. (Requires `master_password` in body)

### Connections
- `GET /api/v1/connections`: Retrieve a list of all connections.
- `POST /api/v1/connections`: Create a new connection. (Requires `name`, `connector_type`, `config`, etc. in body)
- `GET /api/v1/connections/{connection_id}`: Get details of a specific connection by ID.
- `DELETE /api/v1/connections/{connection_id}`: Delete a connection by ID.

---
**Note:** This README will be updated as new features and API endpoints are implemented.
