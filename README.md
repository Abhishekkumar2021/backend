# Universal Data Agent & Orchestrator (Backend)

A production-grade, self-hosted data integration platform (similar to Airbyte/Meltano) built with Python, FastAPI, and React.

## 📚 Documentation

Full documentation is available in the `docs/` directory:

-   [**Architecture**](docs/architecture.md) - System design, components, and data models.
-   [**Setup & Installation**](docs/setup.md) - How to install and run the backend.
-   [**Connectors Guide**](docs/connectors.md) - Supported connectors (PostgreSQL, S3, Files) and usage.
-   [**Caching**](docs/caching.md) - Redis integration for performance.
-   [**Roadmap**](docs/roadmap.md) - Project status and future plans.
-   [**Development Standards**](docs/standards.md) - Coding guidelines and architectural principles.

## 🚀 Quick Start

1.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configure `.env`:**
    Create a `.env` file in the `backend` directory with your database and Redis URLs. Additionally, set a `MASTER_PASSWORD` for encryption.
    ```env
    DATABASE_URL="postgresql://user:password@localhost:5432/data_agent_system"
    REDIS_URL="redis://localhost:6379/0"
    MASTER_PASSWORD="your_secure_master_password" # This is used to encrypt/decrypt sensitive data
    ```

3.  **Run Migrations:**
    ```bash
    alembic upgrade head
    ```

4.  **Start Services:**
    To run the full application, you need three separate processes:
    *   **FastAPI Server:**
        ```bash
        uvicorn main:app --reload
        ```
    *   **Celery Worker:** (Ensure `MASTER_PASSWORD` is set in its environment)
        ```bash
        celery -A app.core.celery_app worker -l info
        ```
    *   **Celery Beat Scheduler:** (Ensure `MASTER_PASSWORD` is set in its environment)
        ```bash
        celery -A app.core.celery_app beat -l info
        ```

## ✨ Key Features
-   **Plugin-based Architecture** for Connectors
-   **Secure Credential Storage** (Master Password + DEK)
-   **Dynamic Scheduling** with Celery Beat
-   **Incremental Syncs**
-   **Real-time Monitoring** (WebSockets)
-   **Redis Caching**

## License
[MIT](LICENSE)