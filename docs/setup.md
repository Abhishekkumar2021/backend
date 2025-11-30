# Setup & Installation

## Prerequisites
-   **Python 3.11+**
-   **PostgreSQL** database
-   **Redis** (for caching and background jobs)

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repo-url>
    cd data-agent/backend
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Environment Configuration:**
    Create a `.env` file in the `backend` directory.
    ```env
    DATABASE_URL="postgresql://user:password@localhost:5432/data_agent_system"
    REDIS_URL="redis://localhost:6379/0"
    MASTER_PASSWORD="your_secure_master_password" # This is used to encrypt/decrypt sensitive data
    ```
    *Replace with your actual credentials.*

4.  **Database Migrations:**
    Initialize the database schema.
    ```bash
    alembic upgrade head
    ```

## Running the Application

To run the full application, you need three separate processes:

1.  **Start the FastAPI server:**
    ```bash
    uvicorn main:app --reload --host 0.0.0.0 --port 8000
    ```
    -   **API:** `http://localhost:8000`
    -   **Docs:** `http://localhost:8000/docs`

2.  **Start the Celery Worker:**
    Ensure the `MASTER_PASSWORD` environment variable is set in the worker's environment.
    ```bash
    celery -A app.core.celery_app worker -l info
    ```

3.  **Start the Celery Beat Scheduler:**
    Ensure the `MASTER_PASSWORD` environment variable is set in the Beat scheduler's environment.
    ```bash
    celery -A app.core.celery_app beat -l info
    ```

## Testing
Run the automated test suite:
```bash
python -m pytest tests
```
