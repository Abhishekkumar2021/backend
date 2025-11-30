"""Main FastAPI Application Entry Point.

Universal Data Agent & Orchestrator.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1 import api
from app.connectors import factory
from app.core.config import settings
from app.core.database import engine
from app.models import db_models
from app.services.cache import get_cache
from app.utils.logging import setup_logging, get_logger

# Initialize logging (call once at startup)
setup_logging(
    log_dir="logs",
    log_level=logging.INFO if settings.ENVIRONMENT == "production" else logging.DEBUG,
    enable_console=True,
    enable_file=True,
    enable_daily=True,
)

# Get logger for this module
logger = get_logger(__name__)


def init_database() -> None:
    """Create database tables if they do not exist."""
    try:
        db_models.Base.metadata.create_all(bind=engine)
        logger.info("🗄️ Database tables ensured")
    except Exception as exc:
        logger.exception("❌ Failed to initialize database: %s", exc)
        raise


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Handles FastAPI startup & shutdown events (modern API)."""
    # --- Startup ---
    logger.info("🚀 Starting Data Agent System")
    logger.info("   Environment: %s", settings.ENVIRONMENT)
    logger.info("   API Version: %s", settings.API_V1_PREFIX)
    
    init_database()

    # Hide DB credentials in logs
    db_info = settings.DATABASE_URL.split("@")[-1] if "@" in settings.DATABASE_URL else settings.DATABASE_URL
    logger.info("📊 Connected Database: %s", db_info)

    logger.info("🔌 Registered Source Connectors: %s", factory.ConnectorFactory.list_sources())
    logger.info("🔌 Registered Destination Connectors: %s", factory.ConnectorFactory.list_destinations())

    # Check Redis availability
    cache = get_cache()
    if cache.is_available():
        stats = cache.get_stats()
        logger.info("✅ Redis available — Memory: %s", stats.get("used_memory_human", "N/A"))
    else:
        logger.warning("⚠️ Redis not available — running without cache")

    logger.info("✅ Application ready")

    yield

    # --- Shutdown ---
    logger.info("👋 Shutting down Data Agent System")
    logger.info("   Total uptime logged to logs/app.log")


app = FastAPI(
    title=settings.APP_NAME,
    openapi_url=f"{settings.API_V1_PREFIX}/openapi.json",
    docs_url=f"{settings.API_V1_PREFIX}/docs",
    redoc_url=f"{settings.API_V1_PREFIX}/redoc",
    lifespan=lifespan,
)

if settings.ALLOWED_ORIGINS != "*":
    origins = [origin.strip() for origin in settings.ALLOWED_ORIGINS.split(",")]
else:
    origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api.api_router, prefix=settings.API_V1_PREFIX)


@app.get("/", tags=["system"])
async def root() -> dict:
    """Root endpoint - system health check."""
    return {
        "app_name": settings.APP_NAME,
        "version": "0.1.0",
        "status": "running",
        "docs": f"{settings.API_V1_PREFIX}/docs",
    }


@app.get("/health", tags=["system"])
async def health_check() -> dict:
    """Health check endpoint for monitoring."""
    return {"status": "healthy", "environment": settings.ENVIRONMENT}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.ENVIRONMENT == "development",
    )