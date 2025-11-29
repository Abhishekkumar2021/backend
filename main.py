"""
Main FastAPI Application Entry Point
Universal Data Agent & Orchestrator
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging

from app.core.config import settings
from app.core.database import engine
from app.models import db_models
from app.api.v1 import api
from app.services.cache import get_cache

# Import to trigger connector registration
from app.connectors import factory

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# Create database tables
db_models.Base.metadata.create_all(bind=engine)

# Initialize FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    openapi_url=f"{settings.API_V1_PREFIX}/openapi.json",
    docs_url=f"{settings.API_V1_PREFIX}/docs",
    redoc_url=f"{settings.API_V1_PREFIX}/redoc",
)

# CORS middleware
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

# Include API router
app.include_router(api.api_router, prefix=settings.API_V1_PREFIX)


@app.on_event("startup")
async def startup_event():
    """Application startup tasks"""
    logger.info("🚀 Starting Data Agent System")
    logger.info(
        f"📊 Database: {settings.DATABASE_URL.split('@')[-1]}"
    )  # Hide credentials
    logger.info(
        f"🔌 Registered Source Connectors: {factory.ConnectorFactory.list_sources()}"
    )
    logger.info(
        f"🔌 Registered Destination Connectors: {factory.ConnectorFactory.list_destinations()}"
    )

    # Check Redis cache
    cache = get_cache()
    if cache.is_available():
        stats = cache.get_stats()
        logger.info(
            f"✅ Redis cache available - Memory: {stats.get('used_memory_human', 'N/A')}"
        )
    else:
        logger.warning("⚠️  Redis cache not available - Running without cache")

    logger.info("✅ Application ready")


@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown tasks"""
    logger.info("👋 Shutting down Data Agent System")


@app.get("/")
async def root():
    """Root endpoint - system health check"""
    return {
        "app_name": settings.APP_NAME,
        "version": "0.1.0",
        "status": "running",
        "docs": f"{settings.API_V1_PREFIX}/docs",
    }


@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    return {"status": "healthy", "environment": settings.ENVIRONMENT}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True if settings.ENVIRONMENT == "development" else False,
    )
