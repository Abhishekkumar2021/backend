"""Main FastAPI Application Entry Point.

Universal Data Agent & Orchestrator - Production-ready ETL platform.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Dict, Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.v1 import router
from app.connectors import factory
from app.core.config import settings
from app.core.database import engine
from app.core.logging import setup_logging, get_logger
from app.core.exceptions import DataAgentException
from app.models import database
from app.services.cache import get_cache

# Initialize structured logging
setup_logging(
    log_level=settings.LOG_LEVEL,
    log_dir="logs",
    json_logs=settings.JSON_LOGS,
)

logger = get_logger(__name__)


def init_database() -> None:
    """Initialize database tables.
    
    Creates all tables defined in SQLAlchemy models if they don't exist.
    
    Raises:
        Exception: If database initialization fails
    """
    try:
        database.Base.metadata.create_all(bind=engine)
        logger.info("database_initialized", message="All database tables created/verified")
    except Exception as exc:
        logger.error(
            "database_initialization_failed",
            error=str(exc),
            error_type=exc.__class__.__name__,
            exc_info=True,
        )
        raise


def register_connectors() -> Dict[str, int]:
    """Register all available connectors.
    
    Returns:
        Dictionary with counts of registered sources and destinations
    """
    sources = factory.ConnectorFactory.list_sources()
    destinations = factory.ConnectorFactory.list_destinations()
    
    logger.info(
        "connectors_registered",
        source_count=len(sources),
        destination_count=len(destinations),
        sources=sources,
        destinations=destinations,
    )
    
    return {
        "sources": len(sources),
        "destinations": len(destinations),
    }


def check_redis_availability() -> Dict[str, Any]:
    """Check Redis cache availability and stats.
    
    Returns:
        Dictionary with Redis status and statistics
    """
    cache = get_cache()
    
    if cache.is_available():
        stats = cache.get_stats()
        
        logger.info(
            "redis_available",
            memory_used=stats.get("used_memory_human", "unknown"),
            connected_clients=stats.get("connected_clients", 0),
            total_keys=stats.get("total_keys", 0),
        )
        
        return {
            "available": True,
            "memory_used": stats.get("used_memory_human"),
            "total_keys": stats.get("total_keys", 0),
        }
    else:
        logger.warning("redis_unavailable", message="Running without cache")
        return {"available": False}


def sanitize_database_url(url: str) -> str:
    """Remove credentials from database URL for logging.
    
    Args:
        url: Full database URL with credentials
        
    Returns:
        Sanitized URL without credentials
        
    Example:
        postgresql://user:pass@localhost:5432/db
        -> localhost:5432/db
    """
    if "@" in url:
        return url.split("@")[-1]
    return url


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Handle application lifecycle events.
    
    Manages startup and shutdown tasks including:
    - Database initialization
    - Connector registration
    - Cache availability check
    - Graceful shutdown logging
    
    Args:
        app: FastAPI application instance
        
    Yields:
        None during application runtime
    """
    # ========== STARTUP ==========
    logger.info(
        "application_starting",
        app_name=settings.APP_NAME,
        environment=settings.ENVIRONMENT,
        api_version=settings.API_V1_PREFIX,
    )
    
    try:
        # Initialize database
        init_database()
        
        # Log database connection (sanitized)
        db_info = sanitize_database_url(settings.DATABASE_URL)
        logger.info("database_connected", connection_string=db_info)
        
        # Register connectors
        connector_stats = register_connectors()
        
        # Check Redis availability
        redis_stats = check_redis_availability()
        
        logger.info(
            "application_ready",
            database="connected",
            connectors_registered=connector_stats,
            redis=redis_stats,
        )
        
    except Exception as e:
        logger.error(
            "application_startup_failed",
            error=str(e),
            error_type=e.__class__.__name__,
            exc_info=True,
        )
        raise
    
    yield
    
    # ========== SHUTDOWN ==========
    logger.info("application_shutting_down", message="Graceful shutdown initiated")
    
    # Cleanup tasks can be added here
    # e.g., close database connections, flush caches, etc.
    
    logger.info("application_shutdown_complete", message="All services stopped")


# Create FastAPI application
app = FastAPI(
    title=settings.APP_NAME,
    description="Production-ready ETL platform for universal data integration",
    version="0.1.0",
    openapi_url=f"{settings.API_V1_PREFIX}/openapi.json",
    docs_url=f"{settings.API_V1_PREFIX}/docs",
    redoc_url=f"{settings.API_V1_PREFIX}/redoc",
    lifespan=lifespan,
)


# Configure CORS
def get_cors_origins() -> list[str]:
    """Parse CORS origins from settings.
    
    Returns:
        List of allowed origins
    """
    if settings.ALLOWED_ORIGINS == ["*"]:
        return ["*"]
    
    # Settings.ALLOWED_ORIGINS is already a list from Pydantic validation
    return settings.ALLOWED_ORIGINS


app.add_middleware(
    CORSMiddleware,
    allow_origins=get_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Custom exception handler for application exceptions
@app.exception_handler(DataAgentException)
async def data_agent_exception_handler(request, exc: DataAgentException):
    """Handle custom application exceptions.
    
    Args:
        request: FastAPI request object
        exc: Application exception
        
    Returns:
        JSON error response
    """
    logger.error(
        "application_exception",
        exception_type=exc.__class__.__name__,
        error=str(exc),
        path=request.url.path,
    )
    
    return JSONResponse(
        status_code=500,
        content={
            "error": exc.__class__.__name__,
            "message": str(exc),
        },
    )


# Include API router
app.include_router(router.api_router, prefix=settings.API_V1_PREFIX)


@app.get("/", tags=["system"], response_model=Dict[str, Any])
async def root() -> Dict[str, Any]:
    """Root endpoint with system information.
    
    Returns:
        Dictionary with application metadata
    """
    logger.debug("root_endpoint_accessed")
    
    return {
        "app_name": settings.APP_NAME,
        "version": "0.1.0",
        "status": "running",
        "environment": settings.ENVIRONMENT,
        "documentation": f"{settings.API_V1_PREFIX}/docs",
        "api_version": settings.API_V1_PREFIX,
    }


@app.get("/health", tags=["system"], response_model=Dict[str, Any])
async def health_check() -> Dict[str, Any]:
    """Comprehensive health check endpoint.
    
    Checks:
    - Application status
    - Database connectivity
    - Redis cache availability
    - Registered connectors
    
    Returns:
        Dictionary with health status
    """
    logger.debug("health_check_requested")
    
    # Check database
    try:
        # Simple query to verify database connection
        engine.connect()
        db_status = "healthy"
    except Exception as e:
        logger.error("health_check_database_failed", error=str(e))
        db_status = "unhealthy"
    
    # Check Redis
    cache = get_cache()
    cache_status = "healthy" if cache.is_available() else "unavailable"
    
    # Connector counts
    connector_stats = {
        "sources": len(factory.ConnectorFactory.list_sources()),
        "destinations": len(factory.ConnectorFactory.list_destinations()),
    }
    
    health_status = {
        "status": "healthy" if db_status == "healthy" else "degraded",
        "environment": settings.ENVIRONMENT,
        "components": {
            "database": db_status,
            "cache": cache_status,
            "connectors": connector_stats,
        },
    }
    
    logger.info("health_check_completed", **health_status)
    
    return health_status


@app.get("/ready", tags=["system"], response_model=Dict[str, bool])
async def readiness_check() -> Dict[str, bool]:
    """Kubernetes readiness probe endpoint.
    
    Indicates if the application is ready to serve traffic.
    
    Returns:
        Dictionary with ready status
    """
    # Check critical dependencies
    try:
        engine.connect()
        return {"ready": True}
    except Exception:
        return {"ready": False}


@app.get("/live", tags=["system"], response_model=Dict[str, bool])
async def liveness_check() -> Dict[str, bool]:
    """Kubernetes liveness probe endpoint.
    
    Indicates if the application is alive and running.
    
    Returns:
        Dictionary with alive status
    """
    return {"alive": True}


if __name__ == "__main__":
    import uvicorn
    
    logger.info(
        "starting_uvicorn",
        host="0.0.0.0",
        port=8000,
        reload=settings.ENVIRONMENT == "development",
    )
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.ENVIRONMENT == "development",
        log_level=settings.LOG_LEVEL.lower(),
    )