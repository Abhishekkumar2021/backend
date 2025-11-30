"""API Router - Aggregates all endpoint routers
"""

from fastapi import APIRouter

from app.api.v1.endpoints import connections, jobs, metadata, pipelines, system, websocket, alerts

api_router = APIRouter()

# System endpoints (initialization, health)
api_router.include_router(system.router, prefix="/system", tags=["system"])

# Connection management
api_router.include_router(connections.router, prefix="/connections", tags=["connections"])

# Metadata scanning
api_router.include_router(metadata.router, prefix="/metadata", tags=["metadata"])

# Pipeline management
api_router.include_router(pipelines.router, prefix="/pipelines", tags=["pipelines"])

# Job management
api_router.include_router(jobs.router, prefix="/jobs", tags=["jobs"])

# Alert management
api_router.include_router(alerts.router, prefix="/alerts", tags=["alerts"])

# WebSockets
api_router.include_router(websocket.router)
