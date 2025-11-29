"""
API Router - Aggregates all endpoint routers
"""

from fastapi import APIRouter

from app.api.v1.endpoints import connections, system, metadata

api_router = APIRouter()

# System endpoints (initialization, health)
api_router.include_router(system.router, prefix="/system", tags=["system"])

# Connection management
api_router.include_router(
    connections.router, prefix="/connections", tags=["connections"]
)

# Metadata scanning
api_router.include_router(metadata.router, prefix="/metadata", tags=["metadata"])
