"""Pipeline Management API Endpoints.

Provides CRUD operations for data pipelines with versioning support.
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.logging import get_logger
from app.models.enums import PipelineStatus
from app.schemas.job import Job as JobSchema
from app.schemas.pipeline import (
    Pipeline,
    PipelineCreate,
    PipelineUpdate,
    PipelineVersion,
    PipelineVersionCreate,
)
from app.services.job import JobService
from app.services.pipeline import PipelineService

logger = get_logger(__name__)
router = APIRouter()


# ===================================================================
# Pipeline CRUD Endpoints
# ===================================================================

@router.get(
    "/",
    response_model=List[Pipeline],
    summary="List pipelines",
)
def list_pipelines(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records to return"),
    status: Optional[PipelineStatus] = Query(None, description="Filter by status"),
    db: Session = Depends(get_db),
) -> List[Pipeline]:
    """List all pipelines with pagination and filtering.
    
    Args:
        skip: Pagination offset
        limit: Maximum records to return
        status: Filter by pipeline status
        db: Database session
        
    Returns:
        List of pipelines
    """
    logger.info(
        "pipelines_list_requested",
        skip=skip,
        limit=limit,
        status=status.value if status else None,
    )
    
    pipelines = PipelineService.get_pipelines(
        db,
        skip=skip,
        limit=limit,
        status=status,
    )
    
    logger.info(
        "pipelines_listed",
        count=len(pipelines),
    )
    
    return pipelines


@router.post(
    "/",
    response_model=Pipeline,
    status_code=status.HTTP_201_CREATED,
    summary="Create pipeline",
)
def create_pipeline(
    pipeline_in: PipelineCreate,
    db: Session = Depends(get_db),
) -> Pipeline:
    """Create a new data pipeline.
    
    Creates a pipeline with initial version (v1).
    
    Args:
        pipeline_in: Pipeline creation data
        db: Database session
        
    Returns:
        Created pipeline
        
    Raises:
        HTTPException: If pipeline name already exists
    """
    logger.info(
        "pipeline_creation_requested",
        name=pipeline_in.name,
    )
    
    # Check for duplicate name
    existing = PipelineService.get_pipeline_by_name(db, name=pipeline_in.name)
    if existing:
        logger.warning(
            "pipeline_name_conflict",
            name=pipeline_in.name,
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Pipeline with name '{pipeline_in.name}' already exists",
        )
    
    pipeline = PipelineService.create_pipeline(db, pipeline_in=pipeline_in)
    
    logger.info(
        "pipeline_created",
        pipeline_id=pipeline.id,
        name=pipeline.name,
    )
    
    return pipeline


@router.get(
    "/{pipeline_id}",
    response_model=Pipeline,
    summary="Get pipeline by ID",
)
def get_pipeline(
    pipeline_id: int,
    db: Session = Depends(get_db),
) -> Pipeline:
    """Retrieve a specific pipeline by ID.
    
    Args:
        pipeline_id: Pipeline ID
        db: Database session
        
    Returns:
        Pipeline object
        
    Raises:
        HTTPException: If pipeline not found
    """
    logger.debug(
        "pipeline_retrieval_requested",
        pipeline_id=pipeline_id,
    )
    
    pipeline = PipelineService.get_pipeline(db, pipeline_id=pipeline_id)
    
    if not pipeline:
        logger.warning(
            "pipeline_not_found",
            pipeline_id=pipeline_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline not found",
        )
    
    logger.debug(
        "pipeline_retrieved",
        pipeline_id=pipeline_id,
    )
    
    return pipeline


@router.put(
    "/{pipeline_id}",
    response_model=Pipeline,
    summary="Update pipeline",
)
def update_pipeline(
    pipeline_id: int,
    pipeline_in: PipelineUpdate,
    db: Session = Depends(get_db),
) -> Pipeline:
    """Update an existing pipeline.
    
    Creates a new version if configuration changes.
    
    Args:
        pipeline_id: Pipeline ID
        pipeline_in: Update data
        db: Database session
        
    Returns:
        Updated pipeline
        
    Raises:
        HTTPException: If pipeline not found or name conflict
    """
    logger.info(
        "pipeline_update_requested",
        pipeline_id=pipeline_id,
    )
    
    pipeline = PipelineService.get_pipeline(db, pipeline_id=pipeline_id)
    if not pipeline:
        logger.warning(
            "pipeline_not_found",
            pipeline_id=pipeline_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline not found",
        )

    # Check for name conflicts
    if pipeline_in.name:
        existing = PipelineService.get_pipeline_by_name(db, name=pipeline_in.name)
        if existing and existing.id != pipeline_id:
            logger.warning(
                "pipeline_name_conflict",
                name=pipeline_in.name,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Pipeline with name '{pipeline_in.name}' already exists",
            )

    updated_pipeline = PipelineService.update_pipeline(
        db,
        pipeline_id=pipeline_id,
        pipeline_in=pipeline_in,
    )
    
    logger.info(
        "pipeline_updated",
        pipeline_id=pipeline_id,
    )
    
    return updated_pipeline


@router.delete(
    "/{pipeline_id}",
    response_model=Pipeline,
    summary="Delete pipeline",
)
def delete_pipeline(
    pipeline_id: int,
    db: Session = Depends(get_db),
) -> Pipeline:
    """Delete a pipeline and all its versions.
    
    Args:
        pipeline_id: Pipeline ID
        db: Database session
        
    Returns:
        Deleted pipeline
        
    Raises:
        HTTPException: If pipeline not found
    """
    logger.info(
        "pipeline_deletion_requested",
        pipeline_id=pipeline_id,
    )
    
    pipeline = PipelineService.get_pipeline(db, pipeline_id=pipeline_id)
    if not pipeline:
        logger.warning(
            "pipeline_not_found",
            pipeline_id=pipeline_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline not found",
        )
    
    deleted_pipeline = PipelineService.delete_pipeline(db, pipeline_id=pipeline_id)
    
    logger.info(
        "pipeline_deleted",
        pipeline_id=pipeline_id,
    )
    
    return deleted_pipeline


# ===================================================================
# Pipeline Job History
# ===================================================================

@router.get(
    "/{pipeline_id}/history",
    response_model=List[JobSchema],
    summary="Get pipeline job history",
)
def get_pipeline_job_history(
    pipeline_id: int,
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records to return"),
    db: Session = Depends(get_db),
) -> List[JobSchema]:
    """Get execution history for a pipeline.
    
    Returns all jobs executed for this pipeline.
    
    Args:
        pipeline_id: Pipeline ID
        skip: Pagination offset
        limit: Maximum records to return
        db: Database session
        
    Returns:
        List of jobs
        
    Raises:
        HTTPException: If pipeline not found
    """
    logger.debug(
        "pipeline_history_requested",
        pipeline_id=pipeline_id,
        skip=skip,
        limit=limit,
    )
    
    # Verify pipeline exists
    pipeline = PipelineService.get_pipeline(db, pipeline_id=pipeline_id)
    if not pipeline:
        logger.warning(
            "pipeline_not_found",
            pipeline_id=pipeline_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline not found",
        )

    jobs = JobService.get_jobs(
        db,
        skip=skip,
        limit=limit,
        pipeline_id=pipeline_id,
    )
    
    logger.debug(
        "pipeline_history_retrieved",
        pipeline_id=pipeline_id,
        job_count=len(jobs),
    )
    
    return jobs


# ===================================================================
# Pipeline Versioning Endpoints
# ===================================================================

@router.get(
    "/{pipeline_id}/versions",
    response_model=List[PipelineVersion],
    summary="List pipeline versions",
)
def list_pipeline_versions(
    pipeline_id: int,
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records to return"),
    db: Session = Depends(get_db),
) -> List[PipelineVersion]:
    """List all historical versions for a pipeline.
    
    Args:
        pipeline_id: Pipeline ID
        skip: Pagination offset
        limit: Maximum records to return
        db: Database session
        
    Returns:
        List of pipeline versions
        
    Raises:
        HTTPException: If pipeline not found
    """
    logger.debug(
        "pipeline_versions_list_requested",
        pipeline_id=pipeline_id,
        skip=skip,
        limit=limit,
    )
    
    # Verify pipeline exists
    pipeline = PipelineService.get_pipeline(db, pipeline_id=pipeline_id)
    if not pipeline:
        logger.warning(
            "pipeline_not_found",
            pipeline_id=pipeline_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline not found",
        )
    
    versions = PipelineService.get_pipeline_versions(
        db,
        pipeline_id=pipeline_id,
        skip=skip,
        limit=limit,
    )
    
    logger.debug(
        "pipeline_versions_listed",
        pipeline_id=pipeline_id,
        version_count=len(versions),
    )
    
    return versions


@router.post(
    "/{pipeline_id}/versions",
    response_model=PipelineVersion,
    status_code=status.HTTP_201_CREATED,
    summary="Create pipeline version snapshot",
)
def create_pipeline_version_snapshot(
    pipeline_id: int,
    version_in: PipelineVersionCreate,
    db: Session = Depends(get_db),
) -> PipelineVersion:
    """Create a new version snapshot of the pipeline.
    
    Captures current configuration for versioning.
    
    Args:
        pipeline_id: Pipeline ID
        version_in: Version creation data
        db: Database session
        
    Returns:
        Created version
        
    Raises:
        HTTPException: If pipeline not found
    """
    logger.info(
        "pipeline_version_creation_requested",
        pipeline_id=pipeline_id,
        description=version_in.description,
    )
    
    # Verify pipeline exists
    pipeline = PipelineService.get_pipeline(db, pipeline_id=pipeline_id)
    if not pipeline:
        logger.warning(
            "pipeline_not_found",
            pipeline_id=pipeline_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline not found",
        )
    
    version = PipelineService.create_pipeline_version(
        db,
        pipeline_id,
        version_in.description,
    )
    
    logger.info(
        "pipeline_version_created",
        pipeline_id=pipeline_id,
        version_number=version.version_number,
    )
    
    return version


@router.get(
    "/{pipeline_id}/versions/{version_number}",
    response_model=PipelineVersion,
    summary="Get pipeline version",
)
def get_pipeline_version(
    pipeline_id: int,
    version_number: int,
    db: Session = Depends(get_db),
) -> PipelineVersion:
    """Get a specific pipeline version by number.
    
    Args:
        pipeline_id: Pipeline ID
        version_number: Version number to retrieve
        db: Database session
        
    Returns:
        Pipeline version
        
    Raises:
        HTTPException: If pipeline or version not found
    """
    logger.debug(
        "pipeline_version_retrieval_requested",
        pipeline_id=pipeline_id,
        version_number=version_number,
    )
    
    # Verify pipeline exists
    pipeline = PipelineService.get_pipeline(db, pipeline_id=pipeline_id)
    if not pipeline:
        logger.warning(
            "pipeline_not_found",
            pipeline_id=pipeline_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline not found",
        )
    
    version = PipelineService.get_pipeline_version_by_number(
        db,
        pipeline_id,
        version_number,
    )
    
    if not version:
        logger.warning(
            "pipeline_version_not_found",
            pipeline_id=pipeline_id,
            version_number=version_number,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Version {version_number} not found for pipeline",
        )
    
    logger.debug(
        "pipeline_version_retrieved",
        pipeline_id=pipeline_id,
        version_number=version_number,
    )
    
    return version


@router.post(
    "/{pipeline_id}/restore/{version_number}",
    response_model=Pipeline,
    summary="Restore pipeline to version",
)
def restore_pipeline_to_version(
    pipeline_id: int,
    version_number: int,
    db: Session = Depends(get_db),
) -> Pipeline:
    """Restore a pipeline to a previous version.
    
    Reverts configuration to the specified version and
    creates a new version entry for the restoration.
    
    Args:
        pipeline_id: Pipeline ID
        version_number: Version number to restore
        db: Database session
        
    Returns:
        Restored pipeline
        
    Raises:
        HTTPException: If pipeline or version not found
    """
    logger.info(
        "pipeline_restore_requested",
        pipeline_id=pipeline_id,
        version_number=version_number,
    )
    
    # Verify pipeline exists
    pipeline = PipelineService.get_pipeline(db, pipeline_id=pipeline_id)
    if not pipeline:
        logger.warning(
            "pipeline_not_found",
            pipeline_id=pipeline_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline not found",
        )
    
    restored_pipeline = PipelineService.restore_pipeline_version(
        db,
        pipeline_id,
        version_number,
    )
    
    if not restored_pipeline:
        logger.warning(
            "pipeline_version_not_found",
            pipeline_id=pipeline_id,
            version_number=version_number,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Version {version_number} not found for pipeline",
        )
    
    logger.info(
        "pipeline_restored",
        pipeline_id=pipeline_id,
        restored_to_version=version_number,
    )
    
    return restored_pipeline


# ===================================================================
# Pipeline Control Endpoints
# ===================================================================

@router.post(
    "/{pipeline_id}/activate",
    response_model=Pipeline,
    summary="Activate pipeline",
)
def activate_pipeline(
    pipeline_id: int,
    db: Session = Depends(get_db),
) -> Pipeline:
    """Activate a pipeline for scheduling.
    
    Args:
        pipeline_id: Pipeline ID
        db: Database session
        
    Returns:
        Activated pipeline
        
    Raises:
        HTTPException: If pipeline not found
    """
    logger.info(
        "pipeline_activation_requested",
        pipeline_id=pipeline_id,
    )
    
    pipeline = PipelineService.activate_pipeline(db, pipeline_id)
    
    if not pipeline:
        logger.warning(
            "pipeline_not_found",
            pipeline_id=pipeline_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline not found",
        )
    
    logger.info(
        "pipeline_activated",
        pipeline_id=pipeline_id,
    )
    
    return pipeline


@router.post(
    "/{pipeline_id}/pause",
    response_model=Pipeline,
    summary="Pause pipeline",
)
def pause_pipeline(
    pipeline_id: int,
    db: Session = Depends(get_db),
) -> Pipeline:
    """Pause a pipeline (disable scheduling).
    
    Args:
        pipeline_id: Pipeline ID
        db: Database session
        
    Returns:
        Paused pipeline
        
    Raises:
        HTTPException: If pipeline not found
    """
    logger.info(
        "pipeline_pause_requested",
        pipeline_id=pipeline_id,
    )
    
    pipeline = PipelineService.pause_pipeline(db, pipeline_id)
    
    if not pipeline:
        logger.warning(
            "pipeline_not_found",
            pipeline_id=pipeline_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline not found",
        )
    
    logger.info(
        "pipeline_paused",
        pipeline_id=pipeline_id,
    )
    
    return pipeline