from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.pipeline import Pipeline, PipelineCreate, PipelineUpdate, PipelineVersion, PipelineVersionCreate
from app.services.pipeline_service import PipelineService
from app.services.job_service import JobService
from app.schemas.job import Job as JobSchema

router = APIRouter()


@router.get("/", response_model=List[Pipeline])
def read_pipelines(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)) -> Any:
    """Retrieve pipelines.
    """
    pipelines = PipelineService.get_pipelines(db, skip=skip, limit=limit)
    return pipelines


@router.post("/", response_model=Pipeline, status_code=status.HTTP_201_CREATED)
def create_pipeline(
    *,
    pipeline_in: PipelineCreate,
    db: Session = Depends(get_db),
) -> Any:
    """Create a new pipeline.
    """
    pipeline = PipelineService.get_pipeline_by_name(db, name=pipeline_in.name)
    if pipeline:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The pipeline with this name already exists in the system.",
        )
    pipeline = PipelineService.create_pipeline(db, pipeline_in=pipeline_in)
    return pipeline


@router.get("/{pipeline_id}", response_model=Pipeline)
def read_pipeline(
    *,
    db: Session = Depends(get_db),
    pipeline_id: int,
) -> Any:
    """Get pipeline by ID.
    """
    pipeline = PipelineService.get_pipeline(db, pipeline_id=pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
    return pipeline


@router.get("/{pipeline_id}/history", response_model=List[JobSchema])
def get_pipeline_job_history(
    *,
    db: Session = Depends(get_db),
    pipeline_id: int,
    skip: int = 0,
    limit: int = 100,
) -> Any:
    """Get the job history for a specific pipeline.
    """
    # Check if pipeline exists
    pipeline = PipelineService.get_pipeline(db, pipeline_id=pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")

    jobs = JobService.get_jobs(db, skip=skip, limit=limit, pipeline_id=pipeline_id)
    return jobs


@router.get("/{pipeline_id}/versions", response_model=List[PipelineVersion])
def list_pipeline_versions(
    *,
    db: Session = Depends(get_db),
    pipeline_id: int,
    skip: int = 0,
    limit: int = 100,
) -> Any:
    """List all historical versions for a given pipeline."""
    pipeline = PipelineService.get_pipeline(db, pipeline_id=pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
    
    versions = PipelineService.get_pipeline_versions(db, pipeline_id=pipeline_id, skip=skip, limit=limit)
    return versions


@router.post("/{pipeline_id}/versions", response_model=PipelineVersion, status_code=status.HTTP_201_CREATED)
def create_pipeline_version_snapshot(
    *,
    db: Session = Depends(get_db),
    pipeline_id: int,
    version_in: PipelineVersionCreate,
) -> Any:
    """Create a new version (snapshot) of the current pipeline configuration."""
    pipeline = PipelineService.get_pipeline(db, pipeline_id=pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
    
    version = PipelineService.create_pipeline_version(db, pipeline_id, version_in.description)
    return version


@router.get("/{pipeline_id}/versions/{version_number}", response_model=PipelineVersion)
def get_pipeline_version_by_number(
    *,
    db: Session = Depends(get_db),
    pipeline_id: int,
    version_number: int,
) -> Any:
    """Get a specific pipeline version by its version number."""
    pipeline = PipelineService.get_pipeline(db, pipeline_id=pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
    
    version = PipelineService.get_pipeline_version_by_number(db, pipeline_id, version_number)
    if not version:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline version not found")
    return version


@router.post("/{pipeline_id}/restore/{version_number}", response_model=Pipeline)
def restore_pipeline_to_version(
    *,
    db: Session = Depends(get_db),
    pipeline_id: int,
    version_number: int,
) -> Any:
    """Restore a pipeline to a previous version."""
    pipeline = PipelineService.get_pipeline(db, pipeline_id=pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
    
    restored_pipeline = PipelineService.restore_pipeline_version(db, pipeline_id, version_number)
    if not restored_pipeline:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline version to restore not found")
    return restored_pipeline


@router.put("/{pipeline_id}", response_model=Pipeline)
def update_pipeline(
    *,
    db: Session = Depends(get_db),
    pipeline_id: int,
    pipeline_in: PipelineUpdate,
) -> Any:
    """Update a pipeline.
    """
    pipeline = PipelineService.get_pipeline(db, pipeline_id=pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")

    if pipeline_in.name:
        existing = PipelineService.get_pipeline_by_name(db, name=pipeline_in.name)
        if existing and existing.id != pipeline_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="The pipeline with this name already exists in the system.",
            )

    pipeline = PipelineService.update_pipeline(db, pipeline_id=pipeline_id, pipeline_in=pipeline_in)
    return pipeline


@router.delete("/{pipeline_id}", response_model=Pipeline)
def delete_pipeline(
    *,
    db: Session = Depends(get_db),
    pipeline_id: int,
) -> Any:
    """Delete a pipeline.
    """
    pipeline = PipelineService.get_pipeline(db, pipeline_id=pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
    pipeline = PipelineService.delete_pipeline(db, pipeline_id=pipeline_id)
    return pipeline
