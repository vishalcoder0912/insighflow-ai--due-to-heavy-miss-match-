"""Pipeline API endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db
from app.models.user import User
from app.services.pipeline import (
    Pipeline,
    auto_configure_pipeline,
    PipelineStage,
    PipelineStatus,
)

router = APIRouter(prefix="/pipelines", tags=["pipelines"])


class PipelineRequest(BaseModel):
    """Pipeline execution request."""

    data: list[dict[str, Any]] = Field(..., description="Input data")
    config: dict[str, Any] | None = Field(
        default=None, description="Pipeline configuration"
    )


class PipelineResponse(BaseModel):
    """Pipeline execution response."""

    pipeline_id: str
    status: str
    input_rows: int
    input_columns: int
    output_rows: int
    output_columns: int
    stages: list[dict[str, Any]]
    total_duration_ms: float


@router.post(
    "/auto-configure",
    response_model=PipelineResponse,
    summary="Auto-configure pipeline",
)
async def auto_configure(
    request: Request,
    pipeline_request: PipelineRequest,
    current_user: User = Depends(get_current_user),
) -> PipelineResponse:
    """Auto-configure and execute a data processing pipeline."""
    result = auto_configure_pipeline(pipeline_request.data)
    return PipelineResponse(**result)


@router.post(
    "/execute", response_model=PipelineResponse, summary="Execute custom pipeline"
)
async def execute_pipeline(
    request: Request,
    pipeline_request: PipelineRequest,
    current_user: User = Depends(get_current_user),
) -> PipelineResponse:
    """Execute a data processing pipeline with custom configuration."""
    pipeline = Pipeline(pipeline_request.data, pipeline_request.config)
    result = pipeline.execute()
    return PipelineResponse(**result)
