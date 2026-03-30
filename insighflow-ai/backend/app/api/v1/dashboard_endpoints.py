"""Dashboard API endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db
from app.models.user import User
from app.services.dashboard import generate_dashboard
from app.services.visualization import score_visualizations

router = APIRouter(prefix="/dashboards", tags=["dashboards"])


class DashboardRequest(BaseModel):
    """Dashboard generation request."""

    data: list[dict[str, Any]] = Field(..., description="Input data")


class VisualizationScoreRequest(BaseModel):
    """Visualization scoring request."""

    data: list[dict[str, Any]] = Field(..., description="Input data")
    top_n: int = Field(5, description="Number of top charts to return")


class DashboardResponse(BaseModel):
    """Dashboard generation response."""

    dashboard_id: str
    created_at: str
    domain: str
    theme: dict[str, Any]
    metadata: dict[str, Any]
    kpis: list[dict[str, Any]]
    charts: list[dict[str, Any]]
    filters: list[dict[str, Any]]
    layout: list[dict[str, Any]]
    layout_summary: dict[str, Any]


class VisualizationScoreResponse(BaseModel):
    """Visualization scoring response."""

    charts: list[dict[str, Any]]


@router.post(
    "/auto-generate",
    response_model=DashboardResponse,
    summary="Auto-generate dashboard",
)
async def auto_generate_dashboard(
    request: DashboardRequest,
    current_user: User = Depends(get_current_user),
) -> DashboardResponse:
    """Automatically generate a dashboard based on data structure."""
    result = generate_dashboard(request.data)
    return DashboardResponse(**result)


@router.post(
    "/visualization-scores",
    response_model=VisualizationScoreResponse,
    summary="Score visualizations",
)
async def get_visualization_scores(
    request: VisualizationScoreRequest,
    current_user: User = Depends(get_current_user),
) -> VisualizationScoreResponse:
    """Score and rank visualizations based on data patterns."""
    charts = score_visualizations(request.data, request.top_n)
    return VisualizationScoreResponse(charts=charts)
