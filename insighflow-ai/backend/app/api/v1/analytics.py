"""Analytics endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db, get_service_or_user
from app.models.user import User
from app.schemas.analytics import DashboardMetricsResponse, ProjectAnalyticsResponse, TrackEventRequest
from app.services import analytics as analytics_service

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/dashboard", response_model=DashboardMetricsResponse)
async def get_dashboard_metrics(
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> DashboardMetricsResponse:
    return DashboardMetricsResponse(**await analytics_service.get_dashboard_metrics(session, actor=current_user))


@router.get("/projects/{project_id}", response_model=ProjectAnalyticsResponse)
async def get_project_analytics(
    project_id: int,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectAnalyticsResponse:
    return ProjectAnalyticsResponse(**await analytics_service.get_project_analytics(session, actor=current_user, project_id=project_id))


@router.post("/track", status_code=status.HTTP_201_CREATED)
async def track_event(
    payload: TrackEventRequest,
    session: AsyncSession = Depends(get_db),
    actor: User | None = Depends(get_service_or_user),
) -> dict[str, int]:
    event = await analytics_service.track_event(session, actor=actor, payload=payload)
    return {"event_id": event.id}
