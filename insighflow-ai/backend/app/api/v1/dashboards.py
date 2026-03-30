"""Dashboard generation endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db, get_request_ip
from app.models.user import User
from app.schemas.dashboard import DashboardBlueprintResponse, DashboardGenerateRequest
from app.services import dashboards as dashboard_service

router = APIRouter(prefix="/dashboards", tags=["dashboards"])


@router.post("/generate", response_model=DashboardBlueprintResponse, status_code=status.HTTP_201_CREATED)
async def generate_dashboard(
    payload: DashboardGenerateRequest,
    request: Request,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> DashboardBlueprintResponse:
    dashboard = await dashboard_service.generate_dashboard(
        session,
        actor=current_user,
        payload=payload,
        ip_address=get_request_ip(request),
    )
    return DashboardBlueprintResponse(
        dashboard_id=dashboard.id,
        dataset_id=dashboard.dataset_id,
        blueprint=dashboard.blueprint_payload,
    )


@router.get("/{dashboard_id}/blueprint", response_model=DashboardBlueprintResponse)
async def get_dashboard_blueprint(
    dashboard_id: int,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> DashboardBlueprintResponse:
    dashboard = await dashboard_service.get_dashboard_or_404(session, dashboard_id=dashboard_id, actor=current_user)
    return DashboardBlueprintResponse(
        dashboard_id=dashboard.id,
        dataset_id=dashboard.dataset_id,
        blueprint=dashboard.blueprint_payload,
    )
