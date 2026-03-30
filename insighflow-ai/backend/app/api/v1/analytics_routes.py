"""Advanced analytics engine endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db
from app.models.user import User
from app.schemas.advanced_analytics import AdvancedAnalyticsRequest, AdvancedAnalyticsResponse
from app.services import advanced_analytics as advanced_analytics_service

router = APIRouter(prefix="/analytics/advanced", tags=["advanced-analytics"])


@router.post("/run", response_model=AdvancedAnalyticsResponse)
async def run_advanced_analytics(
    payload: AdvancedAnalyticsRequest,
    request: Request,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> AdvancedAnalyticsResponse:
    result = await advanced_analytics_service.run_advanced_analytics(
        session,
        actor=current_user,
        dataset_id=payload.dataset_id,
        analyses=payload.analyses,
        options=payload.options,
        correlation_id=getattr(request.state, "request_id", None),
    )
    return AdvancedAnalyticsResponse(**result)
