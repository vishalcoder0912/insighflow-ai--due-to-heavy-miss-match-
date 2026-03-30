"""Insight endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db, get_request_ip
from app.models.enums import InsightStatus
from app.models.user import User
from app.schemas.common import PaginationMeta
from app.schemas.insight import InsightCreate, InsightListResponse, InsightRead, InsightUpdate
from app.services import insights as insight_service

router = APIRouter(prefix="/insights", tags=["insights"])


@router.post("", response_model=InsightRead, status_code=status.HTTP_201_CREATED)
async def create_insight(
    payload: InsightCreate,
    request: Request,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> InsightRead:
    return await insight_service.create_insight(session, actor=current_user, payload=payload, ip_address=get_request_ip(request))


@router.get("", response_model=InsightListResponse)
async def list_insights(
    limit: int = 20,
    offset: int = 0,
    project_id: int | None = None,
    search: str | None = None,
    status_filter: InsightStatus | None = None,
    sort_by: str = "created_at",
    sort_order: str = "desc",
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> InsightListResponse:
    items, total = await insight_service.list_insights(
        session,
        actor=current_user,
        limit=limit,
        offset=offset,
        project_id=project_id,
        search=search,
        status=status_filter,
        sort_by=sort_by,
        sort_order=sort_order,
    )
    return InsightListResponse(items=items, pagination=PaginationMeta(total=total, limit=limit, offset=offset))


@router.get("/{insight_id}", response_model=InsightRead)
async def get_insight(
    insight_id: int,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> InsightRead:
    return await insight_service.get_insight_or_404(session, insight_id=insight_id, actor=current_user)


@router.put("/{insight_id}", response_model=InsightRead)
async def update_insight(
    insight_id: int,
    payload: InsightUpdate,
    request: Request,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> InsightRead:
    insight = await insight_service.get_insight_or_404(session, insight_id=insight_id, actor=current_user, require_write=True)
    return await insight_service.update_insight(
        session,
        insight=insight,
        actor=current_user,
        payload=payload,
        ip_address=get_request_ip(request),
    )


@router.delete("/{insight_id}", status_code=status.HTTP_204_NO_CONTENT, response_class=Response)
async def delete_insight(
    insight_id: int,
    request: Request,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Response:
    insight = await insight_service.get_insight_or_404(session, insight_id=insight_id, actor=current_user, require_write=True)
    await insight_service.delete_insight(session, insight=insight, actor=current_user, ip_address=get_request_ip(request))
    return Response(status_code=status.HTTP_204_NO_CONTENT)
