"""Insight services."""

from __future__ import annotations

from sqlalchemy import asc, desc, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.exceptions import ApiException
from app.models.enums import InsightStatus, UserRole
from app.models.insight import Insight
from app.models.project import Project, ProjectShare
from app.models.user import User
from app.schemas.insight import InsightCreate, InsightUpdate
from app.services.audit import log_audit_event
from app.services.projects import get_project_or_404
from app.services.insight_generation import InsightsGenerator


def _is_admin(user: User) -> bool:
    return (
        user.role == UserRole.ADMIN or getattr(user.role, "value", user.role) == "admin"
    )


async def create_insight(
    session: AsyncSession,
    *,
    actor: User,
    payload: InsightCreate,
    ip_address: str | None,
) -> Insight:
    """Create an insight under a project."""

    await get_project_or_404(
        session, project_id=payload.project_id, user=actor, require_write=True
    )
    insight = Insight(**payload.model_dump())
    session.add(insight)
    await session.flush()
    await log_audit_event(
        session,
        action="insight.created",
        resource_type="insight",
        resource_id=str(insight.id),
        user_id=actor.id,
        ip_address=ip_address,
        payload={"project_id": payload.project_id, "title": payload.title},
    )
    await session.commit()
    await session.refresh(insight)
    return insight


async def get_insight_or_404(
    session: AsyncSession, *, insight_id: int, actor: User, require_write: bool = False
) -> Insight:
    """Fetch an insight and enforce project access."""

    result = await session.execute(
        select(Insight)
        .options(selectinload(Insight.project))
        .where(Insight.id == insight_id)
    )
    insight = result.scalar_one_or_none()
    if insight is None:
        raise ApiException(
            status_code=404, code="insight_not_found", message="Insight not found."
        )
    await get_project_or_404(
        session, project_id=insight.project_id, user=actor, require_write=require_write
    )
    return insight


async def update_insight(
    session: AsyncSession,
    *,
    insight: Insight,
    actor: User,
    payload: InsightUpdate,
    ip_address: str | None,
) -> Insight:
    """Update an insight."""

    for field, value in payload.model_dump(exclude_none=True).items():
        setattr(insight, field, value)
    await log_audit_event(
        session,
        action="insight.updated",
        resource_type="insight",
        resource_id=str(insight.id),
        user_id=actor.id,
        ip_address=ip_address,
        payload=payload.model_dump(exclude_none=True),
    )
    await session.commit()
    await session.refresh(insight)
    return insight


async def delete_insight(
    session: AsyncSession, *, insight: Insight, actor: User, ip_address: str | None
) -> None:
    """Delete an insight."""

    await log_audit_event(
        session,
        action="insight.deleted",
        resource_type="insight",
        resource_id=str(insight.id),
        user_id=actor.id,
        ip_address=ip_address,
    )
    await session.delete(insight)
    await session.commit()


async def list_insights(
    session: AsyncSession,
    *,
    actor: User,
    limit: int,
    offset: int,
    project_id: int | None = None,
    search: str | None = None,
    status: InsightStatus | None = None,
    sort_by: str = "created_at",
    sort_order: str = "desc",
) -> tuple[list[Insight], int]:
    """List accessible insights with filters."""

    order_column = {
        "title": Insight.title,
        "created_at": Insight.created_at,
        "updated_at": Insight.updated_at,
    }.get(
        sort_by,
        Insight.created_at,
    )
    order_clause = desc(order_column) if sort_order == "desc" else asc(order_column)

    query = select(Insight).join(Project, Insight.project_id == Project.id)
    if not _is_admin(actor):
        query = query.where(
            or_(
                Project.owner_id == actor.id,
                Project.shares.any(ProjectShare.user_id == actor.id),
            )
        )
    if project_id is not None:
        query = query.where(Insight.project_id == project_id)
    if status:
        query = query.where(Insight.status == status)
    if search:
        like_term = f"%{search.lower()}%"
        query = query.where(
            or_(
                func.lower(Insight.title).like(like_term),
                func.lower(func.coalesce(Insight.summary, "")).like(like_term),
                func.lower(Insight.content).like(like_term),
            )
        )

    count_result = await session.execute(
        select(func.count()).select_from(query.subquery())
    )
    total = int(count_result.scalar_one())
    result = await session.execute(
        query.order_by(order_clause).offset(offset).limit(limit)
    )
    return list(result.scalars().all()), total
