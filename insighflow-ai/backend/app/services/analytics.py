"""Analytics services."""

from __future__ import annotations

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.analytics_event import AnalyticsEvent
from app.models.dataset import DatasetAsset
from app.models.enums import UserRole
from app.models.insight import Insight
from app.models.project import Project, ProjectShare
from app.models.user import User
from app.schemas.analytics import TrackEventRequest
from app.services.projects import get_project_or_404


def _is_admin(user: User) -> bool:
    return user.role == UserRole.ADMIN or getattr(user.role, "value", user.role) == "admin"


async def track_event(session: AsyncSession, *, actor: User | None, payload: TrackEventRequest) -> AnalyticsEvent:
    """Persist a custom analytics event."""

    event = AnalyticsEvent(
        event_name=payload.event_name,
        source=payload.source,
        payload=payload.payload,
        user_id=actor.id if actor else None,
        project_id=payload.project_id,
    )
    session.add(event)
    await session.commit()
    await session.refresh(event)
    return event


async def get_dashboard_metrics(session: AsyncSession, *, actor: User) -> dict:
    """Aggregate top-level dashboard metrics for the user."""

    project_query = select(Project.id)
    if not _is_admin(actor):
        project_query = project_query.where(
            or_(
                Project.owner_id == actor.id,
                Project.shares.any(ProjectShare.user_id == actor.id),
            )
        )
    project_ids_result = await session.execute(project_query)
    project_ids = list(project_ids_result.scalars().all())

    total_projects = len(project_ids)
    shared_projects = 0
    if not _is_admin(actor):
        shared_result = await session.execute(select(func.count()).select_from(ProjectShare).where(ProjectShare.user_id == actor.id))
        shared_projects = int(shared_result.scalar_one())

    insight_count = 0
    dataset_count = 0
    status_breakdown: dict[str, int] = {}
    recent_events: list[dict] = []
    if project_ids:
        insight_count_result = await session.execute(select(func.count()).select_from(Insight).where(Insight.project_id.in_(project_ids)))
        insight_count = int(insight_count_result.scalar_one())
        dataset_count_result = await session.execute(select(func.count()).select_from(DatasetAsset).where(DatasetAsset.project_id.in_(project_ids)))
        dataset_count = int(dataset_count_result.scalar_one())
        status_result = await session.execute(
            select(Insight.status, func.count())
            .where(Insight.project_id.in_(project_ids))
            .group_by(Insight.status)
        )
        status_breakdown = {str(status.value if hasattr(status, "value") else status): count for status, count in status_result.all()}

        recent_result = await session.execute(
            select(AnalyticsEvent)
            .where(AnalyticsEvent.project_id.in_(project_ids))
            .order_by(AnalyticsEvent.created_at.desc())
            .limit(10)
        )
        recent_events = [
            {
                "id": event.id,
                "event_name": event.event_name,
                "source": event.source,
                "project_id": event.project_id,
                "created_at": event.created_at.isoformat(),
            }
            for event in recent_result.scalars().all()
        ]

    return {
        "total_projects": total_projects,
        "total_insights": insight_count,
        "shared_projects": shared_projects,
        "total_datasets": dataset_count,
        "insights_by_status": status_breakdown,
        "recent_events": recent_events,
    }


async def get_project_analytics(session: AsyncSession, *, actor: User, project_id: int) -> dict:
    """Aggregate analytics for one project."""

    project = await get_project_or_404(session, project_id=project_id, user=actor)
    insights_result = await session.execute(select(Insight).where(Insight.project_id == project.id))
    insights = list(insights_result.scalars().all())

    events_count_result = await session.execute(select(func.count()).select_from(AnalyticsEvent).where(AnalyticsEvent.project_id == project.id))
    datasets_count_result = await session.execute(select(func.count()).select_from(DatasetAsset).where(DatasetAsset.project_id == project.id))

    status_breakdown: dict[str, int] = {}
    severity_breakdown: dict[str, int] = {}
    for insight in insights:
        status_breakdown[insight.status.value] = status_breakdown.get(insight.status.value, 0) + 1
        severity_breakdown[insight.severity.value] = severity_breakdown.get(insight.severity.value, 0) + 1

    return {
        "project_id": project.id,
        "total_insights": len(insights),
        "total_events": int(events_count_result.scalar_one()),
        "total_datasets": int(datasets_count_result.scalar_one()),
        "status_breakdown": status_breakdown,
        "severity_breakdown": severity_breakdown,
    }
