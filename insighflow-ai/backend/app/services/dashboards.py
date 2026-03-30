"""Dashboard generation services."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import ApiException
from app.models.dashboard import Dashboard
from app.models.dataset import DatasetAsset
from app.models.user import User
from app.schemas.dashboard import DashboardGenerateRequest
from app.services.audit import log_audit_event
from app.services.projects import get_project_or_404


async def get_dashboard_or_404(session: AsyncSession, *, dashboard_id: int, actor: User) -> Dashboard:
    """Fetch a dashboard and enforce project access."""

    result = await session.execute(select(Dashboard).where(Dashboard.id == dashboard_id))
    dashboard = result.scalar_one_or_none()
    if dashboard is None:
        raise ApiException(status_code=404, code="dashboard_not_found", message="Dashboard not found.")
    await get_project_or_404(session, project_id=dashboard.project_id, user=actor)
    return dashboard


async def generate_dashboard(
    session: AsyncSession,
    *,
    actor: User,
    payload: DashboardGenerateRequest,
    ip_address: str | None,
) -> Dashboard:
    """Persist a dashboard blueprint derived from an analyzed dataset."""

    dataset = await session.get(DatasetAsset, payload.dataset_id)
    if dataset is None:
        raise ApiException(status_code=404, code="dataset_not_found", message="Dataset not found.")
    await get_project_or_404(session, project_id=dataset.project_id, user=actor, require_write=True)

    analysis_payload = dataset.analysis_payload or {}
    recommended_kpis = analysis_payload.get("recommended_kpis", [])
    chart_recommendations = analysis_payload.get("chart_recommendations", [])
    dashboard_layout = analysis_payload.get("dashboard_blueprint", {})

    if payload.custom_kpi_selections:
        selected = set(payload.custom_kpi_selections)
        filtered_kpis = [kpi for kpi in recommended_kpis if kpi.get("id") in selected]
        if not filtered_kpis:
            raise ApiException(status_code=400, code="invalid_kpi_selection", message="No valid KPI selections were provided.")
    else:
        filtered_kpis = recommended_kpis

    selected_columns = {column for kpi in filtered_kpis for column in kpi.get("source_columns", [])}
    filtered_charts = [
        chart
        for chart in chart_recommendations
        if not selected_columns or chart.get("x_field") in selected_columns or chart.get("y_field") in selected_columns
    ]
    allowed_component_ids = {kpi.get("id") for kpi in filtered_kpis} | {chart.get("id") for chart in filtered_charts} | {"data_preview"}
    components = [
        component for component in dashboard_layout.get("components", []) if component.get("id") in allowed_component_ids
    ]

    blueprint_payload: dict[str, Any] = {
        "dataset_id": dataset.id,
        "detected_domain": dataset.detected_domain,
        "recommended_kpis": filtered_kpis,
        "chart_recommendations": filtered_charts,
        "dashboard_layout": {**dashboard_layout, "components": components},
        "filter_options": dashboard_layout.get("filters", []),
        "drill_down_links": [
            {"target": chart.get("id"), "path": f"/datasets/{dataset.id}/insights"}
            for chart in filtered_charts[:3]
        ],
        "ai_insights": analysis_payload.get("ai_insights", {}),
    }

    dashboard = Dashboard(
        name=payload.name or f"{dataset.original_filename} Dashboard",
        project_id=dataset.project_id,
        dataset_id=dataset.id,
        generated_by_id=actor.id,
        blueprint_payload=blueprint_payload,
    )
    session.add(dashboard)
    await session.flush()
    await log_audit_event(
        session,
        action="dashboard.generated",
        resource_type="dashboard",
        resource_id=str(dashboard.id),
        user_id=actor.id,
        ip_address=ip_address,
        payload={"dataset_id": dataset.id, "dashboard_name": dashboard.name},
    )
    await session.commit()
    await session.refresh(dashboard)
    return dashboard
