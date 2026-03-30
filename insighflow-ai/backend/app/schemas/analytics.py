"""Analytics schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class TrackEventRequest(BaseModel):
    """Custom event tracking payload."""

    event_name: str = Field(min_length=2, max_length=255)
    project_id: int | None = None
    payload: dict[str, Any] = {}
    source: str = "api"


class DashboardMetricsResponse(BaseModel):
    """Dashboard metrics response."""

    total_projects: int
    total_insights: int
    shared_projects: int
    total_datasets: int
    insights_by_status: dict[str, int]
    recent_events: list[dict[str, Any]]


class ProjectAnalyticsResponse(BaseModel):
    """Project analytics response."""

    project_id: int
    total_insights: int
    total_events: int
    total_datasets: int
    status_breakdown: dict[str, int]
    severity_breakdown: dict[str, int]
