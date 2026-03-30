"""Dashboard generation schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from app.schemas.common import TimestampedModel


class DashboardGenerateRequest(BaseModel):
    """Dashboard generation request."""

    dataset_id: int
    custom_kpi_selections: list[str] = Field(default_factory=list)
    name: str | None = None


class DashboardRead(TimestampedModel):
    """Persisted dashboard response."""

    name: str
    project_id: int
    dataset_id: int
    generated_by_id: int | None
    blueprint_payload: dict[str, Any] = Field(default_factory=dict)


class DashboardBlueprintResponse(BaseModel):
    """Frontend blueprint response."""

    dashboard_id: int
    dataset_id: int
    blueprint: dict[str, Any] = Field(default_factory=dict)
