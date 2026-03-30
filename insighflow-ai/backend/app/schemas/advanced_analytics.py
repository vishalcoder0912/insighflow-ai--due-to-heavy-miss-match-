"""Schemas for advanced analytics endpoints."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


AnalysisName = Literal["forecasting", "clustering", "regression", "cohort", "rfm"]


class AdvancedAnalyticsRequest(BaseModel):
    """Request payload for the advanced analytics engine."""

    dataset_id: int
    analyses: list[AnalysisName] = Field(default_factory=lambda: ["forecasting", "clustering", "regression", "cohort", "rfm"])
    options: dict[str, Any] = Field(default_factory=dict)


class AdvancedAnalyticsResponse(BaseModel):
    """Structured advanced analytics response."""

    dataset_id: int
    filename: str
    detected_domain: str | None = None
    status: str
    analyses_requested: list[str] = Field(default_factory=list)
    successful_analyses: list[str] = Field(default_factory=list)
    failed_analyses: list[str] = Field(default_factory=list)
    results: dict[str, Any] = Field(default_factory=dict)
    failures: list[dict[str, Any]] = Field(default_factory=list)
    ai_insights: dict[str, Any] = Field(default_factory=dict)
