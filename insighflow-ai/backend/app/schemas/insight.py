"""Insight schemas."""

from __future__ import annotations

from pydantic import BaseModel, Field

from app.models.enums import InsightSeverity, InsightStatus
from app.schemas.common import PaginationMeta, TimestampedModel


class InsightCreate(BaseModel):
    """Insight creation payload."""

    project_id: int
    title: str = Field(min_length=3, max_length=255)
    summary: str | None = None
    content: str = Field(min_length=10)
    status: InsightStatus = InsightStatus.DRAFT
    severity: InsightSeverity = InsightSeverity.MEDIUM
    tags: list[str] = []


class InsightUpdate(BaseModel):
    """Insight update payload."""

    title: str | None = Field(default=None, min_length=3, max_length=255)
    summary: str | None = None
    content: str | None = Field(default=None, min_length=10)
    status: InsightStatus | None = None
    severity: InsightSeverity | None = None
    tags: list[str] | None = None


class InsightRead(TimestampedModel):
    """Insight response."""

    project_id: int
    title: str
    summary: str | None
    content: str
    status: InsightStatus
    severity: InsightSeverity
    tags: list[str]


class InsightListResponse(BaseModel):
    """Paginated insights."""

    items: list[InsightRead]
    pagination: PaginationMeta
