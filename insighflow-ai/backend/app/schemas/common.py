"""Common response schemas."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class ORMModel(BaseModel):
    """Base schema configured for ORM objects."""

    model_config = ConfigDict(from_attributes=True)


class ErrorDetail(BaseModel):
    """Structured API error details."""

    code: str
    message: str
    details: Any | None = None


class ErrorEnvelope(BaseModel):
    """Top-level API error format."""

    error: ErrorDetail
    request_id: str | None = None


class PaginationMeta(BaseModel):
    """Pagination metadata."""

    total: int
    limit: int
    offset: int


class PaginatedResponse(BaseModel):
    """Generic paginated response."""

    items: list[Any]
    pagination: PaginationMeta


class SuccessResponse(BaseModel):
    """Generic success response."""

    success: bool = True
    message: str
    data: Any | None = None


class ErrorResponse(BaseModel):
    """Generic error response."""

    success: bool = False
    error: str
    details: Any | None = None


class TimestampedModel(ORMModel):
    """Base schema with timestamps."""

    id: int
    created_at: datetime
    updated_at: datetime
