"""User schemas."""

from __future__ import annotations

from pydantic import BaseModel, EmailStr, Field

from app.models.enums import UserRole
from app.schemas.common import TimestampedModel


class UserCreate(BaseModel):
    """Registration payload."""

    email: EmailStr
    full_name: str = Field(min_length=2, max_length=255)
    password: str = Field(min_length=8, max_length=128)


class UserUpdate(BaseModel):
    """Profile update payload."""

    full_name: str | None = Field(default=None, min_length=2, max_length=255)
    password: str | None = Field(default=None, min_length=8, max_length=128)
    two_factor_enabled: bool | None = None


class UserRead(TimestampedModel):
    """Public user response."""

    email: EmailStr
    full_name: str
    role: UserRole
    is_active: bool
    email_verified: bool
    two_factor_enabled: bool
