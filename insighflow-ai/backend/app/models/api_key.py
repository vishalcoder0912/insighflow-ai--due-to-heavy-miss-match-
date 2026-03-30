"""Service API keys."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, JSON, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, IntegerIDMixin, TimestampMixin


class ApiKey(Base, IntegerIDMixin, TimestampMixin):
    """Database-backed API keys."""

    __tablename__ = "api_keys"

    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    key_hash: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    scopes: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_by_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    creator = relationship("User", back_populates="created_api_keys")
