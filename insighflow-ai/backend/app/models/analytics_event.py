"""Analytics event model."""

from __future__ import annotations

from sqlalchemy import ForeignKey, JSON, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, IntegerIDMixin, TimestampMixin


class AnalyticsEvent(Base, IntegerIDMixin, TimestampMixin):
    """Tracked analytics event."""

    __tablename__ = "analytics_events"

    event_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(100), nullable=False, default="api")
    payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    project_id: Mapped[int | None] = mapped_column(ForeignKey("projects.id", ondelete="SET NULL"), nullable=True, index=True)

    user = relationship("User", back_populates="analytics_events")
    project = relationship("Project", back_populates="analytics_events")
