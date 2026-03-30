"""Insight model."""

from __future__ import annotations

from sqlalchemy import JSON, Enum, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, IntegerIDMixin, TimestampMixin
from app.models.enums import InsightSeverity, InsightStatus


class Insight(Base, IntegerIDMixin, TimestampMixin):
    """Business insight attached to a project."""

    __tablename__ = "insights"

    project_id: Mapped[int] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[InsightStatus] = mapped_column(Enum(InsightStatus), nullable=False, default=InsightStatus.DRAFT)
    severity: Mapped[InsightSeverity] = mapped_column(
        Enum(InsightSeverity),
        nullable=False,
        default=InsightSeverity.MEDIUM,
    )
    tags: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)

    project = relationship("Project", back_populates="insights")
