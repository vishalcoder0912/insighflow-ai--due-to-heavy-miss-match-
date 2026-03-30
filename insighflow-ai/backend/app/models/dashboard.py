"""Persisted generated dashboard blueprints."""

from __future__ import annotations

from sqlalchemy import ForeignKey, JSON, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, IntegerIDMixin, TimestampMixin


class Dashboard(Base, IntegerIDMixin, TimestampMixin):
    """Stored dashboard blueprint generated from a dataset."""

    __tablename__ = "dashboards"

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    project_id: Mapped[int] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True)
    dataset_id: Mapped[int] = mapped_column(ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False, index=True)
    generated_by_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    blueprint_payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

    project = relationship("Project", back_populates="dashboards")
    dataset = relationship("DatasetAsset", back_populates="dashboards")
    generated_by = relationship("User", back_populates="generated_dashboards")
