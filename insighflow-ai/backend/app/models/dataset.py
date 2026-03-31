"""Uploaded dataset analysis model."""

from __future__ import annotations

from sqlalchemy import ForeignKey, Integer, JSON, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, IntegerIDMixin, TimestampMixin


class DatasetAsset(Base, IntegerIDMixin, TimestampMixin):
    """Persisted uploaded dataset and generated analysis."""

    __tablename__ = "datasets"

    project_id: Mapped[int] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True
    )
    uploaded_by_id: Mapped[int | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True
    )
    original_filename: Mapped[str] = mapped_column(String(255), nullable=False)
    file_format: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    encoding: Mapped[str | None] = mapped_column(String(100), nullable=True)
    file_size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    stored_path: Mapped[str] = mapped_column(String(500), nullable=False, unique=True)
    row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    column_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    detected_domain: Mapped[str | None] = mapped_column(String(100), nullable=True)
    analysis_payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

    project = relationship("Project", back_populates="datasets")
    uploaded_by = relationship("User", back_populates="uploaded_datasets")
    dashboards = relationship(
        "Dashboard", back_populates="dataset", cascade="all, delete-orphan"
    )
    chat_sessions = relationship(
        "ChatSession", back_populates="dataset", cascade="all, delete-orphan"
    )
