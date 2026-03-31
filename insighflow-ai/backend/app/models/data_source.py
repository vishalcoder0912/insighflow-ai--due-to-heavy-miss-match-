"""Data source models for automated imports."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from sqlalchemy import (
    Enum as SQLEnum,
    ForeignKey,
    Integer,
    String,
    Text,
    DateTime,
    JSON,
    Boolean,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, IntegerIDMixin, TimestampMixin


class DataSourceType(str, Enum):
    """Supported data source types."""

    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    CSV_FILE = "csv_file"
    JSON_FILE = "json_file"
    API = "api"
    S3 = "s3"
    GOOGLE_SHEETS = "google_sheets"


class ScheduleFrequency(str, Enum):
    """Schedule frequencies."""

    ONCE = "once"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class DataSource(Base, IntegerIDMixin, TimestampMixin):
    """Data source configuration."""

    __tablename__ = "data_sources"

    name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    description: Mapped[str | None] = mapped_column(Text)

    source_type: Mapped[DataSourceType] = mapped_column(
        SQLEnum(DataSourceType), nullable=False
    )
    connection_config: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    project_id: Mapped[int] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True
    )

    is_scheduled: Mapped[bool] = mapped_column(Boolean, default=False)
    schedule_frequency: Mapped[ScheduleFrequency | None] = mapped_column(
        SQLEnum(ScheduleFrequency), nullable=True
    )
    next_sync: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_sync: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    user = relationship("User")
    project = relationship("Project")
    sync_logs = relationship(
        "DataSyncLog", back_populates="source", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<DataSource {self.id} ({self.source_type})>"


class DataSyncLog(Base, IntegerIDMixin, TimestampMixin):
    """Log of data sync operations."""

    __tablename__ = "data_sync_logs"

    source_id: Mapped[int] = mapped_column(
        ForeignKey("data_sources.id", ondelete="CASCADE"), nullable=False, index=True
    )

    status: Mapped[str] = mapped_column(String(20), nullable=False)
    rows_processed: Mapped[int] = mapped_column(Integer, default=0)
    rows_failed: Mapped[int] = mapped_column(Integer, default=0)
    duration_seconds: Mapped[float] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    source = relationship("DataSource", back_populates="sync_logs")

    def __repr__(self) -> str:
        return f"<DataSyncLog {self.id} ({self.status})>"
