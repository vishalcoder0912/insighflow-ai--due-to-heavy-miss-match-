"""User model."""

from __future__ import annotations

from sqlalchemy import Boolean, Enum, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, IntegerIDMixin, TimestampMixin
from app.models.enums import UserRole


class User(Base, IntegerIDMixin, TimestampMixin):
    """Application user."""

    __tablename__ = "users"

    email: Mapped[str] = mapped_column(
        String(255), unique=True, index=True, nullable=False
    )
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[UserRole] = mapped_column(
        Enum(UserRole), nullable=False, default=UserRole.USER
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    email_verified: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    two_factor_enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    two_factor_secret: Mapped[str | None] = mapped_column(String(255), nullable=True)
    verification_token: Mapped[str | None] = mapped_column(String(255), nullable=True)

    owned_projects = relationship(
        "Project", back_populates="owner", cascade="all, delete-orphan"
    )
    project_memberships = relationship(
        "ProjectShare", back_populates="user", cascade="all, delete-orphan"
    )
    refresh_tokens = relationship(
        "RefreshToken", back_populates="user", cascade="all, delete-orphan"
    )
    audit_logs = relationship("AuditLog", back_populates="user")
    analytics_events = relationship("AnalyticsEvent", back_populates="user")
    created_api_keys = relationship("ApiKey", back_populates="creator")
    uploaded_documents = relationship("ProjectDocument", back_populates="uploaded_by")
    uploaded_datasets = relationship("DatasetAsset", back_populates="uploaded_by")
    generated_dashboards = relationship("Dashboard", back_populates="generated_by")
    chat_sessions = relationship(
        "ChatSession", back_populates="user", cascade="all, delete-orphan"
    )
