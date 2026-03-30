"""Project-related models."""

from __future__ import annotations

from sqlalchemy import Enum, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, IntegerIDMixin, TimestampMixin
from app.models.enums import ProjectPermission, ProjectStatus


class Project(Base, IntegerIDMixin, TimestampMixin):
    """Project aggregate."""

    __tablename__ = "projects"

    name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[ProjectStatus] = mapped_column(Enum(ProjectStatus), nullable=False, default=ProjectStatus.ACTIVE)
    owner_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    owner = relationship("User", back_populates="owned_projects")
    shares = relationship("ProjectShare", back_populates="project", cascade="all, delete-orphan")
    insights = relationship("Insight", back_populates="project", cascade="all, delete-orphan")
    documents = relationship("ProjectDocument", back_populates="project", cascade="all, delete-orphan")
    datasets = relationship("DatasetAsset", back_populates="project", cascade="all, delete-orphan")
    dashboards = relationship("Dashboard", back_populates="project", cascade="all, delete-orphan")
    analytics_events = relationship("AnalyticsEvent", back_populates="project")


class ProjectShare(Base, IntegerIDMixin, TimestampMixin):
    """Project membership and sharing metadata."""

    __tablename__ = "project_shares"
    __table_args__ = (UniqueConstraint("project_id", "user_id", name="uq_project_shares_project_user"),)

    project_id: Mapped[int] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    permission: Mapped[ProjectPermission] = mapped_column(
        Enum(ProjectPermission),
        nullable=False,
        default=ProjectPermission.VIEWER,
    )

    project = relationship("Project", back_populates="shares")
    user = relationship("User", back_populates="project_memberships")


class ProjectDocument(Base, IntegerIDMixin, TimestampMixin):
    """Uploaded project document metadata."""

    __tablename__ = "project_documents"

    project_id: Mapped[int] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True)
    uploaded_by_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    stored_path: Mapped[str] = mapped_column(String(500), nullable=False, unique=True)
    content_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)

    project = relationship("Project", back_populates="documents")
    uploaded_by = relationship("User", back_populates="uploaded_documents")
