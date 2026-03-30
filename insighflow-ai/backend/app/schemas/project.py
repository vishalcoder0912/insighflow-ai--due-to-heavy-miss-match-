"""Project schemas."""

from __future__ import annotations

from pydantic import BaseModel, Field

from app.models.enums import ProjectPermission, ProjectStatus
from app.schemas.common import PaginationMeta, TimestampedModel
from app.schemas.user import UserRead


class ProjectCreate(BaseModel):
    """Project creation payload."""

    name: str = Field(min_length=2, max_length=255)
    description: str | None = None
    status: ProjectStatus = ProjectStatus.ACTIVE


class ProjectUpdate(BaseModel):
    """Project update payload."""

    name: str | None = Field(default=None, min_length=2, max_length=255)
    description: str | None = None
    status: ProjectStatus | None = None


class ProjectShareRequest(BaseModel):
    """Project sharing payload."""

    user_id: int
    permission: ProjectPermission = ProjectPermission.VIEWER


class ProjectShareRead(TimestampedModel):
    """Shared membership response."""

    user_id: int
    permission: ProjectPermission


class ProjectDocumentRead(TimestampedModel):
    """Uploaded document response."""

    project_id: int
    uploaded_by_id: int | None
    filename: str
    stored_path: str
    content_type: str | None
    size_bytes: int


class ProjectRead(TimestampedModel):
    """Project response."""

    name: str
    description: str | None
    status: ProjectStatus
    owner_id: int
    owner: UserRead
    shares: list[ProjectShareRead] = Field(default_factory=list)


class ProjectListResponse(BaseModel):
    """Paginated projects."""

    items: list[ProjectRead]
    pagination: PaginationMeta
