"""Project services and authorization helpers."""

from __future__ import annotations

from sqlalchemy import asc, desc, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.exceptions import ApiException
from app.models.dataset import DatasetAsset
from app.models.enums import ProjectPermission, ProjectStatus, UserRole
from app.models.project import Project, ProjectDocument, ProjectShare
from app.models.user import User
from app.schemas.project import ProjectCreate, ProjectShareRequest, ProjectUpdate
from app.services.audit import log_audit_event


def _is_admin(user: User) -> bool:
    return user.role == UserRole.ADMIN or getattr(user.role, "value", user.role) == "admin"


async def get_project_or_404(
    session: AsyncSession,
    *,
    project_id: int,
    user: User,
    require_write: bool = False,
) -> Project:
    """Fetch a project and enforce project-level access."""

    result = await session.execute(
        select(Project)
        .options(
            selectinload(Project.owner),
            selectinload(Project.shares),
            selectinload(Project.documents),
            selectinload(Project.datasets),
        )
        .where(Project.id == project_id)
    )
    project = result.scalar_one_or_none()
    if project is None:
        raise ApiException(status_code=404, code="project_not_found", message="Project not found.")
    if _is_admin(user) or project.owner_id == user.id:
        return project

    membership = next((share for share in project.shares if share.user_id == user.id), None)
    if membership is None:
        raise ApiException(status_code=403, code="project_access_denied", message="Access denied to this project.")
    if require_write and membership.permission != ProjectPermission.EDITOR:
        raise ApiException(status_code=403, code="project_write_denied", message="Write access denied for this project.")
    return project


async def list_projects(
    session: AsyncSession,
    *,
    user: User,
    limit: int,
    offset: int,
    sort_by: str,
    sort_order: str,
) -> tuple[list[Project], int]:
    """Return paginated projects visible to the user."""

    order_column = {"name": Project.name, "created_at": Project.created_at, "updated_at": Project.updated_at}.get(
        sort_by,
        Project.created_at,
    )
    order_clause = desc(order_column) if sort_order == "desc" else asc(order_column)

    base_query = select(Project).options(selectinload(Project.owner), selectinload(Project.shares))
    if not _is_admin(user):
        base_query = base_query.where(
            or_(
                Project.owner_id == user.id,
                Project.shares.any(ProjectShare.user_id == user.id),
            )
        )

    count_result = await session.execute(select(func.count()).select_from(base_query.subquery()))
    total = int(count_result.scalar_one())
    result = await session.execute(base_query.order_by(order_clause).offset(offset).limit(limit))
    return list(result.scalars().unique().all()), total


async def create_project(session: AsyncSession, *, user: User, payload: ProjectCreate, ip_address: str | None) -> Project:
    """Create a new project."""

    project = Project(
        name=payload.name,
        description=payload.description,
        status=payload.status,
        owner_id=user.id,
    )
    session.add(project)
    await session.flush()
    await log_audit_event(
        session,
        action="project.created",
        resource_type="project",
        resource_id=str(project.id),
        user_id=user.id,
        ip_address=ip_address,
        payload={"name": project.name},
    )
    await session.commit()
    return await get_project_or_404(session, project_id=project.id, user=user)


async def get_or_create_import_project(session: AsyncSession, *, user: User) -> Project:
    """Return a stable default project for imported datasets."""

    result = await session.execute(
        select(Project).where(Project.owner_id == user.id, Project.name == "Imported Datasets")
    )
    project = result.scalar_one_or_none()
    if project is not None:
        return project

    project = Project(
        name="Imported Datasets",
        description="Auto-created project for uploaded datasets.",
        status=ProjectStatus.ACTIVE,
        owner_id=user.id,
    )
    session.add(project)
    await session.flush()
    return project


async def update_project(
    session: AsyncSession,
    *,
    project: Project,
    actor: User,
    payload: ProjectUpdate,
    ip_address: str | None,
) -> Project:
    """Update project fields."""

    for field, value in payload.model_dump(exclude_none=True).items():
        setattr(project, field, value)
    await log_audit_event(
        session,
        action="project.updated",
        resource_type="project",
        resource_id=str(project.id),
        user_id=actor.id,
        ip_address=ip_address,
        payload=payload.model_dump(exclude_none=True),
    )
    await session.commit()
    return await get_project_or_404(session, project_id=project.id, user=actor)


async def delete_project(session: AsyncSession, *, project: Project, actor: User, ip_address: str | None) -> None:
    """Delete a project permanently."""

    await log_audit_event(
        session,
        action="project.deleted",
        resource_type="project",
        resource_id=str(project.id),
        user_id=actor.id,
        ip_address=ip_address,
    )
    await session.delete(project)
    await session.commit()


async def share_project(
    session: AsyncSession,
    *,
    project: Project,
    payload: ProjectShareRequest,
    actor: User,
    ip_address: str | None,
) -> ProjectShare:
    """Create or update a project share record."""

    if project.owner_id == payload.user_id:
        raise ApiException(status_code=400, code="invalid_share_target", message="Project owner already has full access.")

    result = await session.execute(
        select(ProjectShare).where(ProjectShare.project_id == project.id, ProjectShare.user_id == payload.user_id)
    )
    share = result.scalar_one_or_none()
    if share is None:
        share = ProjectShare(project_id=project.id, user_id=payload.user_id, permission=payload.permission)
        session.add(share)
    else:
        share.permission = payload.permission

    await log_audit_event(
        session,
        action="project.shared",
        resource_type="project",
        resource_id=str(project.id),
        user_id=actor.id,
        ip_address=ip_address,
        payload={"target_user_id": payload.user_id, "permission": payload.permission.value},
    )
    await session.commit()
    await session.refresh(share)
    return share


async def list_project_documents(project: Project) -> list[ProjectDocument]:
    """Return attached documents for a project."""

    return list(project.documents)


async def list_project_datasets(project: Project) -> list[DatasetAsset]:
    """Return analyzed datasets for a project."""

    return list(project.datasets)
