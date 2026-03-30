"""Project endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, File, Request, Response, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db, get_request_ip
from app.models.user import User
from app.schemas.dataset import DatasetAnalysisResponse
from app.schemas.analytics import ProjectAnalyticsResponse
from app.schemas.insight import InsightListResponse
from app.schemas.project import ProjectCreate, ProjectListResponse, ProjectRead, ProjectShareRead, ProjectShareRequest, ProjectUpdate
from app.schemas.common import PaginationMeta
from app.services import analysis as analysis_service
from app.services import analytics as analytics_service
from app.services import insights as insight_service
from app.services import projects as project_service

router = APIRouter(prefix="/projects", tags=["projects"])


@router.post("", response_model=ProjectRead, status_code=status.HTTP_201_CREATED)
async def create_project(
    payload: ProjectCreate,
    request: Request,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectRead:
    return await project_service.create_project(session, user=current_user, payload=payload, ip_address=get_request_ip(request))


@router.get("", response_model=ProjectListResponse)
async def list_projects(
    limit: int = 20,
    offset: int = 0,
    sort_by: str = "created_at",
    sort_order: str = "desc",
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectListResponse:
    items, total = await project_service.list_projects(
        session,
        user=current_user,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        sort_order=sort_order,
    )
    return ProjectListResponse(items=items, pagination=PaginationMeta(total=total, limit=limit, offset=offset))


@router.get("/{project_id}", response_model=ProjectRead)
async def get_project(
    project_id: int,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectRead:
    return await project_service.get_project_or_404(session, project_id=project_id, user=current_user)


@router.put("/{project_id}", response_model=ProjectRead)
async def update_project(
    project_id: int,
    payload: ProjectUpdate,
    request: Request,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectRead:
    project = await project_service.get_project_or_404(session, project_id=project_id, user=current_user, require_write=True)
    return await project_service.update_project(
        session,
        project=project,
        actor=current_user,
        payload=payload,
        ip_address=get_request_ip(request),
    )


@router.delete("/{project_id}", status_code=status.HTTP_204_NO_CONTENT, response_class=Response)
async def delete_project(
    project_id: int,
    request: Request,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Response:
    project = await project_service.get_project_or_404(session, project_id=project_id, user=current_user, require_write=True)
    await project_service.delete_project(session, project=project, actor=current_user, ip_address=get_request_ip(request))
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/{project_id}/share", response_model=ProjectShareRead)
async def share_project(
    project_id: int,
    payload: ProjectShareRequest,
    request: Request,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectShareRead:
    project = await project_service.get_project_or_404(session, project_id=project_id, user=current_user, require_write=True)
    return await project_service.share_project(
        session,
        project=project,
        payload=payload,
        actor=current_user,
        ip_address=get_request_ip(request),
    )


@router.get("/{project_id}/insights", response_model=InsightListResponse)
async def get_project_insights(
    project_id: int,
    limit: int = 20,
    offset: int = 0,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> InsightListResponse:
    await project_service.get_project_or_404(session, project_id=project_id, user=current_user)
    items, total = await insight_service.list_insights(
        session,
        actor=current_user,
        limit=limit,
        offset=offset,
        project_id=project_id,
    )
    return InsightListResponse(items=items, pagination=PaginationMeta(total=total, limit=limit, offset=offset))


@router.post("/{project_id}/datasets/analyze", response_model=DatasetAnalysisResponse, status_code=status.HTTP_201_CREATED)
async def analyze_dataset(
    project_id: int,
    request: Request,
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> DatasetAnalysisResponse:
    return await analysis_service.analyze_dataset_upload(
        session,
        actor=current_user,
        project_id=project_id,
        upload=file,
        ip_address=get_request_ip(request),
    )


@router.get("/{project_id}/datasets", response_model=list[DatasetAnalysisResponse])
async def list_project_datasets(
    project_id: int,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[DatasetAnalysisResponse]:
    project = await project_service.get_project_or_404(session, project_id=project_id, user=current_user)
    return await project_service.list_project_datasets(project)


@router.get("/{project_id}/analytics", response_model=ProjectAnalyticsResponse)
async def get_project_analytics(
    project_id: int,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectAnalyticsResponse:
    return ProjectAnalyticsResponse(**await analytics_service.get_project_analytics(session, actor=current_user, project_id=project_id))
