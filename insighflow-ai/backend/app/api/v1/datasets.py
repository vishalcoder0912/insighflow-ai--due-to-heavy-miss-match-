"""Dataset analysis endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, File, Form, Request, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db, get_request_ip
from app.models.user import User
from app.schemas.dataset import (
    DatasetAnalysisResponse,
    DatasetFullAnalysisResponse,
    DatasetInsightsResponse,
    DatasetPreviewResponse,
    DatasetStatisticsResponse,
    DatasetUploadResponse,
)
from app.services import datasets as dataset_service

router = APIRouter(prefix="/datasets", tags=["datasets"])


@router.post("/upload", response_model=DatasetUploadResponse, status_code=status.HTTP_201_CREATED)
async def upload_dataset(
    request: Request,
    file: UploadFile = File(...),
    project_id: int | None = Form(default=None),
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> DatasetUploadResponse:
    dataset = await dataset_service.upload_dataset(
        session,
        actor=current_user,
        upload=file,
        project_id=project_id,
        ip_address=get_request_ip(request),
    )
    return DatasetUploadResponse(**dataset_service.build_upload_response(dataset))


@router.post("/{dataset_id}/analyze", response_model=DatasetFullAnalysisResponse)
async def analyze_dataset(
    dataset_id: int,
    request: Request,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> DatasetFullAnalysisResponse:
    dataset = await dataset_service.get_dataset_or_404(session, dataset_id=dataset_id, actor=current_user)
    dataset = await dataset_service.reanalyze_dataset(
        session,
        dataset=dataset,
        actor=current_user,
        ip_address=get_request_ip(request),
    )
    return DatasetFullAnalysisResponse(**dataset_service.build_full_analysis_response(dataset))


@router.get("/{dataset_id}", response_model=DatasetAnalysisResponse)
async def get_dataset(
    dataset_id: int,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> DatasetAnalysisResponse:
    return await dataset_service.get_dataset_or_404(session, dataset_id=dataset_id, actor=current_user)


@router.get("/{dataset_id}/preview", response_model=DatasetPreviewResponse)
async def get_dataset_preview(
    dataset_id: int,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> DatasetPreviewResponse:
    dataset = await dataset_service.get_dataset_or_404(session, dataset_id=dataset_id, actor=current_user)
    return DatasetPreviewResponse(**dataset_service.build_preview_response(dataset))


@router.get("/{dataset_id}/statistics", response_model=DatasetStatisticsResponse)
async def get_dataset_statistics(
    dataset_id: int,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> DatasetStatisticsResponse:
    dataset = await dataset_service.get_dataset_or_404(session, dataset_id=dataset_id, actor=current_user)
    return DatasetStatisticsResponse(**dataset_service.build_statistics_response(dataset))


@router.get("/{dataset_id}/insights", response_model=DatasetInsightsResponse)
async def get_dataset_insights(
    dataset_id: int,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> DatasetInsightsResponse:
    dataset = await dataset_service.get_dataset_or_404(session, dataset_id=dataset_id, actor=current_user)
    return DatasetInsightsResponse(**dataset_service.build_insights_response(dataset))
