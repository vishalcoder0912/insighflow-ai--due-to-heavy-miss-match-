"""Dataset query and re-analysis services."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from fastapi import UploadFile
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import ApiException
from app.models.dataset import DatasetAsset
from app.models.user import User
from app.services import analysis as analysis_service
from app.services.audit import log_audit_event
from app.services.projects import get_or_create_import_project, get_project_or_404


async def get_dataset_or_404(
    session: AsyncSession, *, dataset_id: int, actor: User
) -> DatasetAsset:
    """Fetch a dataset and enforce project access."""

    result = await session.execute(
        select(DatasetAsset).where(DatasetAsset.id == dataset_id)
    )
    dataset = result.scalar_one_or_none()
    if dataset is None:
        raise ApiException(
            status_code=404, code="dataset_not_found", message="Dataset not found."
        )
    await get_project_or_404(session, project_id=dataset.project_id, user=actor)
    return dataset


async def upload_dataset(
    session: AsyncSession,
    *,
    actor: User,
    upload: UploadFile,
    ip_address: str | None,
    project_id: int | None = None,
) -> DatasetAsset:
    """Upload a dataset into the requested or default project."""

    if project_id is None:
        project = await get_or_create_import_project(session, user=actor)
        await session.flush()
        project_id = project.id
    return await analysis_service.analyze_dataset_upload(
        session,
        actor=actor,
        project_id=project_id,
        upload=upload,
        ip_address=ip_address,
    )


async def reanalyze_dataset(
    session: AsyncSession,
    *,
    dataset: DatasetAsset,
    actor: User,
    ip_address: str | None,
) -> DatasetAsset:
    """Re-run the analysis pipeline against a stored dataset."""

    await get_project_or_404(
        session, project_id=dataset.project_id, user=actor, require_write=True
    )
    stored_path = Path(dataset.stored_path)
    if not stored_path.exists():
        raise ApiException(
            status_code=404,
            code="dataset_file_missing",
            message="Stored dataset file was not found.",
        )

    with stored_path.open("rb") as handle:
        sniff_bytes = handle.read(50000)
    dataframe, encoding = await asyncio.to_thread(
        analysis_service._load_dataframe, stored_path, sniff_bytes
    )
    analysis_payload = await asyncio.to_thread(
        analysis_service.build_analysis_payload, dataframe
    )

    dataset.encoding = encoding
    dataset.row_count = int(len(dataframe))
    dataset.column_count = int(len(dataframe.columns))
    dataset.detected_domain = analysis_payload["domain_detection"]["primary_domain"]
    dataset.analysis_payload = analysis_payload

    await log_audit_event(
        session,
        action="dataset.reanalyzed",
        resource_type="dataset",
        resource_id=str(dataset.id),
        user_id=actor.id,
        ip_address=ip_address,
        payload={
            "project_id": dataset.project_id,
            "filename": dataset.original_filename,
        },
    )
    await session.commit()
    await session.refresh(dataset)
    return dataset


def build_upload_response(dataset: DatasetAsset) -> dict[str, Any]:
    """Project stored analysis into the upload response shape."""

    payload = dataset.analysis_payload or {}
    domain = payload.get("domain_detection", {})
    return {
        "dataset_id": dataset.id,
        "filename": dataset.original_filename,
        "file_size": dataset.file_size_bytes,
        "row_count": dataset.row_count,
        "column_count": dataset.column_count,
        "detected_domain": dataset.detected_domain,
        "domain_confidence": domain.get("confidence"),
        "columns_schema": payload.get("schema_mapping", {}).get("columns", []),
        "data_preview": payload.get("data_preview", {}),
    }


def build_preview_response(dataset: DatasetAsset) -> dict[str, Any]:
    """Return the detailed preview payload."""

    preview = (dataset.analysis_payload or {}).get("data_preview", {})
    return {
        "dataset_id": dataset.id,
        "filename": dataset.original_filename,
        "sample_rows": preview.get("sample_rows", []),
        "column_headers": preview.get("column_headers", []),
        "column_statistics": preview.get("column_statistics", {}),
        "data_quality_metrics": preview.get("data_quality_metrics", {}),
        "mini_histograms": preview.get("mini_histograms", {}),
        "missing_data_heatmap": preview.get("missing_data_heatmap", []),
        "correlation_matrix": preview.get("correlation_matrix", []),
        "value_frequency_distribution": preview.get("value_frequency_distribution", {}),
    }


def build_statistics_response(dataset: DatasetAsset) -> dict[str, Any]:
    """Return comprehensive statistics for one dataset."""

    payload = dataset.analysis_payload or {}
    preview = payload.get("data_preview", {})
    return {
        "dataset_id": dataset.id,
        "filename": dataset.original_filename,
        "row_count": dataset.row_count,
        "column_count": dataset.column_count,
        "columns_schema": payload.get("schema_mapping", {}).get("columns", []),
        "statistical_summary": preview.get("statistical_summary", {}),
        "statistical_analysis": payload.get("statistical_analysis", {}),
        "quality_report": payload.get("quality_report", {}),
    }


def build_insights_response(dataset: DatasetAsset) -> dict[str, Any]:
    """Return structured AI insights."""

    payload = dataset.analysis_payload or {}
    return {
        "dataset_id": dataset.id,
        "filename": dataset.original_filename,
        "ai_insights": payload.get("ai_insights", {}),
    }


def build_full_analysis_response(dataset: DatasetAsset) -> dict[str, Any]:
    """Return the full dataset analysis response."""

    payload = dataset.analysis_payload or {}
    preview = payload.get("data_preview", {})
    domain = payload.get("domain_detection", {})
    return {
        "dataset_id": str(dataset.id),
        "filename": dataset.original_filename,
        "file_size": dataset.file_size_bytes,
        "row_count": dataset.row_count,
        "column_count": dataset.column_count,
        "detected_domain": dataset.detected_domain,
        "domain_confidence": domain.get("confidence"),
        "columns_schema": payload.get("schema_mapping", {}).get("columns", []),
        "recommended_kpis": payload.get("recommended_kpis", []),
        "chart_recommendations": payload.get("chart_recommendations", []),
        "dashboard_layout": payload.get("dashboard_blueprint", {}),
        "data_preview": preview,
        "ai_insights": payload.get("ai_insights", {}),
    }
