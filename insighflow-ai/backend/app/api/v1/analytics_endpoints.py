"""Quality, ML, Reports, and Scheduler API endpoints."""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db
from app.models.user import User
from app.services.quality import assess_data_quality
from app.services.ml_engine import (
    train_classification_model,
    train_clustering_model,
    train_regression_model,
)
from app.services.statistics import run_anova, run_correlation, run_ttest
from app.services.reports import generate_excel_report, generate_pdf_report

router = APIRouter(tags=["analytics"])


class QualityRequest(BaseModel):
    """Data quality assessment request."""

    data: list[dict[str, Any]] = Field(..., description="Input data")


class QualityResponse(BaseModel):
    """Quality response."""

    quality_score: int
    total_issues: int
    issues_by_severity: dict[str, int]
    issues: list[dict[str, Any]]
    recommendations: list[dict[str, Any]]
    dataset_info: dict[str, Any]


class MLRegressionRequest(BaseModel):
    """ML regression request."""

    data: list[dict[str, Any]] = Field(..., description="Input data")
    target_column: str = Field(..., description="Target column to predict")
    model_type: str = Field("linear_regression", description="Model type")
    feature_columns: Optional[list[str]] = Field(None, description="Feature columns")


class MLClassificationRequest(BaseModel):
    """ML classification request."""

    data: list[dict[str, Any]] = Field(..., description="Input data")
    target_column: str = Field(..., description="Target column to predict")
    model_type: str = Field("logistic_regression", description="Model type")
    feature_columns: Optional[list[str]] = Field(None, description="Feature columns")


class MLClusteringRequest(BaseModel):
    """ML clustering request."""

    data: list[dict[str, Any]] = Field(..., description="Input data")
    n_clusters: int = Field(3, description="Number of clusters")
    feature_columns: Optional[list[str]] = Field(None, description="Feature columns")
    algorithm: str = Field("kmeans", description="Clustering algorithm")


class StatisticalRequest(BaseModel):
    """Statistical test request."""

    data: list[dict[str, Any]] = Field(..., description="Input data")
    group1: str = Field(..., description="First group/column")
    group2: Optional[str] = Field(None, description="Second group/column")
    method: Optional[str] = Field("pearson", description="Test method")


class ReportRequest(BaseModel):
    """Report generation request."""

    data: list[dict[str, Any]] = Field(..., description="Input data")
    title: str = Field("Data Report", description="Report title")
    include_summary: bool = Field(True, description="Include summary")
    include_statistics: bool = Field(True, description="Include statistics")


@router.post("/quality", response_model=QualityResponse, summary="Assess data quality")
async def assess_quality(request: QualityRequest) -> QualityResponse:
    """Assess data quality and generate report."""
    result = assess_data_quality(request.data)
    return QualityResponse(**result)


@router.post("/ml/regression", summary="Train regression model")
async def train_regression(
    request: MLRegressionRequest,
) -> dict[str, Any]:
    """Train a regression model."""
    return train_regression_model(
        data=request.data,
        target_column=request.target_column,
        model_type=request.model_type,
        feature_columns=request.feature_columns,
    )


@router.post("/ml/classification", summary="Train classification model")
async def train_classification(
    request: MLClassificationRequest,
) -> dict[str, Any]:
    """Train a classification model."""
    return train_classification_model(
        data=request.data,
        target_column=request.target_column,
        model_type=request.model_type,
        feature_columns=request.feature_columns,
    )


@router.post("/ml/clustering", summary="Train clustering model")
async def train_clustering(
    request: MLClusteringRequest,
) -> dict[str, Any]:
    """Train a clustering model."""
    return train_clustering_model(
        data=request.data,
        n_clusters=request.n_clusters,
        feature_columns=request.feature_columns,
        algorithm=request.algorithm,
    )


@router.post("/statistics/ttest", summary="Run t-test")
async def run_ttest_endpoint(request: StatisticalRequest) -> dict[str, Any]:
    """Run independent t-test."""
    return run_ttest(request.data, request.group1, request.group2)


@router.post("/statistics/anova", summary="Run ANOVA")
async def run_anova_endpoint(request: StatisticalRequest) -> dict[str, Any]:
    """Run one-way ANOVA."""
    groups = (
        request.group1.split(",")
        if isinstance(request.group1, str)
        else [request.group1]
    )
    return run_anova(request.data, groups)


@router.post("/statistics/correlation", summary="Run correlation test")
async def run_correlation_endpoint(
    request: StatisticalRequest,
) -> dict[str, Any]:
    """Run correlation test."""
    return run_correlation(
        request.data,
        request.group1,
        request.group2 or request.group1,
        request.method or "pearson",
    )


@router.post("/reports/excel", summary="Generate Excel report")
async def generate_excel(
    request: ReportRequest,
) -> Response:
    """Generate Excel report."""
    try:
        excel_data = generate_excel_report(request.data, request.title)
        return Response(
            content=excel_data,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={request.title}.xlsx"
            },
        )
    except ImportError as e:
        return Response(content=str(e).encode(), status_code=500)


@router.post("/reports/pdf", summary="Generate PDF report")
async def generate_pdf(
    request: ReportRequest,
) -> Response:
    """Generate PDF report."""
    try:
        pdf_data = generate_pdf_report(request.data, request.title)
        return Response(
            content=pdf_data,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"attachment; filename={request.title}.pdf"
            },
        )
    except ImportError as e:
        return Response(content=str(e).encode(), status_code=500)
