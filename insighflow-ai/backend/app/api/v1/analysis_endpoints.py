"""Analysis API endpoints."""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db
from app.models.user import User
from app.services.analysis_engine import (
    AnalysisEngine,
    analyze_correlations,
    analyze_summary,
    analyze_trends,
    detect_anomalies,
    forecast_data,
    segment_data,
)

router = APIRouter(prefix="/analysis", tags=["analysis"])


class AnalysisRequest(BaseModel):
    """Analysis request."""

    data: list[dict[str, Any]] = Field(..., description="Input data")


class SummaryResponse(BaseModel):
    """Summary statistics response."""

    result: dict[str, Any]


class TrendRequest(BaseModel):
    """Trend analysis request."""

    data: list[dict[str, Any]] = Field(..., description="Input data")
    column: Optional[str] = Field(None, description="Target column")
    date_column: Optional[str] = Field(None, description="Date column for time series")


class CorrelationResponse(BaseModel):
    """Correlation analysis response."""

    result: dict[str, Any]


class AnomalyRequest(BaseModel):
    """Anomaly detection request."""

    data: list[dict[str, Any]] = Field(..., description="Input data")
    column: Optional[str] = Field(None, description="Target column")
    method: str = Field(
        "iqr", description="Detection method: iqr, zscore, isolation_forest"
    )


class SegmentationRequest(BaseModel):
    """Segmentation request."""

    data: list[dict[str, Any]] = Field(..., description="Input data")
    n_clusters: int = Field(3, description="Number of clusters")


class ForecastRequest(BaseModel):
    """Forecasting request."""

    data: list[dict[str, Any]] = Field(..., description="Input data")
    column: Optional[str] = Field(None, description="Target column")
    date_column: Optional[str] = Field(None, description="Date column")
    periods: int = Field(5, description="Number of periods to forecast")


@router.post(
    "/summary", response_model=SummaryResponse, summary="Get summary statistics"
)
async def get_summary(
    request: AnalysisRequest,
) -> SummaryResponse:
    """Generate summary statistics for the dataset."""
    result = analyze_summary(request.data)
    return SummaryResponse(result=result)


@router.post("/trends", response_model=SummaryResponse, summary="Analyze trends")
async def get_trends(
    request: TrendRequest,
) -> SummaryResponse:
    """Analyze trends in the data."""
    result = analyze_trends(request.data, request.column, request.date_column)
    return SummaryResponse(result=result)


@router.post(
    "/correlation", response_model=CorrelationResponse, summary="Get correlation matrix"
)
async def get_correlation(
    request: AnalysisRequest,
) -> CorrelationResponse:
    """Generate correlation matrix for numeric columns."""
    result = analyze_correlations(request.data)
    return CorrelationResponse(result=result)


@router.post("/anomalies", response_model=SummaryResponse, summary="Detect anomalies")
async def get_anomalies(
    request: AnomalyRequest,
) -> SummaryResponse:
    """Detect anomalies in the data."""
    result = detect_anomalies(request.data, request.column, request.method)
    return SummaryResponse(result=result)


@router.post("/segmentation", response_model=SummaryResponse, summary="Segment data")
async def segment(
    request: SegmentationRequest,
) -> SummaryResponse:
    """Perform K-Means clustering for segmentation."""
    result = segment_data(request.data, request.n_clusters)
    return SummaryResponse(result=result)


@router.post("/forecast", response_model=SummaryResponse, summary="Forecast values")
async def forecast(
    request: ForecastRequest,
) -> SummaryResponse:
    """Generate forecasts using linear regression."""
    result = forecast_data(
        request.data, request.column, request.date_column, request.periods
    )
    return SummaryResponse(result=result)


@router.post("/profile", response_model=SummaryResponse, summary="Profile data")
async def profile_data(
    request: AnalysisRequest,
) -> SummaryResponse:
    """Generate comprehensive data profile."""
    from app.services.profiling import profile_data as do_profile

    result = do_profile(request.data)
    return SummaryResponse(result=result)
