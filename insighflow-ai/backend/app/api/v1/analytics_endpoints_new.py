"""New API endpoints for analytics, query, and visualization services."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db
from app.models.user import User
from app.services.analytics_engine import AnalyticsEngine
from app.services.chart_generator import ChartGenerator
from app.services.nl_to_sql import NLToSQLEngine, QueryExecutor
from app.services.profiling import DataProfiler, SchemaDetector

router = APIRouter(prefix="/analytics", tags=["analytics-v2"])


class QueryRequest(BaseModel):
    """Natural language query request."""

    dataset_id: int = Field(..., description="Dataset ID to query")
    message: str = Field(..., description="Natural language query")


class QueryResponse(BaseModel):
    """Query response with results and chart config."""

    status: str
    message: str | None = None
    sql_query: str | None = None
    columns: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    chart_config: dict[str, Any] | None = None
    explanation: str | None = None
    cached: bool = False


class ProfileRequest(BaseModel):
    """Data profiling request."""

    dataset_id: int = Field(..., description="Dataset ID to profile")


class ProfileResponse(BaseModel):
    """Profile response."""

    status: str
    profile: dict[str, Any] | None = None
    schema: list[dict[str, Any]] | None = None
    suggestions: list[str] | None = None


class AnomalyRequest(BaseModel):
    """Anomaly detection request."""

    dataset_id: int = Field(..., description="Dataset ID")
    column: str | None = Field(None, description="Column to analyze")
    method: str = Field("iqr", description="Detection method: iqr, zscore, or both")
    threshold: float = Field(1.5, description="Threshold for detection")


class AnomalyResponse(BaseModel):
    """Anomaly detection response."""

    status: str
    column: str | None = None
    anomalies: dict[str, Any] | None = None
    summary: dict[str, Any] | None = None


class CorrelationRequest(BaseModel):
    """Correlation analysis request."""

    dataset_id: int = Field(..., description="Dataset ID")
    columns: list[str] | None = Field(None, description="Columns to analyze")


class CorrelationResponse(BaseModel):
    """Correlation response."""

    status: str
    columns: list[str] | None = None
    matrix: dict[str, Any] | None = None
    top_correlations: list[dict[str, Any]] | None = None
    summary: dict[str, Any] | None = None


class ForecastRequest(BaseModel):
    """Forecast request."""

    dataset_id: int = Field(..., description="Dataset ID")
    value_column: str = Field(..., description="Value column to forecast")
    time_column: str | None = Field(None, description="Time column for ordering")
    periods: int = Field(5, description="Number of periods to forecast")


class ForecastResponse(BaseModel):
    """Forecast response."""

    status: str
    historical: list[float] | None = None
    forecast: list[float] | None = None
    trend: float | None = None
    summary: dict[str, Any] | None = None


class ChartRequest(BaseModel):
    """Chart generation request."""

    data: list[dict[str, Any]] = Field(..., description="Data for chart")
    columns: list[str] | None = Field(None, description="Columns to include")
    chart_type: str | None = Field(None, description="Chart type override")


class ChartResponse(BaseModel):
    """Chart configuration response."""

    status: str
    chart_config: dict[str, Any]


class InsightsResponse(BaseModel):
    """Automated insights response."""

    status: str
    insights: list[dict[str, Any]] | None = None
    dataset_summary: dict[str, Any] | None = None


@router.post("/query", response_model=QueryResponse)
async def execute_nl_query(
    request: QueryRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> QueryResponse:
    """Execute natural language query on dataset."""
    try:
        engine = NLToSQLEngine(db, request.dataset_id)
        result = await engine.execute(request.message)

        if result.get("status") == "error":
            return QueryResponse(
                status="error",
                message=result.get("message", "Query execution failed"),
            )

        chart_config = None
        if result.get("rows"):
            chart_config = ChartGenerator.generate_chart_config(
                result["rows"],
                result.get("columns", []),
            )

        return QueryResponse(
            status="success",
            sql_query=result.get("sql_query"),
            columns=result.get("columns", []),
            rows=result.get("rows", []),
            row_count=result.get("row_count", len(result.get("rows", []))),
            chart_config=chart_config,
            explanation=result.get("explanation"),
            cached=result.get("cached", False),
        )
    except Exception as e:
        return QueryResponse(
            status="error",
            message=str(e),
        )


@router.post("/profile", response_model=ProfileResponse)
async def profile_dataset(
    request: ProfileRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> ProfileResponse:
    """Profile dataset and detect schema."""
    try:
        from app.services.storage_engine import PostgresStorageEngine

        storage = PostgresStorageEngine(db)
        sample = await storage.sample_data(request.dataset_id, 1000)

        if not sample:
            return ProfileResponse(
                status="error",
                profile=None,
                schema=None,
                suggestions=[],
            )

        profiler = DataProfiler(sample)
        profile = profiler.get_summary()

        schema = SchemaDetector.detect_schema(sample)
        suggestions = SchemaDetector.suggest_aggregations(schema)

        return ProfileResponse(
            status="success",
            profile=profile,
            schema=schema,
            suggestions=suggestions,
        )
    except Exception as e:
        return ProfileResponse(
            status="error",
            message=str(e),
        )


@router.post("/anomalies", response_model=AnomalyResponse)
async def detect_anomalies(
    request: AnomalyRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> AnomalyResponse:
    """Detect anomalies in dataset."""
    try:
        engine = AnalyticsEngine(db, request.dataset_id)
        result = await engine.detect_anomalies(
            column=request.column,
            method=request.method,
            threshold=request.threshold,
        )

        if result.get("status") == "error":
            return AnomalyResponse(
                status="error",
                message=result.get("message"),
            )

        return AnomalyResponse(
            status="success",
            column=result.get("column"),
            anomalies=result.get("anomalies"),
            summary=result.get("summary"),
        )
    except Exception as e:
        return AnomalyResponse(
            status="error",
            message=str(e),
        )


@router.post("/correlation", response_model=CorrelationResponse)
async def calculate_correlation(
    request: CorrelationRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CorrelationResponse:
    """Calculate correlation matrix."""
    try:
        engine = AnalyticsEngine(db, request.dataset_id)
        result = await engine.calculate_correlation(columns=request.columns)

        if result.get("status") == "error":
            return CorrelationResponse(
                status="error",
                message=result.get("message"),
            )

        return CorrelationResponse(
            status="success",
            columns=result.get("columns"),
            matrix=result.get("matrix"),
            top_correlations=result.get("top_correlations"),
            summary=result.get("summary"),
        )
    except Exception as e:
        return CorrelationResponse(
            status="error",
            message=str(e),
        )


@router.post("/forecast", response_model=ForecastResponse)
async def generate_forecast(
    request: ForecastRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> ForecastResponse:
    """Generate forecast for time series data."""
    try:
        engine = AnalyticsEngine(db, request.dataset_id)
        result = await engine.forecast(
            value_column=request.value_column,
            time_column=request.time_column,
            periods=request.periods,
        )

        if result.get("status") == "error":
            return ForecastResponse(
                status="error",
                message=result.get("message"),
            )

        return ForecastResponse(
            status="success",
            historical=result.get("historical"),
            forecast=result.get("forecast"),
            trend=result.get("trend"),
            summary=result.get("summary"),
        )
    except Exception as e:
        return ForecastResponse(
            status="error",
            message=str(e),
        )


@router.post("/chart", response_model=ChartResponse)
async def generate_chart(
    request: ChartRequest,
    current_user: User = Depends(get_current_user),
) -> ChartResponse:
    """Generate chart configuration from data."""
    try:
        chart_config = ChartGenerator.generate_chart_config(
            request.data,
            request.columns,
            request.chart_type,
        )

        return ChartResponse(
            status="success",
            chart_config=chart_config,
        )
    except Exception as e:
        return ChartResponse(
            status="error",
            chart_config=ChartGenerator._empty_config(),
        )


@router.get("/chart-types")
async def get_chart_types(
    current_user: User = Depends(get_current_user),
) -> dict[str, Any]:
    """Get available chart types."""
    return {
        "chart_types": ChartGenerator.get_available_chart_types(),
    }


@router.get("/insights/{dataset_id}", response_model=InsightsResponse)
async def get_insights(
    dataset_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> InsightsResponse:
    """Get automated insights for dataset."""
    try:
        engine = AnalyticsEngine(db, dataset_id)
        result = await engine.get_insights()

        return InsightsResponse(
            status=result.get("status", "success"),
            insights=result.get("insights"),
            dataset_summary=result.get("dataset_summary"),
        )
    except Exception as e:
        return InsightsResponse(
            status="error",
            message=str(e),
        )


@router.get("/sample/{dataset_id}")
async def get_sample_data(
    dataset_id: int,
    limit: int = Query(100, ge=1, le=1000),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Get sample data from dataset."""
    try:
        executor = QueryExecutor(db, dataset_id)
        result = await executor.get_sample_data(limit)
        return result
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "rows": [],
        }


@router.get("/schema/{dataset_id}")
async def get_dataset_schema(
    dataset_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Get dataset schema."""
    try:
        from app.services.storage_engine import PostgresStorageEngine

        storage = PostgresStorageEngine(db)
        columns = await storage.get_table_columns(dataset_id)
        row_count = await storage.get_table_row_count(dataset_id)

        return {
            "status": "success",
            "dataset_id": dataset_id,
            "columns": columns,
            "row_count": row_count,
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
        }
