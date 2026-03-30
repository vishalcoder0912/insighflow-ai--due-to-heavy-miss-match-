"""Dataset analysis schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.common import TimestampedModel


class ColumnProfile(BaseModel):
    """Profile for a single dataset column."""

    name: str
    inferred_type: str
    subtype: str | None = None
    unique_count: int
    missing_percentage: float
    cardinality: str
    sample_values: list[str]
    stats: dict[str, Any] = Field(default_factory=dict)


class KPIRecommendation(BaseModel):
    """Recommended KPI card."""

    id: str
    title: str
    description: str
    value: str | None = None
    format_hint: str | None = None
    priority: int
    business_impact: str
    source_columns: list[str]
    recommended_visual: str


class ChartRecommendation(BaseModel):
    """Recommended chart component."""

    id: str
    title: str
    chart_type: str
    x_field: str | None = None
    y_field: str | None = None
    aggregation: str | None = None
    rationale: str


class DashboardBlueprint(BaseModel):
    """Frontend render blueprint."""

    layout_system: str
    filters: list[dict[str, Any]] = Field(default_factory=list)
    theme: dict[str, Any] = Field(default_factory=dict)
    components: list[dict[str, Any]] = Field(default_factory=list)


class DatasetAnalysisResponse(TimestampedModel):
    """Persisted dataset analysis response."""

    project_id: int
    uploaded_by_id: int | None
    original_filename: str
    file_format: str
    encoding: str | None
    file_size_bytes: int
    row_count: int
    column_count: int
    detected_domain: str | None
    analysis_payload: dict[str, Any]


class DatasetUploadResponse(BaseModel):
    """Upload response with preview metadata."""

    model_config = ConfigDict(populate_by_name=True, protected_namespaces=())

    dataset_id: int
    filename: str
    file_size: int
    row_count: int
    column_count: int
    detected_domain: str | None
    domain_confidence: float | None = None
    columns_schema: list[dict[str, Any]] = Field(
        default_factory=list, serialization_alias="schema"
    )
    data_preview: dict[str, Any] = Field(default_factory=dict)


class DatasetPreviewResponse(BaseModel):
    """Dataset preview payload."""

    dataset_id: int
    filename: str
    sample_rows: list[dict[str, Any]] = Field(default_factory=list)
    column_headers: list[dict[str, Any]] = Field(default_factory=list)
    column_statistics: dict[str, Any] = Field(default_factory=dict)
    data_quality_metrics: dict[str, Any] = Field(default_factory=dict)
    mini_histograms: dict[str, Any] = Field(default_factory=dict)
    missing_data_heatmap: list[dict[str, Any]] = Field(default_factory=list)
    correlation_matrix: list[dict[str, Any]] = Field(default_factory=list)
    value_frequency_distribution: dict[str, Any] = Field(default_factory=dict)


class DatasetStatisticsResponse(BaseModel):
    """Comprehensive dataset statistics."""

    model_config = ConfigDict(populate_by_name=True, protected_namespaces=())

    dataset_id: int
    filename: str
    row_count: int
    column_count: int
    columns_schema: list[dict[str, Any]] = Field(
        default_factory=list, serialization_alias="schema"
    )
    statistical_summary: dict[str, Any] = Field(default_factory=dict)
    statistical_analysis: dict[str, Any] = Field(default_factory=dict)
    quality_report: dict[str, Any] = Field(default_factory=dict)


class DatasetInsightsResponse(BaseModel):
    """AI-generated insights."""

    dataset_id: int
    filename: str
    ai_insights: dict[str, Any] = Field(default_factory=dict)


class DatasetFullAnalysisResponse(BaseModel):
    """Full structured analysis payload."""

    model_config = ConfigDict(populate_by_name=True, protected_namespaces=())

    dataset_id: str
    filename: str
    file_size: int
    row_count: int
    column_count: int
    detected_domain: str | None
    domain_confidence: float | None = None
    columns_schema: list[dict[str, Any]] = Field(
        default_factory=list, serialization_alias="schema"
    )
    recommended_kpis: list[dict[str, Any]] = Field(default_factory=list)
    chart_recommendations: list[dict[str, Any]] = Field(default_factory=list)
    dashboard_layout: dict[str, Any] = Field(default_factory=dict)
    data_preview: dict[str, Any] = Field(default_factory=dict)
    ai_insights: dict[str, Any] = Field(default_factory=dict)
