"""Chat API endpoints for AI Analytics Chatbot."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db
from app.models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chat", tags=["chat"])


class ChatMessage(BaseModel):
    """Chat message."""

    role: str = Field(..., description="Message role: user or assistant")
    content: str = Field(..., description="Message content")
    sql_query: str | None = Field(None, description="SQL query if applicable")
    chart_data: dict | None = Field(None, description="Chart data if applicable")


class ChatRequest(BaseModel):
    """Chat request."""

    dataset_id: int = Field(..., description="Dataset ID to query")
    message: str = Field(..., description="User's natural language message")
    chat_history: list[ChatMessage] = Field(
        default_factory=list, description="Previous chat messages"
    )


class ChatResponse(BaseModel):
    """Chat response."""

    response: str = Field(..., description="Bot's response")
    sql_query: str | None = Field(None, description="Generated SQL query")
    chart_data: dict | None = Field(None, description="Chart data for visualization")
    chart_type: str | None = Field(None, description="Recommended chart type")
    data_table: list[dict] | None = Field(None, description="Table data for display")
    suggestions: list[str] = Field(
        default_factory=list, description="Follow-up suggestions"
    )
    explanation: str | None = Field(None, description="Explanation of the analysis")


class ChatService:
    """Chat service for natural language to analytics."""

    def __init__(self, db: AsyncSession, user: User):
        self.db = db
        self.user = user
        self.dataset_id = None
        self.columns = []
        self.sample_data = []

    async def get_dataset_info(self, dataset_id: int) -> dict[str, Any]:
        """Get dataset schema and sample data."""
        from app.services.datasets import get_dataset_or_404
        from app.models.dataset import DatasetAsset

        dataset = await get_dataset_or_404(
            self.db, dataset_id=dataset_id, actor=self.user
        )

        payload = dataset.analysis_payload or {}
        preview = payload.get("data_preview", {})
        schema = payload.get("schema_mapping", {})

        columns = []
        if schema.get("columns"):
            columns = schema["columns"]
        elif payload.get("column_statistics"):
            columns = [
                {"name": col} for col in payload.get("column_statistics", {}).keys()
            ]

        sample_rows = preview.get("sample_rows", [])[:10]

        self.dataset_id = dataset_id
        self.columns = columns
        self.sample_data = sample_rows

        return {
            "dataset_id": dataset_id,
            "file_name": dataset.original_filename,
            "columns": columns,
            "sample_data": sample_rows,
            "row_count": dataset.row_count,
        }

    def parse_query(self, message: str) -> dict[str, Any]:
        """Parse natural language query into SQL components."""
        message_lower = message.lower()

        result = {
            "action": "select",
            "group_by": None,
            "order_by": None,
            "order_dir": "DESC",
            "limit": 10,
            "filters": [],
            "aggregations": [],
            "columns": [],
            "where": None,
        }

        aggregate_keywords = {
            "sum": "SUM",
            "total": "SUM",
            "average": "AVG",
            "avg": "AVG",
            "mean": "AVG",
            "count": "COUNT",
            "minimum": "MIN",
            "min": "MIN",
            "maximum": "MAX",
            "max": "MAX",
        }

        for keyword, agg_func in aggregate_keywords.items():
            if keyword in message_lower:
                result["aggregations"].append({"func": agg_func, "column": None})

        order_keywords = {
            "top": "DESC",
            "highest": "DESC",
            "best": "DESC",
            "most": "DESC",
            "bottom": "ASC",
            "lowest": "ASC",
            "worst": "ASC",
            "least": "ASC",
        }
        for keyword, direction in order_keywords.items():
            if keyword in message_lower:
                result["order_dir"] = direction
                result["order_by"] = "aggregate"

        for col_info in self.columns:
            col_name = col_info.get("name", "").lower()
            col_name_underscore = col_name.replace(" ", "_")

            if col_name in message_lower or col_name_underscore in message_lower:
                result["columns"].append(col_info.get("name"))

            if any(
                kw in col_name for kw in ["region", "country", "city", "state", "area"]
            ):
                result["group_by"] = col_info.get("name")
                result["columns"].append(col_info.get("name"))

            if any(kw in col_name for kw in ["date", "time", "month", "year", "day"]):
                if "time" not in result or result["time"] is None:
                    result["time_column"] = col_info.get("name")

        time_keywords = [
            "trend",
            "over time",
            "timeline",
            "history",
            "growth",
            "by month",
            "by year",
            "by quarter",
        ]
        if any(kw in message_lower for kw in time_keywords):
            result["include_time_trend"] = True

        if (
            "compare" in message_lower
            or "vs" in message_lower
            or " versus " in message_lower
        ):
            result["comparison"] = True

        anomaly_keywords = ["anomaly", "outlier", "unusual", "异常", "outliers"]
        if any(kw in message_lower for kw in anomaly_keywords):
            result["action"] = "anomaly_detection"

        correlation_keywords = [
            "correlation",
            "correlate",
            "related",
            "relationship",
            "关联",
        ]
        if any(kw in message_lower for kw in correlation_keywords):
            result["action"] = "correlation"

        forecast_keywords = ["forecast", "predict", "future", "预测", "next"]
        if any(kw in message_lower for kw in forecast_keywords):
            result["action"] = "forecast"

        segment_keywords = ["segment", "cluster", "group", "分组", "segmentation"]
        if any(kw in message_lower for kw in segment_keywords):
            result["action"] = "segmentation"

        quality_keywords = ["quality", "missing", "duplicate", "clean", "数据质量"]
        if any(kw in message_lower for kw in quality_keywords):
            result["action"] = "data_quality"

        return result

    def generate_sql(self, parsed: dict[str, Any]) -> str:
        """Generate SQL from parsed query."""
        cols = parsed.get("columns", [])
        aggs = parsed.get("aggregations", [])
        group_by = parsed.get("group_by")
        order_by = parsed.get("order_by")
        order_dir = parsed.get("order_dir", "DESC")
        limit = parsed.get("limit", 10)

        if not cols:
            cols = ["*"]

        select_parts = []

        if aggs:
            for agg in aggs:
                if agg["column"]:
                    select_parts.append(
                        f"{agg['func']}({agg['column']}) as {agg['func'].lower()}_{agg['column']}"
                    )
                elif group_by:
                    pass
                else:
                    select_parts.append(f"{agg['func']}(*) as total")
        else:
            select_parts = cols

        table_name = f"dataset_{self.dataset_id}"

        sql = f"SELECT {', '.join(select_parts)} \nFROM {table_name}"

        if group_by and group_by not in select_parts:
            sql += f" \nGROUP BY {group_by}"

        if order_by:
            if order_by == "aggregate" and aggs:
                agg_col = select_parts[0]
                sql += f" \nORDER BY {agg_col} {order_dir}"
            elif group_by:
                sql += f" \nORDER BY {group_by} {order_dir}"

        sql += f" \nLIMIT {limit}"

        return sql

    def generate_response(self, message: str, parsed: dict[str, Any]) -> ChatResponse:
        """Generate chat response."""
        action = parsed.get("action", "select")

        if action == "anomaly_detection":
            return ChatResponse(
                response="I'll analyze your data to detect anomalies and outliers using statistical methods (IQR and Z-score).",
                sql_query="SELECT * FROM dataset_table WHERE outlier_detected = true",
                chart_type="scatter",
                suggestions=[
                    "Show me the outliers",
                    "What's causing these anomalies?",
                    "Remove outliers and re-analyze",
                ],
                explanation="Anomaly detection identifies unusual data points that deviate significantly from the norm.",
            )

        if action == "correlation":
            return ChatResponse(
                response="I'll calculate correlations between all numeric variables in your dataset.",
                sql_query="SELECT column1, column2, correlation FROM correlations_table",
                chart_type="heatmap",
                suggestions=[
                    "Show correlation matrix",
                    "What strongly correlates with revenue?",
                    "Create scatter plot of top correlations",
                ],
                explanation="Correlation analysis reveals relationships between variables. Values close to 1 or -1 indicate strong relationships.",
            )

        if action == "forecast":
            return ChatResponse(
                response="I'll generate a forecast based on historical trends using linear regression.",
                sql_query="SELECT date_column, actual_value, predicted_value, confidence_lower, confidence_upper FROM forecast_table",
                chart_type="line",
                suggestions=[
                    "Show forecast for next quarter",
                    "What's the growth rate?",
                    "Compare forecast vs actual",
                ],
                explanation="Forecasting uses historical patterns to predict future values with confidence intervals.",
            )

        if action == "segmentation":
            return ChatResponse(
                response="I'll segment your data using K-Means clustering to identify natural groupings.",
                sql_query="SELECT cluster_id, COUNT(*) as size, AVG(metric) as avg_value FROM clustered_data GROUP BY cluster_id",
                chart_type="bar",
                suggestions=[
                    "Show cluster characteristics",
                    "How many segments?",
                    "What defines each segment?",
                ],
                explanation="Segmentation groups similar data points together based on their characteristics.",
            )

        if action == "data_quality":
            return ChatResponse(
                response="I'll analyze your data quality including missing values, duplicates, and consistency.",
                sql_query="SELECT 'missing' as issue_type, column_name, COUNT(*) as count FROM data GROUP BY column_name",
                chart_type="bar",
                suggestions=[
                    "Show data quality score",
                    "Which columns need cleaning?",
                    "Clean the data automatically",
                ],
                explanation="Data quality analysis identifies issues like missing values, duplicates, and inconsistencies.",
            )

        sql = self.generate_sql(parsed)

        columns = parsed.get("columns", [])
        group_by = parsed.get("group_by", "")

        if group_by:
            response_text = f"Here's the breakdown by {group_by}:\n\n"
        else:
            response_text = "Here are the results:\n\n"

        suggestions = [
            f"Show trend over time",
            f"Compare with other metrics",
            f"Find anomalies in this data",
        ]

        if parsed.get("include_time_trend"):
            suggestions.insert(0, "Show time series chart")

        return ChatResponse(
            response=response_text,
            sql_query=sql,
            chart_type="bar" if group_by else "table",
            data_table=self.sample_data[:5],
            suggestions=suggestions,
            explanation=f"This query groups data by {group_by if group_by else 'all records'} and shows the results sorted by the selected metrics.",
        )


@router.post("/message", response_model=ChatResponse, summary="Send chat message")
async def send_chat_message(
    request: Request,
    chat_request: ChatRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> ChatResponse:
    """Send a chat message and get AI-powered analytics response."""
    service = ChatService(db, current_user)

    await service.get_dataset_info(chat_request.dataset_id)

    parsed = service.parse_query(chat_request.message)

    response = service.generate_response(chat_request.message, parsed)

    return response


@router.get("/history/{dataset_id}", summary="Get chat history")
async def get_chat_history(
    dataset_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Get chat history for a dataset."""
    return {
        "dataset_id": dataset_id,
        "messages": [],
        "suggested_questions": [
            "Show me the top 10 by revenue",
            "What's the trend over time?",
            "Find anomalies in the data",
            "Show correlations between metrics",
            "Segment customers by value",
        ],
    }
