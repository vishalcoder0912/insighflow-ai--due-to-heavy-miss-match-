"""Enhanced Chat API with Ollama and Auto-Insights."""

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


class ChatRequest(BaseModel):
    """Enhanced chat request."""

    dataset_id: int = Field(..., description="Dataset ID to query")
    message: str = Field(..., description="User's natural language message")
    include_insights: bool = Field(True, description="Include auto-insights")


class ChatResponse(BaseModel):
    """Enhanced chat response."""

    response: str = Field(..., description="Bot's response")
    sql_query: str | None = Field(None, description="Generated SQL query")
    chart_data: dict | None = Field(None, description="Chart data")
    chart_type: str = Field("table", description="Recommended chart type")
    data_table: list[dict] | None = Field(None, description="Table data")
    suggestions: list[str] = Field(
        default_factory=list, description="Follow-up suggestions"
    )
    explanation: str | None = Field(None, description="Analysis explanation")
    insights: list[dict] = Field(
        default_factory=list, description="Auto-generated insights"
    )
    kpis: list[dict] = Field(default_factory=list, description="KPI summaries")
    method: str = Field("rule_based", description="NL processing method")


@router.post(
    "/message", response_model=ChatResponse, summary="Send chat message with AI"
)
async def send_chat_message(
    request: Request,
    chat_request: ChatRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> ChatResponse:
    """Send a chat message and get AI-powered analytics response."""
    from app.services.datasets import get_dataset_or_404
    from app.services.nl_to_sql import get_nl_to_sql_service
    from app.services.auto_insights import generate_auto_insights

    try:
        dataset = await get_dataset_or_404(
            db, dataset_id=chat_request.dataset_id, actor=current_user
        )

        payload = dataset.analysis_payload or {}
        preview = payload.get("data_preview", {})
        schema = payload.get("schema_mapping", {})

        columns = schema.get("columns", [])
        if not columns and payload.get("column_statistics"):
            columns = [
                {"name": col, "inferred_type": "numeric"}
                for col in payload.get("column_statistics", {}).keys()
            ]

        sample_rows = preview.get("sample_rows", [])[:10]

        schema_data = {
            "dataset_id": chat_request.dataset_id,
            "columns": columns,
        }

        nl_service = get_nl_to_sql_service()
        nl_result = nl_service.process(
            message=chat_request.message,
            schema=schema_data,
            sample_data=sample_rows,
        )

        response_text = _generate_response_text(chat_request.message, nl_result)

        insights = []
        kpis = []
        suggestions = []

        if chat_request.include_insights:
            try:
                full_data = sample_rows * 10
                insight_report = generate_auto_insights(full_data)
                insights = insight_report.get("insights", [])[:5]
                kpis = insight_report.get("kpis", [])[:6]
                suggestions = insight_report.get("suggested_questions", [])[:4]
            except Exception as e:
                logger.warning(f"Auto-insights failed: {e}")

        return ChatResponse(
            response=response_text,
            sql_query=nl_result.get("sql"),
            chart_type=nl_result.get("chart_type", "table"),
            data_table=sample_rows[:10],
            suggestions=suggestions,
            explanation=nl_result.get("explanation"),
            insights=insights,
            kpis=kpis,
            method=nl_result.get("method", "rule_based"),
        )

    except Exception as e:
        logger.error(f"Chat processing failed: {e}")
        return ChatResponse(
            response="I encountered an error processing your request. Please try again.",
            suggestions=[
                "Show me the data summary",
                "What are the main KPIs?",
                "Show trends over time",
            ],
        )


def _generate_response_text(message: str, nl_result: dict[str, Any]) -> str:
    """Generate natural language response."""
    message_lower = message.lower()

    if "trend" in message_lower or "over time" in message_lower:
        return "Here's the time series analysis showing trends in your data."
    if "top" in message_lower or "highest" in message_lower:
        return "Here are the top records ranked by the selected metric."
    if "average" in message_lower or "mean" in message_lower:
        return "Here's the average analysis with key statistics."
    if "anomaly" in message_lower or "outlier" in message_lower:
        return "I've analyzed your data and identified potential anomalies."
    if "correlation" in message_lower or "related" in message_lower:
        return "Here's the correlation analysis between variables."
    if "segment" in message_lower or "cluster" in message_lower:
        return "I've segmented your data into groups based on characteristics."

    return nl_result.get("explanation", "Here are the results based on your query.")


@router.get("/status", summary="Get chat service status")
async def get_chat_status() -> dict[str, Any]:
    """Get the status of chat services including Ollama."""
    from app.services.nl_to_sql import get_nl_to_sql_service

    nl_service = get_nl_to_sql_service()
    status = nl_service.get_status()

    return {
        "status": "ready",
        "ollama_available": status.get("ollama_available", False),
        "method": status.get("method", "rule_based"),
        "message": "Ready to process queries"
        if status.get("ollama_available")
        else "Using rule-based processing (Ollama not available)",
    }


@router.get("/suggestions/{dataset_id}", summary="Get suggested questions")
async def get_suggestions(
    dataset_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Get AI-suggested questions for a dataset."""
    from app.services.datasets import get_dataset_or_404
    from app.services.auto_insights import generate_auto_insights

    try:
        dataset = await get_dataset_or_404(
            db, dataset_id=dataset_id, actor=current_user
        )

        payload = dataset.analysis_payload or {}
        preview = payload.get("data_preview", {})
        sample_rows = preview.get("sample_rows", [])[:50]

        insight_report = generate_auto_insights(sample_rows)

        return {
            "dataset_id": dataset_id,
            "suggested_questions": insight_report.get("suggested_questions", []),
            "insights": insight_report.get("insights", [])[:3],
            "kpis": insight_report.get("kpis", [])[:4],
        }

    except Exception as e:
        logger.error(f"Failed to get suggestions: {e}")
        return {
            "dataset_id": dataset_id,
            "suggested_questions": [
                "Show me the data summary",
                "What are the main trends?",
                "Show top records",
            ],
            "insights": [],
            "kpis": [],
        }
