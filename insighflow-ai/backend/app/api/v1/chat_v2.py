"""Production-grade Chat API with NL-to-SQL capabilities."""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db
from app.models.chat import ChatMessage, ChatSession
from app.models.user import User
from app.services.chart_generator import ChartGenerator
from app.services.nl_to_sql_ollama import NLToSQLOllama, ollama_query_cache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chat", tags=["chat-v2"])


class CreateSessionRequest(BaseModel):
    """Create new chat session request."""

    dataset_id: int = Field(..., description="Dataset ID to chat with")
    title: str | None = Field(None, description="Optional session title")


class CreateSessionResponse(BaseModel):
    """Create session response."""

    status: str
    session_id: int
    title: str
    created_at: str


class SendMessageRequest(BaseModel):
    """Send chat message request."""

    session_id: int | None = Field(
        None, description="Session ID (creates new if not provided)"
    )
    dataset_id: int = Field(..., description="Dataset ID to query")
    message: str = Field(..., description="User's natural language message")
    use_llm: bool = Field(True, description="Use Ollama LLM if available")


class MessageResponse(BaseModel):
    """Individual message in response."""

    id: int
    role: str
    content: str
    sql_query: str | None = None
    chart_config: dict[str, Any] | None = None
    is_error: bool = False
    is_cached: bool = False
    execution_time_ms: int | None = None
    row_count: int | None = None
    created_at: str


class ChatResponse(BaseModel):
    """Chat response with all data."""

    status: str
    session_id: int
    message: MessageResponse
    suggestions: list[str] = []
    columns: list[str] = []
    rows: list[dict[str, Any]] = []


class SessionHistoryResponse(BaseModel):
    """Session history response."""

    status: str
    sessions: list[dict[str, Any]] = []


class ClearCacheResponse(BaseModel):
    """Cache clear response."""

    status: str
    cleared: bool


def _generate_suggestions(message: str, columns: list[str]) -> list[str]:
    """Generate follow-up query suggestions."""
    msg_lower = message.lower()
    suggestions = []

    if "top" in msg_lower or "highest" in msg_lower:
        suggestions.extend(
            [
                "Show the bottom 5",
                "What is the average?",
                "Show trend over time",
            ]
        )
    elif "sum" in msg_lower or "total" in msg_lower:
        suggestions.extend(
            [
                "Show by category",
                "What is the average?",
                "Show distribution",
            ]
        )
    elif "count" in msg_lower:
        suggestions.extend(
            [
                "What is the total?",
                "Show by category",
                "Find anomalies",
            ]
        )
    else:
        suggestions.extend(
            [
                "Show top 10",
                "What is the total?",
                "Find anomalies",
                "Show correlations",
            ]
        )

    for col in columns[:2]:
        if "date" in col.lower() or "time" in col.lower():
            suggestions.append("Show trend over time")
            break

    return suggestions[:4]


@router.post("/sessions", response_model=CreateSessionResponse)
async def create_chat_session(
    request: CreateSessionRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CreateSessionResponse:
    """Create a new chat session."""
    try:
        title = request.title or f"Chat - {datetime.now().strftime('%Y-%m-%d %H:%M')}"

        session = ChatSession(
            user_id=current_user.id,
            dataset_id=request.dataset_id,
            title=title,
            is_active=True,
            message_count=0,
        )

        db.add(session)
        await db.commit()
        await db.refresh(session)

        return CreateSessionResponse(
            status="success",
            session_id=session.id,
            title=session.title,
            created_at=session.created_at.isoformat() if session.created_at else "",
        )

    except Exception as e:
        logger.error(f"Failed to create session: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail="Failed to create chat session")


@router.get("/sessions", response_model=SessionHistoryResponse)
async def get_chat_sessions(
    dataset_id: int | None = Query(None, description="Filter by dataset ID"),
    limit: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> SessionHistoryResponse:
    """Get chat session history for current user."""
    try:
        query = select(ChatSession).where(
            ChatSession.user_id == current_user.id,
            ChatSession.is_active == True,
        )

        if dataset_id:
            query = query.where(ChatSession.dataset_id == dataset_id)

        query = query.order_by(ChatSession.last_message_at.desc()).limit(limit)

        result = await db.execute(query)
        sessions = result.scalars().all()

        session_list = []
        for session in sessions:
            session_list.append(
                {
                    "id": session.id,
                    "title": session.title,
                    "dataset_id": session.dataset_id,
                    "message_count": session.message_count,
                    "last_message_at": session.last_message_at.isoformat()
                    if session.last_message_at
                    else None,
                    "created_at": session.created_at.isoformat()
                    if session.created_at
                    else None,
                }
            )

        return SessionHistoryResponse(
            status="success",
            sessions=session_list,
        )

    except Exception as e:
        logger.error(f"Failed to get sessions: {e}")
        return SessionHistoryResponse(status="error", sessions=[])


@router.get("/sessions/{session_id}/messages")
async def get_session_messages(
    session_id: int,
    limit: int = Query(50, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Get messages for a specific session."""
    try:
        query = select(ChatSession).where(
            ChatSession.id == session_id,
            ChatSession.user_id == current_user.id,
        )
        result = await db.execute(query)
        session = result.scalar_one_or_none()

        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        msg_query = (
            select(ChatMessage)
            .where(
                ChatMessage.session_id == session_id,
            )
            .order_by(ChatMessage.created_at.desc())
            .limit(limit)
        )

        result = await db.execute(msg_query)
        messages = result.scalars().all()

        return {
            "status": "success",
            "session": {
                "id": session.id,
                "title": session.title,
                "dataset_id": session.dataset_id,
            },
            "messages": [msg.to_dict() for msg in messages],
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get messages: {e}")
        return {"status": "error", "message": str(e), "messages": []}


@router.delete("/sessions/{session_id}")
async def delete_chat_session(
    session_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, str]:
    """Delete a chat session."""
    try:
        query = select(ChatSession).where(
            ChatSession.id == session_id,
            ChatSession.user_id == current_user.id,
        )
        result = await db.execute(query)
        session = result.scalar_one_or_none()

        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        session.is_active = False
        await db.commit()

        return {"status": "success", "message": "Session deleted"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete session: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail="Failed to delete session")


@router.post("/message", response_model=ChatResponse)
async def send_chat_message(
    request: SendMessageRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> ChatResponse:
    """Send a chat message and get NL-to-SQL response."""
    start_time = time.time()

    try:
        session = None
        if request.session_id:
            query = select(ChatSession).where(
                ChatSession.id == request.session_id,
                ChatSession.user_id == current_user.id,
            )
            result = await db.execute(query)
            session = result.scalar_one_or_none()

        if not session:
            session = ChatSession(
                user_id=current_user.id,
                dataset_id=request.dataset_id,
                title=request.message[:50] + "..."
                if len(request.message) > 50
                else request.message,
                is_active=True,
                message_count=0,
            )
            db.add(session)
            await db.flush()

        user_message = ChatMessage(
            session_id=session.id,
            role="user",
            content=request.message,
        )
        db.add(user_message)
        await db.flush()

        nl_to_sql = NLToSQLOllama(db, request.dataset_id)

        sql_result = await nl_to_sql.generate_sql(
            request.message, use_llm=request.use_llm
        )

        query_hash = hashlib.sha256(
            f"{request.dataset_id}:{request.message}".encode()
        ).hexdigest()[:16]

        if sql_result.get("status") == "error":
            error_msg = ChatMessage(
                session_id=session.id,
                role="assistant",
                content=sql_result.get("message", "An error occurred"),
                sql_query=sql_result.get("sql"),
                is_error=True,
                error_message=sql_result.get("message"),
                query_hash=query_hash,
                execution_time_ms=int((time.time() - start_time) * 1000),
            )
            db.add(error_msg)
            await db.commit()

            return ChatResponse(
                status="error",
                session_id=session.id,
                message=MessageResponse(
                    id=error_msg.id,
                    role="assistant",
                    content=error_msg.content,
                    is_error=True,
                    execution_time_ms=error_msg.execution_time_ms,
                ),
            )

        sql_query = sql_result.get("sql", "")

        query_result = await nl_to_sql.execute_query(sql_query)

        chart_config = None
        if query_result.get("rows"):
            chart_config = ChartGenerator.generate_chart_config(
                query_result["rows"],
                query_result.get("columns", []),
            )

        explanation = nl_to_sql.generate_explanation(sql_query, request.message)

        response_content = explanation
        if query_result.get("row_count", 0) > 0:
            response_content += f" (Returned {query_result['row_count']} rows)"

        assistant_message = ChatMessage(
            session_id=session.id,
            role="assistant",
            content=response_content,
            sql_query=sql_query,
            query_hash=query_hash,
            chart_config=chart_config,
            execution_time_ms=sql_result.get("execution_time_ms", 0),
            row_count=query_result.get("row_count", 0),
            is_cached=sql_result.get("is_cached", False),
        )
        db.add(assistant_message)

        session.last_message_at = datetime.utcnow()
        session.message_count += 2

        await db.commit()

        suggestions = _generate_suggestions(
            request.message, query_result.get("columns", [])
        )

        return ChatResponse(
            status="success",
            session_id=session.id,
            message=MessageResponse(
                id=assistant_message.id,
                role="assistant",
                content=assistant_message.content,
                sql_query=assistant_message.sql_query,
                chart_config=assistant_message.chart_config,
                is_cached=assistant_message.is_cached,
                execution_time_ms=assistant_message.execution_time_ms,
                row_count=assistant_message.row_count,
                created_at=assistant_message.created_at.isoformat()
                if assistant_message.created_at
                else "",
            ),
            suggestions=suggestions,
            columns=query_result.get("columns", []),
            rows=query_result.get("rows", [])[:20],
        )

    except Exception as e:
        logger.error(f"Chat error: {e}", exc_info=True)

        error_msg = None
        if session:
            error_msg = ChatMessage(
                session_id=session.id,
                role="assistant",
                content="I encountered an error processing your request. Please try again.",
                is_error=True,
                error_message=str(e),
                execution_time_ms=int((time.time() - start_time) * 1000),
            )
            db.add(error_msg)

        await db.commit()

        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cache/clear", response_model=ClearCacheResponse)
async def clear_query_cache(
    dataset_id: int | None = Query(
        None, description="Clear cache for specific dataset"
    ),
    current_user: User = Depends(get_current_user),
) -> ClearCacheResponse:
    """Clear query cache."""
    try:
        if dataset_id:
            ollama_query_cache.invalidate(dataset_id)
        else:
            ollama_query_cache.clear()

        return ClearCacheResponse(status="success", cleared=True)

    except Exception as e:
        logger.error(f"Failed to clear cache: {e}")
        return ClearCacheResponse(status="error", cleared=False)


@router.get("/health")
async def chat_health_check() -> dict[str, str]:
    """Health check for chat service."""
    return {
        "status": "healthy",
        "service": "chat-v2",
    }
