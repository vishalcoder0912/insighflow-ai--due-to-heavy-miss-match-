"""Chat session and message service."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.exceptions import ApiException
from app.models.chat import ChatMessage, ChatSession
from app.models.dataset import DatasetAsset
from app.models.user import User
from app.schemas.chat import ChatMessageCreate, ChatSessionCreate
from app.services.nl_to_sql_service import NLToSQLService

logger = logging.getLogger(__name__)


def _get_nl_to_sql_service():
    """Get NL to SQL service instance."""
    return NLToSQLService()


async def create_chat_session(
    session: AsyncSession,
    *,
    user: User,
    dataset_id: int,
    payload: ChatSessionCreate,
) -> ChatSession:
    """Create a new chat session for analyzing a dataset."""

    result = await session.execute(
        select(DatasetAsset).where(DatasetAsset.id == dataset_id)
    )
    dataset = result.scalar_one_or_none()

    if not dataset:
        raise ApiException(
            status_code=404,
            code="dataset_not_found",
            message="Dataset not found",
        )

    chat_session = ChatSession(
        user_id=user.id,
        dataset_id=dataset_id,
        title=payload.title or "New Chat",
    )

    session.add(chat_session)
    await session.flush()
    await session.refresh(chat_session)

    logger.info(f"Created chat session {chat_session.id} for user {user.id}")
    return chat_session


async def get_chat_session(
    session: AsyncSession,
    *,
    session_id: int,
    user: User,
) -> ChatSession:
    """Get a chat session with authorization check."""

    result = await session.execute(
        select(ChatSession)
        .options(
            selectinload(ChatSession.messages),
            selectinload(ChatSession.dataset),
        )
        .where(ChatSession.id == session_id)
    )

    chat_session = result.scalar_one_or_none()

    if not chat_session:
        raise ApiException(
            status_code=404,
            code="session_not_found",
            message="Chat session not found",
        )

    if chat_session.user_id != user.id:
        raise ApiException(
            status_code=403,
            code="access_denied",
            message="Access denied to this chat session",
        )

    return chat_session


async def add_message_to_session(
    session: AsyncSession,
    *,
    chat_session: ChatSession,
    user_message: str,
) -> tuple[ChatMessage, ChatMessage]:
    """Process user message and return both user and assistant messages."""

    user_msg = ChatMessage(
        session_id=chat_session.id,
        role="user",
        content=user_message,
    )
    session.add(user_msg)
    await session.flush()

    import time

    start_time = time.time()

    try:
        payload = chat_session.dataset.analysis_payload or {}
        schema = payload.get("schema_mapping", {})
        if not schema:
            schema = {
                col: "string" for col in payload.get("column_statistics", {}).keys()
            }

        table_name = f"dataset_{chat_session.dataset_id}"

        recent_messages = [
            {
                "role": msg.role,
                "content": msg.content[:100],
                "sql_query": msg.sql_query[:100] if msg.sql_query else None,
            }
            for msg in chat_session.messages[-5:]
        ]

        nl_to_sql = _get_nl_to_sql_service()
        sql_query = await nl_to_sql.convert_nl_to_sql(
            user_message=user_message,
            schema=schema,
            table_name=table_name,
            chat_history=recent_messages,
        )

        execution_time_ms = int((time.time() - start_time) * 1000)

        assistant_msg = ChatMessage(
            session_id=chat_session.id,
            role="assistant",
            content="Analyzed your question",
            sql_query=sql_query,
            execution_time_ms=execution_time_ms,
        )
        session.add(assistant_msg)

    except Exception as e:
        logger.error(f"Error processing message: {e}")

        assistant_msg = ChatMessage(
            session_id=chat_session.id,
            role="assistant",
            content="Error: Could not process query",
            error_message=str(e),
            execution_time_ms=int((time.time() - start_time) * 1000),
            is_error=True,
        )
        session.add(assistant_msg)

    chat_session.message_count = (chat_session.message_count or 0) + 2

    await session.flush()
    await session.refresh(user_msg)
    await session.refresh(assistant_msg)

    return user_msg, assistant_msg


async def list_chat_sessions(
    session: AsyncSession,
    *,
    user: User,
    limit: int = 20,
    offset: int = 0,
    is_active: bool = True,
) -> tuple[list[ChatSession], int]:
    """List chat sessions for a user."""

    base_query = select(ChatSession).where(
        ChatSession.user_id == user.id, ChatSession.is_active == is_active
    )

    count_result = await session.execute(
        select(func.count()).select_from(base_query.subquery())
    )
    total = count_result.scalar() or 0

    result = await session.execute(
        base_query.order_by(ChatSession.last_message_at.desc().nullslast())
        .offset(offset)
        .limit(limit)
    )

    sessions = result.scalars().all()

    return list(sessions), total


async def archive_chat_session(
    session: AsyncSession,
    *,
    chat_session: ChatSession,
) -> ChatSession:
    """Archive a chat session."""

    chat_session.is_active = False
    await session.commit()
    await session.refresh(chat_session)

    return chat_session


async def delete_chat_session(
    session: AsyncSession,
    *,
    chat_session: ChatSession,
) -> None:
    """Delete a chat session and all messages."""

    await session.delete(chat_session)
    await session.commit()
