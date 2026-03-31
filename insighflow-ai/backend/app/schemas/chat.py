"""Chat schemas for request/response validation."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class ChatSessionCreate(BaseModel):
    """Create chat session payload."""

    title: str | None = Field(None, min_length=1, max_length=255)
    description: str | None = Field(None, max_length=1000)


class ChatSessionRead(BaseModel):
    """Chat session response."""

    id: int
    user_id: int
    dataset_id: int | None
    title: str
    is_active: bool
    last_message_at: datetime | None = None
    message_count: int = 0
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ChatMessageCreate(BaseModel):
    """Send message payload."""

    content: str = Field(..., min_length=1, max_length=5000)


class ChatMessageRead(BaseModel):
    """Chat message response."""

    id: int
    session_id: int
    role: str
    content: str
    sql_query: str | None = None
    chart_config: dict[str, Any] | None = None
    execution_time_ms: int | None = None
    tokens_used: int | None = None
    row_count: int | None = None
    is_error: bool = False
    is_cached: bool = False
    error_message: str | None = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ChatSessionDetailRead(ChatSessionRead):
    """Chat session with messages."""

    messages: list[ChatMessageRead] = Field(default_factory=list)


class ChatHistoryResponse(BaseModel):
    """Chat history response."""

    session_id: int
    messages: list[ChatMessageRead]
    total_messages: int
