"""Chat models for session and message management."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, IntegerIDMixin, TimestampMixin


class ChatSession(Base, IntegerIDMixin, TimestampMixin):
    """Chat session for organizing conversations."""

    __tablename__ = "chat_sessions"

    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    dataset_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("datasets.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    title: Mapped[str] = mapped_column(String(255), nullable=False, default="New Chat")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_message_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    message_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    user = relationship("User", back_populates="chat_sessions")
    dataset = relationship("DatasetAsset", back_populates="chat_sessions")
    messages = relationship(
        "ChatMessage",
        back_populates="session",
        cascade="all, delete-orphan",
        order_by="ChatMessage.created_at",
    )

    __table_args__ = (
        Index("ix_chat_session_user_dataset", "user_id", "dataset_id"),
        Index("ix_chat_session_last_message", "last_message_at"),
    )


class ChatMessage(Base, IntegerIDMixin, TimestampMixin):
    """Individual chat message within a session."""

    __tablename__ = "chat_messages"

    session_id: Mapped[int] = mapped_column(
        ForeignKey("chat_sessions.id", ondelete="CASCADE"), nullable=False, index=True
    )
    role: Mapped[str] = mapped_column(String(20), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)

    sql_query: Mapped[str | None] = mapped_column(Text, nullable=True)
    query_hash: Mapped[str | None] = mapped_column(
        String(64), nullable=True, index=True
    )
    chart_config: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    execution_time_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tokens_used: Mapped[int | None] = mapped_column(Integer, nullable=True)
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    is_error: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_cached: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    session = relationship("ChatSession", back_populates="messages")

    __table_args__ = (
        Index("ix_chat_message_session_created", "session_id", "created_at"),
    )

    def to_dict(self) -> dict[str, Any]:
        """Convert message to dictionary for API response."""
        return {
            "id": self.id,
            "role": self.role,
            "content": self.content,
            "sql_query": self.sql_query,
            "chart_config": self.chart_config,
            "execution_time_ms": self.execution_time_ms,
            "tokens_used": self.tokens_used,
            "row_count": self.row_count,
            "is_error": self.is_error,
            "is_cached": self.is_cached,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
