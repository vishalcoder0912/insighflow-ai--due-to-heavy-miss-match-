"""Audit logging service."""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.audit_log import AuditLog


async def log_audit_event(
    session: AsyncSession,
    *,
    action: str,
    resource_type: str,
    resource_id: str | None,
    user_id: int | None,
    ip_address: str | None,
    payload: dict[str, Any] | None = None,
) -> None:
    """Persist an audit record."""

    session.add(
        AuditLog(
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            user_id=user_id,
            ip_address=ip_address,
            payload=payload or {},
        )
    )
