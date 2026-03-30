"""Shared API dependencies."""

from __future__ import annotations

import hmac
from collections.abc import Callable
from typing import Any

from fastapi import Depends, Header, Request
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.exceptions import ApiException
from app.core.security import decode_token, require_token_type, verify_api_key
from app.db.session import get_db_session
from app.models.api_key import ApiKey
from app.models.user import User

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login", auto_error=False)


async def get_db(session: AsyncSession = Depends(get_db_session)) -> AsyncSession:
    """Provide a database session."""

    return session


async def get_current_user(
    token: str | None = Depends(oauth2_scheme), session: AsyncSession = Depends(get_db)
) -> User:
    """Resolve the current user from a bearer token."""

    if not token:
        raise ApiException(
            status_code=401,
            code="not_authenticated",
            message="Authentication credentials were not provided.",
        )
    payload = decode_token(token)
    require_token_type(payload, "access")
    user = await session.get(User, int(payload["sub"]))
    if user is None:
        raise ApiException(
            status_code=404, code="user_not_found", message="User not found."
        )
    if not user.is_active:
        raise ApiException(
            status_code=403, code="user_inactive", message="User account is inactive."
        )
    return user


def require_roles(*roles: str) -> Callable[..., Any]:
    """Create a role-checking dependency."""

    async def dependency(user: User = Depends(get_current_user)) -> User:
        user_role = user.role.value if hasattr(user.role, "value") else str(user.role)
        if user_role not in roles:
            raise ApiException(
                status_code=403,
                code="insufficient_role",
                message="Insufficient permissions.",
            )
        return user

    return dependency


async def get_service_or_user(
    request: Request,
    session: AsyncSession = Depends(get_db),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> User | None:
    """Accept either a logged-in user or an API key."""

    authorization = request.headers.get("Authorization")
    if authorization:
        token = authorization.removeprefix("Bearer").strip()
        if token:
            payload = decode_token(token)
            require_token_type(payload, "access")
            resolved_user = await session.get(User, int(payload["sub"]))
            if resolved_user is not None:
                return resolved_user

    settings = get_settings()
    if x_api_key:
        if settings.service_api_key and hmac.compare_digest(
            x_api_key, settings.service_api_key
        ):
            return None
        result = await session.execute(select(ApiKey).where(ApiKey.is_active.is_(True)))
        for api_key in result.scalars().all():
            if verify_api_key(x_api_key, api_key.key_hash):
                return None
        raise ApiException(
            status_code=401, code="invalid_api_key", message="API key is invalid."
        )

    raise ApiException(
        status_code=401,
        code="not_authenticated",
        message="Bearer token or API key is required.",
    )


def get_request_ip(request: Request) -> str | None:
    """Resolve the client IP address."""

    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else None
