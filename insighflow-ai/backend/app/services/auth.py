"""Authentication and user lifecycle services."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.exceptions import ApiException
from app.core.security import (
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_password,
    require_token_type,
    verify_password,
)
from app.models.enums import UserRole
from app.models.refresh_token import RefreshToken
from app.models.user import User
from app.schemas.auth import TokenPair
from app.schemas.user import UserCreate, UserUpdate
from app.services.audit import log_audit_event


async def _get_user_by_email(session: AsyncSession, email: str) -> User | None:
    result = await session.execute(select(User).where(func.lower(User.email) == email.lower()))
    return result.scalar_one_or_none()


def _refresh_expiry(payload: dict) -> datetime:
    exp = payload.get("exp")
    if exp is None:
        raise ApiException(status_code=401, code="invalid_token", message="Refresh token is missing expiration.")
    return datetime.fromtimestamp(exp, tz=UTC)


async def _issue_tokens(
    session: AsyncSession,
    *,
    user: User,
    ip_address: str | None,
    user_agent: str | None,
) -> TokenPair:
    refresh_token = create_refresh_token(str(user.id))
    refresh_payload = decode_token(refresh_token)
    session.add(
        RefreshToken(
            user_id=user.id,
            jti=refresh_payload["jti"],
            expires_at=_refresh_expiry(refresh_payload),
            ip_address=ip_address,
            user_agent=user_agent,
        )
    )
    access_token = create_access_token(str(user.id), role=user.role.value, email=user.email)
    return TokenPair(access_token=access_token, refresh_token=refresh_token)


async def register_user(
    session: AsyncSession,
    payload: UserCreate,
    *,
    ip_address: str | None,
    user_agent: str | None,
) -> tuple[User, TokenPair]:
    """Create a new user and return issued tokens."""

    existing = await _get_user_by_email(session, payload.email)
    if existing:
        raise ApiException(status_code=409, code="email_already_registered", message="Email is already registered.")

    settings = get_settings()
    role = UserRole.ADMIN if settings.bootstrap_admin_email and settings.bootstrap_admin_email.lower() == payload.email.lower() else UserRole.USER
    user = User(
        email=payload.email.lower(),
        full_name=payload.full_name,
        hashed_password=hash_password(payload.password),
        role=role,
        email_verified=False,
    )
    session.add(user)
    await session.flush()

    tokens = await _issue_tokens(session, user=user, ip_address=ip_address, user_agent=user_agent)
    await log_audit_event(
        session,
        action="user.registered",
        resource_type="user",
        resource_id=str(user.id),
        user_id=user.id,
        ip_address=ip_address,
        payload={"email": user.email},
    )
    await session.commit()
    await session.refresh(user)
    return user, tokens


async def authenticate_user(
    session: AsyncSession,
    *,
    email: str,
    password: str,
    ip_address: str | None,
    user_agent: str | None,
) -> tuple[User, TokenPair]:
    """Authenticate a user and issue a new token pair."""

    user = await _get_user_by_email(session, email)
    if user is None or not verify_password(password, user.hashed_password):
        raise ApiException(status_code=401, code="invalid_credentials", message="Invalid email or password.")
    if not user.is_active:
        raise ApiException(status_code=403, code="user_inactive", message="User account is inactive.")

    tokens = await _issue_tokens(session, user=user, ip_address=ip_address, user_agent=user_agent)
    await log_audit_event(
        session,
        action="user.logged_in",
        resource_type="user",
        resource_id=str(user.id),
        user_id=user.id,
        ip_address=ip_address,
        payload={"email": user.email},
    )
    await session.commit()
    return user, tokens


async def refresh_user_tokens(
    session: AsyncSession,
    *,
    refresh_token: str,
    ip_address: str | None,
    user_agent: str | None,
) -> tuple[User, TokenPair]:
    """Rotate a refresh token and return a new token pair."""

    payload = decode_token(refresh_token)
    require_token_type(payload, "refresh")
    result = await session.execute(select(RefreshToken).where(RefreshToken.jti == payload["jti"]))
    token_record = result.scalar_one_or_none()
    if token_record is None or token_record.revoked_at is not None or token_record.expires_at <= datetime.now(UTC).replace(tzinfo=None):
        raise ApiException(status_code=401, code="refresh_token_invalid", message="Refresh token is invalid.")

    user = await session.get(User, int(payload["sub"]))
    if user is None:
        raise ApiException(status_code=404, code="user_not_found", message="User not found.")

    token_record.revoked_at = datetime.now(UTC)
    tokens = await _issue_tokens(session, user=user, ip_address=ip_address, user_agent=user_agent)
    await log_audit_event(
        session,
        action="user.token_refreshed",
        resource_type="refresh_token",
        resource_id=str(token_record.id),
        user_id=user.id,
        ip_address=ip_address,
    )
    await session.commit()
    return user, tokens


async def logout_user(
    session: AsyncSession,
    *,
    refresh_token: str,
    actor_user_id: int,
    ip_address: str | None,
) -> None:
    """Revoke a refresh token."""

    payload = decode_token(refresh_token)
    require_token_type(payload, "refresh")
    result = await session.execute(select(RefreshToken).where(RefreshToken.jti == payload["jti"]))
    token_record = result.scalar_one_or_none()
    if token_record is None:
        raise ApiException(status_code=404, code="refresh_token_not_found", message="Refresh token not found.")
    if token_record.user_id != actor_user_id:
        raise ApiException(
            status_code=403,
            code="refresh_token_forbidden",
            message="You cannot revoke another user's session.",
        )

    token_record.revoked_at = datetime.now(UTC)
    await log_audit_event(
        session,
        action="user.logged_out",
        resource_type="refresh_token",
        resource_id=str(token_record.id),
        user_id=token_record.user_id,
        ip_address=ip_address,
    )
    await session.commit()


async def update_user_profile(session: AsyncSession, *, user: User, payload: UserUpdate) -> User:
    """Update current user profile settings."""

    if payload.full_name is not None:
        user.full_name = payload.full_name
    if payload.password is not None:
        user.hashed_password = hash_password(payload.password)
    if payload.two_factor_enabled is not None:
        user.two_factor_enabled = payload.two_factor_enabled

    await session.commit()
    await session.refresh(user)
    return user
