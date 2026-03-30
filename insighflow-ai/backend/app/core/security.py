"""Security helpers for hashing and tokens."""

from __future__ import annotations

import hashlib
import hmac
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any

from jose import JWTError, jwt
from passlib.context import CryptContext

from app.core.config import get_settings
from app.core.exceptions import ApiException

pwd_context = CryptContext(schemes=["pbkdf2_sha256", "bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    """Hash a password using a stable local password hash."""

    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash."""

    return pwd_context.verify(plain_password, hashed_password)


def create_token(*, subject: str, token_type: str, expires_delta: timedelta, extra_claims: dict[str, Any] | None = None) -> str:
    """Create a signed JWT."""

    settings = get_settings()
    now = datetime.now(UTC)
    payload: dict[str, Any] = {
        "sub": subject,
        "type": token_type,
        "iat": int(now.timestamp()),
        "exp": int((now + expires_delta).timestamp()),
        "jti": secrets.token_urlsafe(16),
    }
    if extra_claims:
        payload.update(extra_claims)
    return jwt.encode(payload, settings.secret_key, algorithm=settings.algorithm)


def decode_token(token: str) -> dict[str, Any]:
    """Decode and validate a JWT."""

    settings = get_settings()
    try:
        return jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
    except JWTError as exc:
        raise ApiException(status_code=401, code="invalid_token", message="Token is invalid or expired.") from exc


def create_access_token(subject: str, *, role: str, email: str) -> str:
    """Create an access token."""

    settings = get_settings()
    return create_token(
        subject=subject,
        token_type="access",
        expires_delta=timedelta(minutes=settings.access_token_expire_minutes),
        extra_claims={"role": role, "email": email},
    )


def create_refresh_token(subject: str) -> str:
    """Create a refresh token."""

    settings = get_settings()
    return create_token(
        subject=subject,
        token_type="refresh",
        expires_delta=timedelta(days=settings.refresh_token_expire_days),
    )


def require_token_type(payload: dict[str, Any], token_type: str) -> None:
    """Ensure the token carries the expected type."""

    if payload.get("type") != token_type:
        raise ApiException(status_code=401, code="invalid_token_type", message=f"Expected a {token_type} token.")


def hash_api_key(raw_key: str) -> str:
    """Hash an API key using SHA-256."""

    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def verify_api_key(raw_key: str, hashed_key: str) -> bool:
    """Compare an API key with its stored hash."""

    return hmac.compare_digest(hash_api_key(raw_key), hashed_key)
