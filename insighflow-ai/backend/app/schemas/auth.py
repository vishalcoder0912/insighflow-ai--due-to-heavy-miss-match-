"""Authentication schemas."""

from __future__ import annotations

from pydantic import BaseModel, EmailStr, Field

from app.schemas.user import UserRead


class LoginRequest(BaseModel):
    """Login payload."""

    email: EmailStr
    password: str = Field(min_length=8, max_length=128)


class RefreshRequest(BaseModel):
    """Refresh token payload."""

    refresh_token: str


class LogoutRequest(BaseModel):
    """Logout payload."""

    refresh_token: str


class TokenPair(BaseModel):
    """Access and refresh token response."""

    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class AuthResponse(BaseModel):
    """Authentication success payload."""

    user: UserRead
    tokens: TokenPair
