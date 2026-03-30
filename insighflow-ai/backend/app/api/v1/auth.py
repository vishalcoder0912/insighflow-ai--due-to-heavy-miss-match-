"""Authentication endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db, get_request_ip
from app.schemas.auth import AuthResponse, LoginRequest, LogoutRequest, RefreshRequest
from app.schemas.user import UserCreate
from app.services import auth as auth_service

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", response_model=AuthResponse, status_code=status.HTTP_201_CREATED)
async def register(
    payload: UserCreate,
    request: Request,
    session: AsyncSession = Depends(get_db),
) -> AuthResponse:
    user, tokens = await auth_service.register_user(
        session,
        payload,
        ip_address=get_request_ip(request),
        user_agent=request.headers.get("User-Agent"),
    )
    return AuthResponse(user=user, tokens=tokens)


@router.post("/login", response_model=AuthResponse)
async def login(
    payload: LoginRequest,
    request: Request,
    session: AsyncSession = Depends(get_db),
) -> AuthResponse:
    user, tokens = await auth_service.authenticate_user(
        session,
        email=payload.email,
        password=payload.password,
        ip_address=get_request_ip(request),
        user_agent=request.headers.get("User-Agent"),
    )
    return AuthResponse(user=user, tokens=tokens)


@router.post("/refresh", response_model=AuthResponse)
async def refresh_tokens(
    payload: RefreshRequest,
    request: Request,
    session: AsyncSession = Depends(get_db),
) -> AuthResponse:
    user, tokens = await auth_service.refresh_user_tokens(
        session,
        refresh_token=payload.refresh_token,
        ip_address=get_request_ip(request),
        user_agent=request.headers.get("User-Agent"),
    )
    return AuthResponse(user=user, tokens=tokens)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT, response_class=Response)
async def logout(
    payload: LogoutRequest,
    request: Request,
    session: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
) -> Response:
    await auth_service.logout_user(
        session,
        refresh_token=payload.refresh_token,
        actor_user_id=user.id,
        ip_address=get_request_ip(request),
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
