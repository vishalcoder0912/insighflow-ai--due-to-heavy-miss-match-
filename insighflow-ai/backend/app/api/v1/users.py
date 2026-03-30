"""User endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db, require_roles
from app.models.user import User
from app.schemas.user import UserRead, UserUpdate
from app.services.auth import update_user_profile

router = APIRouter(prefix="/users", tags=["users"])


@router.get("/me", response_model=UserRead)
async def get_me(current_user: User = Depends(get_current_user)) -> User:
    return current_user


@router.put("/me", response_model=UserRead)
async def update_me(
    payload: UserUpdate,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> User:
    return await update_user_profile(session, user=current_user, payload=payload)


@router.get("/{user_id}", response_model=UserRead)
async def get_user_detail(
    user_id: int,
    session: AsyncSession = Depends(get_db),
    admin: User = Depends(require_roles("admin")),
) -> User:
    del admin
    user = await session.get(User, user_id)
    if user is None:
        from app.core.exceptions import ApiException

        raise ApiException(status_code=404, code="user_not_found", message="User not found.")
    return user
