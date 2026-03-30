"""Pytest fixtures for backend integration tests."""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

os.environ.setdefault("SECRET_KEY", "test-secret-key-123456")
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///./test.db")
os.environ.setdefault("ALEMBIC_DATABASE_URL", "sqlite:///./test.db")
os.environ.setdefault("UPLOADS_DIR", str(Path.cwd() / "test-uploads"))
os.environ.setdefault("RATE_LIMIT_REQUESTS", "1000")

from app.api.deps import get_db  # noqa: E402
from app.core.config import get_settings  # noqa: E402
from app.db.base import metadata  # noqa: E402
from app.main import create_app  # noqa: E402


@pytest.fixture
async def client(tmp_path: Path) -> AsyncGenerator[AsyncClient, None]:
    """Create an isolated test client with a temporary SQLite database."""

    db_path = tmp_path / "test.db"
    uploads_dir = tmp_path / "uploads"
    os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{db_path.as_posix()}"
    os.environ["ALEMBIC_DATABASE_URL"] = f"sqlite:///{db_path.as_posix()}"
    os.environ["UPLOADS_DIR"] = str(uploads_dir)
    get_settings.cache_clear()

    engine = create_async_engine(os.environ["DATABASE_URL"], connect_args={"check_same_thread": False})
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as connection:
        await connection.run_sync(metadata.create_all)

    app = create_app()

    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as async_client:
        yield async_client

    app.dependency_overrides.clear()
    await engine.dispose()


async def register_and_login(client: AsyncClient, email: str = "owner@example.com", password: str = "StrongPass123") -> dict:
    """Register a user and return the auth payload."""

    response = await client.post(
        "/api/v1/auth/register",
        json={"email": email, "full_name": "Test User", "password": password},
    )
    assert response.status_code == 201, response.text
    return response.json()


def auth_headers(token: str) -> dict[str, str]:
    """Return bearer auth headers."""

    return {"Authorization": f"Bearer {token}"}
