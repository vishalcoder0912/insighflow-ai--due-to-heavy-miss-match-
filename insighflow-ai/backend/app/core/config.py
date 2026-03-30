"""Application settings."""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "InsighFlow AI API"
    app_version: str = "1.0.0"
    api_v1_prefix: str = "/api/v1"
    debug: bool = False
    environment: str = "development"
    host: str = "0.0.0.0"
    port: int = 8001

    secret_key: str = Field(default="change-me-in-production", min_length=16)
    access_token_expire_minutes: int = 30
    refresh_token_expire_days: int = 7
    algorithm: str = "HS256"

    database_url: str = "sqlite+aiosqlite:///./insighflow.db"
    alembic_database_url: str | None = None
    auto_create_db: bool = False
    db_echo: bool = False

    service_api_key: str | None = None
    rate_limit_requests: int = 120
    rate_limit_window_seconds: int = 60

    cors_origins: list[str] = [
        "http://localhost:3000",
        "http://localhost:8080",
        "http://localhost:5173",
        "http://localhost:8003",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:8080",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:8003",
    ]
    cors_allow_credentials: bool = True

    bootstrap_admin_email: str | None = None
    uploads_dir: str = "uploads"
    max_upload_size_bytes: int = 100 * 1024 * 1024

    redis_url: str | None = None
    celery_broker_url: str | None = None
    celery_result_backend: str | None = None
    local_nlp_provider: str = "spacy"
    spacy_model_name: str = "en_core_web_sm"
    anomaly_detection_mode: str = "hybrid"
    local_llm_provider: str = "rule_based"

    @property
    def sync_database_url(self) -> str:
        """Return a sync URL for Alembic."""

        if self.alembic_database_url:
            return self.alembic_database_url
        if self.database_url.startswith("postgresql+asyncpg://"):
            return self.database_url.replace(
                "postgresql+asyncpg://", "postgresql+psycopg2://", 1
            )
        if self.database_url.startswith("sqlite+aiosqlite://"):
            return self.database_url.replace("sqlite+aiosqlite://", "sqlite://", 1)
        return self.database_url

    @property
    def uploads_path(self) -> Path:
        """Return the configured upload directory."""

        return Path(self.uploads_dir)


@lru_cache
def get_settings() -> Settings:
    """Return cached application settings."""

    return Settings()
