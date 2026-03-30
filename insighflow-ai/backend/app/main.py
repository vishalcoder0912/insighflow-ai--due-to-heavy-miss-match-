"""FastAPI application entrypoint."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.router import api_router
from app.core.config import get_settings
from app.core.exceptions import ApiException
from app.core.logging import configure_logging, logger
from app.db.session import dispose_engine
from app.middleware.logging import RequestLoggingMiddleware
from app.middleware.rate_limit import RateLimitMiddleware
from app.utils.files import ensure_upload_directories


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Application startup and shutdown hooks."""

    settings = get_settings()
    ensure_upload_directories()
    if settings.auto_create_db:
        from app.db.base import metadata
        from app.db.session import get_engine

        async with get_engine().begin() as connection:
            await connection.run_sync(metadata.create_all)
    yield
    await dispose_engine()


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""

    settings = get_settings()
    configure_logging()

    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        debug=settings.debug,
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=settings.cors_allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(RequestLoggingMiddleware)
    app.add_middleware(RateLimitMiddleware)
    app.include_router(api_router)

    @app.exception_handler(ApiException)
    async def api_exception_handler(request: Request, exc: ApiException) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": exc.code,
                    "message": exc.message,
                    "details": exc.details,
                    "severity": getattr(exc, "severity", None),
                    "remediation": getattr(exc, "remediation", None),
                    "analysis_type": getattr(exc, "analysis_type", None),
                    "dataset_id": getattr(exc, "dataset_id", None),
                    "correlation_id": getattr(exc, "correlation_id", None),
                },
                "request_id": getattr(request.state, "request_id", None),
            },
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "validation_error",
                    "message": "Request validation failed.",
                    "details": exc.errors(),
                },
                "request_id": getattr(request.state, "request_id", None),
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled application error", exc_info=exc)
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "internal_server_error",
                    "message": "An unexpected error occurred.",
                    "details": None,
                },
                "request_id": getattr(request.state, "request_id", None),
            },
        )

    return app


app = create_app()
