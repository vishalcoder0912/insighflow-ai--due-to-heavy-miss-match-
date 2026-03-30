"""API router registration."""

from importlib import import_module
import logging

from fastapi import APIRouter

logger = logging.getLogger(__name__)


def _include_router(module_path: str, *, prefix: str | None = None, required: bool = False) -> None:
    """Import and include a router, optionally tolerating missing optional deps."""

    try:
        module = import_module(module_path)
    except Exception:
        if required:
            raise
        logger.exception("Skipping optional router import", extra={"module_path": module_path})
        return

    router = getattr(module, "router")
    if prefix:
        api_router.include_router(router, prefix=prefix)
    else:
        api_router.include_router(router)


api_router = APIRouter()
_include_router("app.api.v1.health", required=True)
_include_router("app.api.v1.auth", prefix="/api/v1", required=True)
_include_router("app.api.v1.users", prefix="/api/v1", required=True)
_include_router("app.api.v1.projects", prefix="/api/v1")
_include_router("app.api.v1.insights", prefix="/api/v1")
_include_router("app.api.v1.analytics", prefix="/api/v1")
_include_router("app.api.v1.analytics_routes", prefix="/api/v1")
_include_router("app.api.v1.advanced_analytics_routes")
_include_router("app.api.v1.datasets", prefix="/api/v1")
_include_router("app.api.v1.dashboards", prefix="/api/v1")
