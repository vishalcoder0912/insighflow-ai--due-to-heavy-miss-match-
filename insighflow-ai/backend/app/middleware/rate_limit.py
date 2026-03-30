"""Simple in-memory rate limiting middleware."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, Response

from app.core.config import get_settings


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Apply a per-client sliding-window rate limit."""

    def __init__(self, app) -> None:  # type: ignore[no-untyped-def]
        super().__init__(app)
        self._buckets: dict[str, deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        settings = get_settings()
        client_id = request.client.host if request.client else "anonymous"
        bucket_key = f"{client_id}:{request.url.path}"
        now = time.time()

        async with self._lock:
            bucket = self._buckets[bucket_key]
            while bucket and bucket[0] <= now - settings.rate_limit_window_seconds:
                bucket.popleft()
            if len(bucket) >= settings.rate_limit_requests:
                return JSONResponse(
                    status_code=429,
                    content={
                        "error": {
                            "code": "rate_limit_exceeded",
                            "message": "Too many requests. Please retry later.",
                            "details": {
                                "limit": settings.rate_limit_requests,
                                "window_seconds": settings.rate_limit_window_seconds,
                            },
                        },
                        "request_id": getattr(request.state, "request_id", None),
                    },
                )
            bucket.append(now)

        return await call_next(request)
