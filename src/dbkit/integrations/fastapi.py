"""FastAPI/Starlette integration — RFC 7807 responses for dbkit errors.

Install once at app construction and every unhandled :class:`~dbkit.errors.DatabaseError`
maps to a clean, category-driven HTTP response instead of a 500 stack trace:

- ``OVERLOAD_CATEGORIES`` (pool timeout, limiter, circuit open, backend unreachable)
  → **503** ``application/problem+json`` with a ``Retry-After`` header. The request is
  retryable by contract: dbkit's fixed pools shed load instead of growing.
- ``TIMEOUT_CATEGORIES`` (query timeout, cancelled, lock timeout) → **504**.
- Everything else (programming, integrity, configuration, ...) → **500**, kept loud —
  those are bugs, not load.

Usage::

    from fastapi import FastAPI
    from dbkit.integrations.fastapi import install_exception_handlers

    app = FastAPI()
    install_exception_handlers(app, retry_after_seconds=2)

Only requires Starlette (FastAPI's base) — imported lazily so dbkit itself keeps no
web-framework dependency.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from ..errors import OVERLOAD_CATEGORIES, TIMEOUT_CATEGORIES, DatabaseError
from ..observability import logging as obslog

if TYPE_CHECKING:  # pragma: no cover - typing only
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.responses import JSONResponse

__all__ = ["install_exception_handlers"]

_PROBLEM_MEDIA_TYPE = "application/problem+json"
_DEFAULT_RETRY_AFTER_SECONDS = 2


def _derive_retry_after(database: Any) -> int:
    """Circuit-breaker ``open_seconds`` (rounded up) when a facade is provided."""
    if database is None:
        return _DEFAULT_RETRY_AFTER_SECONDS
    try:
        open_seconds = database.config.defaults.circuit_breaker.open_seconds
        return max(int(-(-open_seconds // 1)), 1)  # ceil, at least 1s
    except AttributeError:
        return _DEFAULT_RETRY_AFTER_SECONDS


def install_exception_handlers(
    app: Starlette | Any,
    *,
    retry_after_seconds: int | None = None,
    database: Any = None,
    expose_detail: bool = False,
) -> None:
    """Register a :class:`DatabaseError` handler on ``app`` (FastAPI or Starlette).

    ``retry_after_seconds`` populates the 503 ``Retry-After`` header. When omitted
    and ``database`` (an ``AsyncDatabase``/``Database``) is given, it derives from
    the circuit breaker's ``open_seconds`` — the earliest moment a retry can find
    the breaker half-open. Falls back to 2 seconds.

    ``expose_detail`` includes the sanitized error message in the response body —
    keep it off in production unless the API is internal.
    """
    if retry_after_seconds is None:
        retry_after_seconds = _derive_retry_after(database)
    try:
        from starlette.responses import JSONResponse
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "dbkit.integrations.fastapi requires Starlette — install fastapi or starlette"
        ) from exc

    def _problem(
        request: Request,
        *,
        type_slug: str,
        title: str,
        status: int,
        detail: str | None,
        headers: dict[str, str] | None = None,
    ) -> JSONResponse:
        body: dict[str, Any] = {
            "type": f"urn:problem:{type_slug}",
            "title": title,
            "status": status,
            "instance": str(request.url.path),
        }
        if detail is not None:
            body["detail"] = detail
        return JSONResponse(
            body, status_code=status, headers=headers, media_type=_PROBLEM_MEDIA_TYPE
        )

    async def _database_error_handler(request: Request, exc: Exception) -> JSONResponse:
        assert isinstance(exc, DatabaseError)  # registered for DatabaseError only
        detail = str(exc) if expose_detail else None
        if exc.category in TIMEOUT_CATEGORIES:
            return _problem(
                request,
                type_slug="database-timeout",
                title="Database query exceeded its time budget",
                status=504,
                detail=detail,
            )
        if exc.category in OVERLOAD_CATEGORIES:
            return _problem(
                request,
                type_slug="database-overloaded",
                title="Database is temporarily overloaded",
                status=503,
                detail=detail,
                headers={"Retry-After": str(retry_after_seconds)},
            )
        # Programming/integrity/configuration errors are bugs — keep them loud.
        obslog.log_event(
            logging.ERROR,
            "database.unhandled_error",
            category=exc.category.value,
            error=type(exc).__name__,
        )
        return _problem(
            request,
            type_slug="database-error",
            title="An unexpected database error occurred",
            status=500,
            detail=detail,
        )

    app.add_exception_handler(DatabaseError, _database_error_handler)
