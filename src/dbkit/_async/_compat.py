"""Async-specific primitives that do not translate by token substitution.

This module is hand-written on both sides (see ``_sync/_compat.py``); the unasync generator
skips it. It isolates the two genuine sync/async differences: client-side timeouts and
cancellation handling (§12).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any

IS_ASYNC = True

#: Metric/label value distinguishing this frontend (§25.1).
API_LABEL = "async"


def sync_engine_of(engine: Any) -> Any:
    """The synchronous ``Engine`` that owns the pool and fires pool events.

    For an ``AsyncEngine`` this is ``engine.sync_engine``; pool events are always synchronous
    and fire on it. (The sync build returns the engine itself.)
    """
    return engine.sync_engine


def timeout_scope(seconds: float | None) -> contextlib.AbstractAsyncContextManager[Any]:
    """A client-side deadline. Async uses :func:`asyncio.timeout`; the sync build has no
    client-side timeout and relies on the server ``statement_timeout`` instead (§12.1)."""
    if seconds is None or seconds <= 0:
        return contextlib.nullcontext()
    return asyncio.timeout(seconds)


def is_cancellation(exc: BaseException) -> bool:
    """True if ``exc`` is a cooperative cancellation that must be re-raised, not swallowed."""
    return isinstance(exc, asyncio.CancelledError)


@contextlib.asynccontextmanager
async def cancellation_shield() -> AsyncIterator[None]:
    """Best-effort protection of a short cleanup (rollback/return-to-pool) from cancellation."""
    try:
        yield
    finally:
        pass
