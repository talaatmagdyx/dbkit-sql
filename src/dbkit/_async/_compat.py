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


async def sleep(seconds: float) -> None:
    """Frontend-appropriate sleep (async ``asyncio.sleep``; sync ``time.sleep``)."""
    await asyncio.sleep(seconds)


async def stream_mappings(conn: Any, statement: Any, params: Any, batch_size: int) -> Any:
    """Yield row mappings from a server-side cursor without buffering the full result.

    Async uses ``AsyncConnection.stream`` (a real server-side cursor); the sync build uses
    ``yield_per`` execution options. Both fetch in ``batch_size`` chunks (§20).
    """
    result = await conn.stream(statement, params or {}, execution_options={"yield_per": batch_size})
    async for row in result.mappings():
        yield row


async def copy_from_records(sa_conn: Any, table: str, columns: Any, records: Any) -> int:
    """Dispatch to the async PostgreSQL COPY implementation (§19.2)."""
    from ..postgres.copy import copy_records_async

    return await copy_records_async(sa_conn, table, columns, records)


def pipeline_scope(sa_conn: Any) -> contextlib.AbstractAsyncContextManager[None]:
    """Dispatch to the async psycopg pipeline-mode implementation (§7.3)."""
    from ..postgres.pipeline import pipeline_scope_async

    return pipeline_scope_async(sa_conn)


def timeout_scope(seconds: float | None) -> contextlib.AbstractAsyncContextManager[Any]:
    """A client-side deadline. Async uses :func:`asyncio.timeout`; the sync build has no
    client-side timeout and relies on the server ``statement_timeout`` instead (§12.1)."""
    if seconds is None or seconds <= 0:
        return contextlib.nullcontext()
    return asyncio.timeout(seconds)


async def semaphore_acquire(sem: asyncio.Semaphore, timeout: float | None) -> bool:
    """Acquire ``sem``, returning False (never raising) if ``timeout`` elapses first.

    ``asyncio.Semaphore.acquire()`` has no built-in timeout, unlike ``threading.Semaphore``
    (§17); this wraps it in :func:`asyncio.wait_for` so both frontends expose the same
    bounded-wait contract to :class:`ConcurrencyLimiter`.
    """
    if timeout is None:
        await sem.acquire()
        return True
    try:
        await asyncio.wait_for(sem.acquire(), timeout=timeout)
    except TimeoutError:
        return False
    return True


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
