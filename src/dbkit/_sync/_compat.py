"""Sync counterpart of ``_async/_compat.py`` — hand-written, skipped by the unasync generator.

The sync build has no client-side timeout primitive (there is no ``asyncio.timeout``
equivalent that can interrupt a blocking driver call); it relies on the server-side
``statement_timeout`` set per operation instead (§12.1). Cancellation is not a concept in the
sync world, so the cancellation helpers are inert.
"""

from __future__ import annotations

import contextlib
import time
from typing import Any, Iterator

IS_ASYNC = False

#: Metric/label value distinguishing this frontend (§25.1).
API_LABEL = "sync"


def sync_engine_of(engine: Any) -> Any:
    """A sync ``Engine`` already owns its pool and fires pool events directly."""
    return engine


def sleep(seconds: float) -> None:
    """Frontend-appropriate sleep (async ``asyncio.sleep``; sync ``time.sleep``)."""
    time.sleep(seconds)


def stream_mappings(conn: Any, statement: Any, params: Any, batch_size: int) -> Iterator[Any]:
    """Yield row mappings from a server-side cursor without buffering the full result.

    The sync build uses ``yield_per`` execution options so SQLAlchemy streams rows in
    ``batch_size`` chunks rather than materializing the whole result (§20).
    """
    result = conn.execution_options(yield_per=batch_size).execute(statement, params or {})
    yield from result.mappings()


def copy_from_records(sa_conn: Any, table: str, columns: Any, records: Any) -> int:
    """Dispatch to the sync PostgreSQL COPY implementation (§19.2)."""
    from ..postgres.copy import copy_records_sync

    return copy_records_sync(sa_conn, table, columns, records)


def pipeline_scope(sa_conn: Any) -> contextlib.AbstractContextManager[None]:
    """Dispatch to the sync psycopg pipeline-mode implementation (§7.3)."""
    from ..postgres.pipeline import pipeline_scope_sync

    return pipeline_scope_sync(sa_conn)


def timeout_scope(seconds: float | None) -> contextlib.AbstractContextManager[None]:
    """No client-side deadline in the sync build; server ``statement_timeout`` does the work."""
    return contextlib.nullcontext()


def is_cancellation(exc: BaseException) -> bool:
    """No cooperative cancellation in synchronous code."""
    return False


@contextlib.contextmanager
def cancellation_shield() -> Iterator[None]:
    try:
        yield
    finally:
        pass
