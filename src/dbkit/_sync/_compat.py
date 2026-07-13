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
