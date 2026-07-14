"""Consumer micro-batching (§17.1 high-perf addition).

``BatchCollector`` aggregates items from many concurrent message handlers and flushes them as
one batched database write, either when the buffer reaches ``max_size`` or after
``max_delay_ms`` — whichever comes first. This is the primary throughput lever for
message-driven ingestion: N handlers each contribute a row, one COPY/executemany persists them.

Async-only (message consumers are async). The flush callback receives the buffered items and
should perform the batched write (e.g. ``db.copy_records`` / ``db.insert_many``).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import Generic, TypeVar

T = TypeVar("T")


class BatchCollector(Generic[T]):
    """Aggregates items across concurrent producers and flushes them as one batch (§17.1)."""

    def __init__(
        self,
        flush: Callable[[Sequence[T]], Awaitable[None]],
        *,
        max_size: int = 1000,
        max_delay_ms: float = 50.0,
    ) -> None:
        """``flush`` is called with the buffered batch whenever ``max_size``/``max_delay_ms``
        is reached; it should perform the batched write (e.g. ``db.insert_many``)."""
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        self._flush_cb = flush
        self._max_size = max_size
        self._max_delay = max_delay_ms / 1000.0
        self._buffer: list[T] = []
        self._lock = asyncio.Lock()
        self._timer: asyncio.Task[None] | None = None
        self._closed = False

    async def add(self, item: T) -> None:
        """Add an item; flush immediately if the buffer is full, else arm the flush timer."""
        async with self._lock:
            if self._closed:
                raise RuntimeError("BatchCollector is closed")
            self._buffer.append(item)
            if len(self._buffer) >= self._max_size:
                await self._flush_locked()
            elif self._timer is None:
                self._timer = asyncio.create_task(self._timer_flush())

    async def flush(self) -> None:
        """Flush any buffered items now."""
        async with self._lock:
            await self._flush_locked()

    async def close(self) -> None:
        """Flush remaining items and stop the timer."""
        async with self._lock:
            self._closed = True
            await self._flush_locked()

    async def _timer_flush(self) -> None:
        try:
            await asyncio.sleep(self._max_delay)
        except asyncio.CancelledError:
            return
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if not self._buffer:
            return
        items = self._buffer
        self._buffer = []
        await self._flush_cb(items)
