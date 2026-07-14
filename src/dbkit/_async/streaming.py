"""Streaming large result sets without loading them into memory (§20).

``AsyncResultStream`` holds one connection for the lifetime of the stream and pulls rows from a
server-side cursor in bounded batches. It is both an async context manager (guaranteeing the
connection is released) and an async iterator. Because a stream holds its connection across
many yields, it deliberately bypasses the retry wrapper — a partially consumed stream cannot be
transparently restarted.
"""

from __future__ import annotations

import contextlib
import time
from typing import Any

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .._core import result as result_mod
from .._core.errors import DatabaseQueryTimeoutError, classify
from .._core.query import Statement
from ..observability import metrics as m
from ..observability.metrics import MetricsSink
from ..observability.tracing import Tracer
from ._compat import stream_mappings


class AsyncResultStream:
    """A bounded, connection-owning async row stream."""

    def __init__(
        self,
        engine: AsyncEngine,
        statement: Statement,
        params: Any,
        *,
        batch_size: int,
        map_to: Any,
        database: str,
        role: str,
        query_name: str,
        metrics: MetricsSink,
        labels: dict[str, str],
        max_duration: float | None = None,
        tracer: Tracer | None = None,
        shard_id: str | None = None,
    ) -> None:
        """Constructed by ``AsyncDatabase.stream(...)``; not intended to be built directly."""
        self._engine = engine
        self._statement = statement
        self._params = params
        self._batch_size = batch_size
        self._mapper = result_mod.build_mapper(map_to)
        self._database = database
        self._role = role
        self._query_name = query_name
        self._metrics = metrics
        self._labels = labels
        self._max_duration = max_duration
        self._tracer = tracer
        self._shard_id = shard_id
        self._conn: AsyncConnection | None = None
        self._gen: Any = None
        self._count = 0
        self._started_at = 0.0
        self._span_cm: Any = None
        self._span: Any = None

    async def __aenter__(self) -> AsyncResultStream:
        """Open the underlying connection and server-side cursor."""
        if self._tracer is not None:
            self._span_cm = self._tracer.span(
                "dbkit.stream",
                operation_type="stream",
                query_name=self._query_name,
                database=self._database,
                shard=self._shard_id,
                role=self._role,
            )
            self._span = self._span_cm.__enter__()
        try:
            self._conn = await self._engine.connect()
            self._gen = stream_mappings(self._conn, self._statement, self._params, self._batch_size)
        except Exception as exc:
            await self._cleanup()
            raise classify(
                exc, query_name=self._query_name, database_name=self._database, role=self._role
            ) from exc
        self._started_at = time.monotonic()
        return self

    def __aiter__(self) -> AsyncResultStream:
        """Returns ``self`` — the stream is its own iterator."""
        return self

    async def __anext__(self) -> Any:
        """The next mapped row, or raise :class:`StopAsyncIteration`/a timeout error."""
        assert self._gen is not None
        if (
            self._max_duration is not None
            and time.monotonic() - self._started_at > self._max_duration
        ):
            raise DatabaseQueryTimeoutError(
                f"stream {self._query_name!r} exceeded max duration {self._max_duration}s",
                query_name=self._query_name,
                database_name=self._database,
                role=self._role,
            )
        try:
            row = await self._gen.__anext__()
        except StopAsyncIteration:
            self._metrics.observe(m.STREAM_ROWS, self._count, labels=self._labels)
            raise
        except Exception as exc:
            raise classify(
                exc, query_name=self._query_name, database_name=self._database, role=self._role
            ) from exc
        self._count += 1
        return self._mapper(row)

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        """Release the connection/cursor unconditionally; never suppresses the exception."""
        await self._cleanup()
        if self._span_cm is not None:
            if self._span is not None:
                self._span.set_attribute("db.rows_affected", self._count)
            self._span_cm.__exit__(exc_type, exc_val, exc_tb)
        return False

    async def _cleanup(self) -> None:
        if self._gen is not None:
            with contextlib.suppress(Exception):
                await self._gen.aclose()
            self._gen = None
        if self._conn is not None:
            with contextlib.suppress(Exception):
                await self._conn.close()
            self._conn = None
