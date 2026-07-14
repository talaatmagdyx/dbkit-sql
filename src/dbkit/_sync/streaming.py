# This file is GENERATED from ../_async/streaming.py by tools/run_unasync.py.
# Do not edit by hand. Run `make unasync` after changing the async source.

"""Streaming large result sets without loading them into memory (§20).

``ResultStream`` holds one connection for the lifetime of the stream and pulls rows from a
server-side cursor in bounded batches. It is both an async context manager (guaranteeing the
connection is released) and an async iterator. Because a stream holds its connection across
many yields, it deliberately bypasses the retry wrapper — a partially consumed stream cannot be
transparently restarted.
"""

from __future__ import annotations

import contextlib
import time
from typing import Any

from sqlalchemy import Connection, Engine

from .._core import result as result_mod
from .._core.errors import DatabaseQueryTimeoutError, classify
from .._core.query import Statement
from ..observability import metrics as m
from ..observability.metrics import MetricsSink
from ._compat import stream_mappings


class ResultStream:
    """A bounded, connection-owning async row stream."""

    def __init__(
        self,
        engine: Engine,
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
    ) -> None:
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
        self._conn: Connection | None = None
        self._gen: Any = None
        self._count = 0
        self._started_at = 0.0

    def __enter__(self) -> ResultStream:
        try:
            self._conn = self._engine.connect()
            self._gen = stream_mappings(self._conn, self._statement, self._params, self._batch_size)
        except Exception as exc:
            self._cleanup()
            raise classify(
                exc, query_name=self._query_name, database_name=self._database, role=self._role
            ) from exc
        self._started_at = time.monotonic()
        return self

    def __iter__(self) -> ResultStream:
        return self

    def __next__(self) -> Any:
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
            row = self._gen.__next__()
        except StopIteration:
            self._metrics.observe(m.STREAM_ROWS, self._count, labels=self._labels)
            raise
        except Exception as exc:
            raise classify(
                exc, query_name=self._query_name, database_name=self._database, role=self._role
            ) from exc
        self._count += 1
        return self._mapper(row)

    def __exit__(self, *exc: object) -> bool:
        self._cleanup()
        return False

    def _cleanup(self) -> None:
        if self._gen is not None:
            with contextlib.suppress(Exception):
                self._gen.close()
            self._gen = None
        if self._conn is not None:
            with contextlib.suppress(Exception):
                self._conn.close()
            self._conn = None
