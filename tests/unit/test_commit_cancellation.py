"""An interruption during COMMIT (task cancellation, KeyboardInterrupt, SystemExit) propagates
unchanged on every commit path: it is never swallowed, reclassified or followed by a rollback,
and the connection, whose transaction outcome is unknown, is invalidated, not reused."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any

import pytest

from dbkit._async.executor import ResilientExecutor
from dbkit._async.transaction import _AsyncTransactionManager
from dbkit._sync.transaction import _TransactionManager
from dbkit.observability import metrics as m

INTERRUPTS = [asyncio.CancelledError, KeyboardInterrupt, SystemExit]


class _Metrics:
    def __init__(self) -> None:
        self.counts: dict[str, int] = {}

    def incr(self, name: str, value: float = 1, *, labels: Any = None) -> None:
        self.counts[name] = self.counts.get(name, 0) + 1

    def observe(self, *_: Any, **__: Any) -> None:
        return None


class _Conn:
    """Records what happens to the connection; commit() raises the given exception."""

    def __init__(self, exc: BaseException | None = None) -> None:
        self.exc, self.calls = exc, []

    async def commit(self) -> None:
        self.calls.append("commit")
        if self.exc is not None:
            raise self.exc

    async def rollback(self) -> None:
        self.calls.append("rollback")

    async def invalidate(self) -> None:
        self.calls.append("invalidate")

    async def close(self) -> None:
        self.calls.append("close")


class _SyncConn:
    def __init__(self, exc: BaseException) -> None:
        self.exc, self.calls = exc, []

    def commit(self) -> None:
        self.calls.append("commit")
        raise self.exc

    def rollback(self) -> None:
        self.calls.append("rollback")

    def invalidate(self) -> None:
        self.calls.append("invalidate")

    def close(self) -> None:
        self.calls.append("close")


def _async_tx(conn: _Conn, metrics: _Metrics) -> _AsyncTransactionManager:
    tx = _AsyncTransactionManager.__new__(_AsyncTransactionManager)
    tx._conn, tx._trans = conn, conn  # type: ignore[assignment]
    tx._database, tx._shard_id, tx._role, tx._query_name = "db", "s", "write", None
    tx._metrics, tx._labels, tx._span = metrics, {}, None  # type: ignore[assignment]
    tx._started_at, tx._long_transaction_warning = 0.0, float("inf")
    return tx


@pytest.mark.parametrize("interrupt", INTERRUPTS)
async def test_transaction_commit_interrupted_propagates_invalidates_and_counts_commit_unknown(
    interrupt: type[BaseException],
) -> None:
    conn, metrics = _Conn(interrupt()), _Metrics()
    with pytest.raises(interrupt):
        await _async_tx(conn, metrics).__aexit__(None, None, None)
    assert "rollback" not in conn.calls  # the COMMIT may have landed; a rollback proves nothing
    assert conn.calls.index("invalidate") < conn.calls.index("close")  # discarded, then released
    assert metrics.counts.get(m.COMMIT_UNKNOWN) == 1 and m.TX_ROLLBACK not in metrics.counts


def test_sync_transaction_commit_interrupted_propagates_and_invalidates() -> None:
    conn, metrics = _SyncConn(KeyboardInterrupt()), _Metrics()
    tx = _TransactionManager.__new__(_TransactionManager)
    tx._conn, tx._trans = conn, conn  # type: ignore[assignment]
    tx._database, tx._shard_id, tx._role, tx._query_name = "db", "s", "write", None
    tx._metrics, tx._labels, tx._span = metrics, {}, None  # type: ignore[assignment]
    tx._started_at, tx._long_transaction_warning = 0.0, float("inf")
    with pytest.raises(KeyboardInterrupt):
        tx.__exit__(None, None, None)
    assert "rollback" not in conn.calls and "invalidate" in conn.calls
    assert metrics.counts.get(m.COMMIT_UNKNOWN) == 1


def _executor(conn: _Conn, metrics: _Metrics) -> ResilientExecutor:
    """A ResilientExecutor with its collaborators stubbed: only scope()'s commit path is real."""
    ex = ResilientExecutor.__new__(ResilientExecutor)
    entry = SimpleNamespace(
        key=SimpleNamespace(database="db", shard_id=None, role="write"), engine=None
    )

    async def resolve(_: Any) -> Any:
        return entry, None

    async def acquire(*_: Any) -> Any:
        return conn

    @contextlib.contextmanager
    def span(*_: Any, **__: Any) -> Any:
        yield SimpleNamespace(set_attribute=lambda *a: None)

    ex.entry = resolve  # type: ignore[method-assign]
    ex.acquire = acquire  # type: ignore[method-assign]
    ex.query_meta = lambda q: ("q", "execute", None)  # type: ignore[method-assign]
    ex.labels = lambda *a: {}  # type: ignore[method-assign]
    ex.is_postgres = lambda e: False  # type: ignore[method-assign]
    ex._tracer = SimpleNamespace(span=span)  # type: ignore[attr-defined]
    ex._metrics = metrics  # type: ignore[attr-defined]
    ex._config = SimpleNamespace(  # type: ignore[attr-defined]
        defaults=SimpleNamespace(
            query_timeout_seconds=1.0, observability=SimpleNamespace(slow_query_ms=1e9)
        )
    )
    return ex


@pytest.mark.parametrize("interrupt", INTERRUPTS)
async def test_auto_commit_interrupted_propagates_unchanged_and_invalidates(
    interrupt: type[BaseException],
) -> None:
    """db.execute, execute_many, db.connection() and bulk best-effort all commit through scope()."""
    conn, metrics = _Conn(interrupt()), _Metrics()
    scope: AsyncIterator[Any] = _executor(conn, metrics).scope(
        SimpleNamespace(), None, call_timeout=None, deadline=None, commit=True
    )
    with pytest.raises(interrupt):
        async with scope:
            pass
    assert "rollback" not in conn.calls and "invalidate" in conn.calls
    assert metrics.counts.get(m.COMMIT_UNKNOWN) == 1


async def test_auto_commit_success_is_unchanged() -> None:
    conn, metrics = _Conn(), _Metrics()
    async with _executor(conn, metrics).scope(
        SimpleNamespace(), None, call_timeout=None, deadline=None, commit=True
    ):
        pass
    assert conn.calls == ["commit", "close"]
