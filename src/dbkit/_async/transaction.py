"""Explicit transactions, savepoints, commit-unknown, and cancellation cleanup (§11.3-11.6, §15).

A transaction owns a single connection for its lifetime. Commit failures where the outcome is
uncertain surface as :class:`DatabaseCommitUnknownError` (§15). On cancellation the transaction
is rolled back, the connection invalidated, and the cancellation re-raised — never swallowed
(§12.3).
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import AsyncIterator
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .._core.errors import (
    DatabaseCommitUnknownError,
    classify,
)
from ..observability import logging as obslog
from ..observability import metrics as m
from ..observability.metrics import MetricsSink, NoopMetrics
from ._compat import cancellation_shield, is_cancellation
from .connection import AsyncConnectionScope, apply_statement_timeout_sql


class AsyncTransactionScope(AsyncConnectionScope):
    """A connection scope wrapped in an explicit transaction, with savepoint support."""

    def __init__(
        self,
        conn: AsyncConnection,
        *,
        is_postgres: bool,
        default_timeout: float | None,
        database: str,
        shard_id: str,
        role: str,
    ) -> None:
        super().__init__(
            conn,
            is_postgres=is_postgres,
            default_timeout=default_timeout,
            database=database,
            shard_id=shard_id,
            role=role,
        )

    @contextlib.asynccontextmanager
    async def savepoint(self) -> AsyncIterator[AsyncTransactionScope]:
        """A nested transaction (SAVEPOINT). Rolls back to the savepoint on error (§11.4)."""
        nested = await self._conn.begin_nested()
        try:
            yield self
        except BaseException:
            await nested.rollback()
            raise
        else:
            await nested.commit()


def _is_connection_error(exc: BaseException) -> bool:
    """Whether ``exc`` indicates the connection itself is broken, not just a query failure (§15).

    Prefers SQLAlchemy's own per-dialect disconnect detection (``connection_invalidated``,
    computed by each dialect's ``is_disconnect()`` when it wraps the driver exception) over a
    blanket ``OperationalError`` check — ``OperationalError`` also covers many transient-but-
    not-disconnected conditions (e.g. some lock/resource errors), which would otherwise
    over-classify ordinary failures as commit-unknown.
    """
    if isinstance(exc, DBAPIError):
        return exc.connection_invalidated
    return isinstance(exc, (ConnectionError, OSError))


class _AsyncTransactionManager:
    """Async context manager implementing the transaction lifecycle (§11.1, §11.3)."""

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        is_postgres: bool,
        default_timeout: float | None,
        database: str,
        shard_id: str,
        role: str,
        isolation: str | None,
        read_only: bool,
        timeout: float | None,
        lock_timeout: float | None,
        query_name: str,
        metrics: MetricsSink | None = None,
        labels: dict[str, str] | None = None,
        long_transaction_warning_seconds: float = 5.0,
        span: Any = None,
        deferrable: bool = False,
    ) -> None:
        self._engine = engine
        self._is_postgres = is_postgres
        self._default_timeout = default_timeout
        self._database = database
        self._shard_id = shard_id
        self._role = role
        self._isolation = isolation
        self._read_only = read_only
        self._deferrable = deferrable
        self._timeout = timeout if timeout is not None else default_timeout
        self._lock_timeout = lock_timeout
        self._query_name = query_name
        self._metrics = metrics or NoopMetrics()
        self._labels = labels or {"database": database, "shard": shard_id, "role": role}
        self._long_transaction_warning = long_transaction_warning_seconds
        self._span = span
        self._conn: AsyncConnection | None = None
        self._trans: Any = None
        self._scope: AsyncTransactionScope | None = None
        self._started_at: float = 0.0

    async def __aenter__(self) -> AsyncTransactionScope:
        try:
            self._conn = await self._engine.connect()
            # Isolation level / read-only / deferrable must be set *before* BEGIN — SQLAlchemy's
            # execution_options() applies them via the driver's native connection attributes
            # where possible (e.g. psycopg's own .isolation_level/.read_only), not raw SQL
            # (§11.5). isolation_level is dialect-portable; the postgresql_* options are PG-only.
            opts = self._execution_options()
            if opts:
                self._conn = await self._conn.execution_options(**opts)
            self._trans = await self._conn.begin()
            await self._apply_settings(self._conn)
        except BaseException as exc:
            await self._cleanup_on_enter_failure()
            if is_cancellation(exc):
                raise
            raise classify(
                exc, query_name=self._query_name, database_name=self._database, role=self._role
            ) from exc
        self._started_at = time.monotonic()
        self._scope = AsyncTransactionScope(
            self._conn,
            is_postgres=self._is_postgres,
            default_timeout=self._timeout,
            database=self._database,
            shard_id=self._shard_id,
            role=self._role,
        )
        return self._scope

    def _execution_options(self) -> dict[str, Any]:
        opts: dict[str, Any] = {}
        if self._isolation:
            opts["isolation_level"] = self._isolation.upper().replace("_", " ")
        if self._is_postgres:
            if self._read_only:
                opts["postgresql_readonly"] = True
            if self._deferrable:
                opts["postgresql_deferrable"] = True
        return opts

    async def _apply_settings(self, conn: AsyncConnection) -> None:
        # Statement/lock timeout are PostgreSQL GUCs with no portable execution_option — they
        # have no driver-native equivalent, so SET LOCAL is the only option (§10, §12.1). Must
        # run after BEGIN: SET LOCAL only takes effect inside an open transaction.
        if not self._is_postgres:
            return
        stmt = apply_statement_timeout_sql(self._timeout)
        if stmt is not None:
            await conn.execute(text(stmt))
        if self._lock_timeout is not None and self._lock_timeout > 0:
            ms = max(int(self._lock_timeout * 1000), 1)
            await conn.execute(text(f"SET LOCAL lock_timeout = {ms}"))

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        assert self._conn is not None
        outcome = "commit"
        try:
            if exc is not None:
                outcome = "cancelled" if is_cancellation(exc) else "rollback"
                await self._rollback(cancelled=is_cancellation(exc))
                return False  # propagate the original exception
            try:
                await self._commit()
            except DatabaseCommitUnknownError:
                outcome = "commit_unknown"
                raise
            except BaseException:
                outcome = "rollback"  # _commit() already attempted an internal rollback
                raise
            return False
        finally:
            duration = time.monotonic() - self._started_at
            self._metrics.incr(m.TX_TOTAL, labels=self._labels)
            self._metrics.observe(m.TX_DURATION, duration, labels=self._labels)
            if outcome in ("rollback", "cancelled"):
                self._metrics.incr(m.TX_ROLLBACK, labels=self._labels)
            elif outcome == "commit_unknown":
                self._metrics.incr(m.COMMIT_UNKNOWN, labels=self._labels)
            if self._span is not None:
                self._span.set_attribute("db.transaction.duration_ms", round(duration * 1000, 3))
            if duration >= self._long_transaction_warning:
                obslog.long_transaction_warning(
                    duration_ms=duration * 1000,
                    threshold_ms=self._long_transaction_warning * 1000,
                    database=self._database,
                    role=self._role,
                    outcome=outcome,
                )
            await self._release()

    async def _commit(self) -> None:
        assert self._trans is not None
        try:
            await self._trans.commit()
        except BaseException as commit_exc:
            # A failure during COMMIT may mean the transaction committed anyway (§15).
            if _is_connection_error(commit_exc):
                raise DatabaseCommitUnknownError(
                    "connection failed during COMMIT; transaction outcome is unknown",
                    original=commit_exc,
                    database_name=self._database,
                    shard_id=self._shard_id,
                    role=self._role,
                    query_name=self._query_name,
                ) from commit_exc
            # Otherwise attempt rollback and classify normally.
            with contextlib.suppress(Exception):
                await self._trans.rollback()
            raise classify(
                commit_exc,
                query_name=self._query_name,
                database_name=self._database,
                role=self._role,
            ) from commit_exc

    async def _rollback(self, *, cancelled: bool) -> None:
        if self._trans is None:
            return
        async with cancellation_shield():
            try:
                await self._trans.rollback()
            except Exception:
                # Rollback failed — the connection state is uncertain; invalidate it.
                with contextlib.suppress(Exception):
                    await self._conn.invalidate()  # type: ignore[union-attr]
        if cancelled:
            # State after cancellation is uncertain; drop the connection from the pool.
            with contextlib.suppress(Exception):
                await self._conn.invalidate()  # type: ignore[union-attr]

    async def _release(self) -> None:
        if self._conn is None:
            return
        async with cancellation_shield():
            with contextlib.suppress(Exception):
                await self._conn.close()
        self._conn = None
        self._trans = None

    async def _cleanup_on_enter_failure(self) -> None:
        if self._trans is not None:
            with contextlib.suppress(Exception):
                await self._trans.rollback()
        await self._release()
