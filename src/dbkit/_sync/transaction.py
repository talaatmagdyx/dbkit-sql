# This file is GENERATED from ../_async/transaction.py by tools/run_unasync.py.
# Do not edit by hand. Run `make unasync` after changing the async source.

"""Explicit transactions, savepoints, commit-unknown, and cancellation cleanup (§11.3-11.6, §15).

A transaction owns a single connection for its lifetime. Commit failures where the outcome is
uncertain surface as :class:`DatabaseCommitUnknownError` (§15). On cancellation the transaction
is rolled back, the connection invalidated, and the cancellation re-raised — never swallowed
(§12.3).
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError
from sqlalchemy import Connection, Engine

from .._core.errors import (
    DatabaseCommitUnknownError,
    classify,
)
from ._compat import cancellation_shield, is_cancellation
from .connection import ConnectionScope, apply_statement_timeout_sql


class TransactionScope(ConnectionScope):
    """A connection scope wrapped in an explicit transaction, with savepoint support."""

    def __init__(
        self,
        conn: Connection,
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

    @contextlib.contextmanager
    def savepoint(self) -> Iterator[TransactionScope]:
        """A nested transaction (SAVEPOINT). Rolls back to the savepoint on error (§11.4)."""
        nested = self._conn.begin_nested()
        try:
            yield self
        except BaseException:
            nested.rollback()
            raise
        else:
            nested.commit()


def _is_connection_error(exc: BaseException) -> bool:
    if isinstance(exc, (InterfaceError, OperationalError)):
        return True
    if isinstance(exc, DBAPIError):
        return bool(getattr(exc, "connection_invalidated", False))
    return isinstance(exc, (ConnectionError, OSError))


class _TransactionManager:
    """Async context manager implementing the transaction lifecycle (§11.1, §11.3)."""

    def __init__(
        self,
        engine: Engine,
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
    ) -> None:
        self._engine = engine
        self._is_postgres = is_postgres
        self._default_timeout = default_timeout
        self._database = database
        self._shard_id = shard_id
        self._role = role
        self._isolation = isolation
        self._read_only = read_only
        self._timeout = timeout if timeout is not None else default_timeout
        self._lock_timeout = lock_timeout
        self._query_name = query_name
        self._conn: Connection | None = None
        self._trans: Any = None
        self._scope: TransactionScope | None = None

    def __enter__(self) -> TransactionScope:
        try:
            self._conn = self._engine.connect()
            self._trans = self._conn.begin()
            self._apply_settings(self._conn)
        except BaseException as exc:
            self._cleanup_on_enter_failure()
            if is_cancellation(exc):
                raise
            raise classify(
                exc, query_name=self._query_name, database_name=self._database, role=self._role
            ) from exc
        self._scope = TransactionScope(
            self._conn,
            is_postgres=self._is_postgres,
            default_timeout=self._timeout,
            database=self._database,
            shard_id=self._shard_id,
            role=self._role,
        )
        return self._scope

    def _apply_settings(self, conn: Connection) -> None:
        if not self._is_postgres:
            return
        if self._isolation:
            level = self._isolation.upper().replace("_", " ")
            conn.execute(text(f"SET TRANSACTION ISOLATION LEVEL {level}"))
        if self._read_only:
            conn.execute(text("SET TRANSACTION READ ONLY"))
        stmt = apply_statement_timeout_sql(self._timeout)
        if stmt is not None:
            conn.execute(text(stmt))
        if self._lock_timeout is not None and self._lock_timeout > 0:
            ms = max(int(self._lock_timeout * 1000), 1)
            conn.execute(text(f"SET LOCAL lock_timeout = {ms}"))

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        assert self._conn is not None
        try:
            if exc is not None:
                self._rollback(cancelled=is_cancellation(exc))
                return False  # propagate the original exception
            self._commit()
            return False
        finally:
            self._release()

    def _commit(self) -> None:
        assert self._trans is not None
        try:
            self._trans.commit()
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
                self._trans.rollback()
            raise classify(
                commit_exc,
                query_name=self._query_name,
                database_name=self._database,
                role=self._role,
            ) from commit_exc

    def _rollback(self, *, cancelled: bool) -> None:
        if self._trans is None:
            return
        with cancellation_shield():
            try:
                self._trans.rollback()
            except Exception:
                # Rollback failed — the connection state is uncertain; invalidate it.
                with contextlib.suppress(Exception):
                    self._conn.invalidate()  # type: ignore[union-attr]
        if cancelled:
            # State after cancellation is uncertain; drop the connection from the pool.
            with contextlib.suppress(Exception):
                self._conn.invalidate()  # type: ignore[union-attr]

    def _release(self) -> None:
        if self._conn is None:
            return
        with cancellation_shield():
            with contextlib.suppress(Exception):
                self._conn.close()
        self._conn = None
        self._trans = None

    def _cleanup_on_enter_failure(self) -> None:
        if self._trans is not None:
            with contextlib.suppress(Exception):
                self._trans.rollback()
        self._release()
