"""Low-level execution primitives and the explicit connection scope (§11.1-11.2).

The primitives take an already-acquired SQLAlchemy connection and run one statement. Higher
layers (the facade, the connection scope, the transaction scope) reuse them so cardinality,
mapping, timeouts, and error classification live in exactly one place.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import RowMapping, text
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncConnection

from .._core import result as result_mod
from .._core.errors import classify
from .._core.query import Query, Statement, coerce_statement
from ._compat import timeout_scope


def apply_statement_timeout_sql(seconds: float | None) -> str | None:
    """Return a ``SET LOCAL statement_timeout`` statement for PostgreSQL, or ``None``.

    The value is our own integer (milliseconds), never caller input, so inlining it is safe.
    """
    if seconds is None or seconds <= 0:
        return None
    ms = max(int(seconds * 1000), 1)
    return f"SET LOCAL statement_timeout = {ms}"


async def _maybe_set_timeout(
    conn: AsyncConnection, seconds: float | None, *, is_postgres: bool
) -> None:
    if not is_postgres:
        return
    stmt = apply_statement_timeout_sql(seconds)
    if stmt is not None:
        await conn.execute(text(stmt))


async def run_fetch(
    conn: AsyncConnection,
    statement: Statement,
    params: Mapping[str, Any] | None,
    *,
    timeout: float | None,
    is_postgres: bool,
) -> list[RowMapping]:
    """Execute a read and return buffered row mappings."""
    async with timeout_scope(timeout):
        await _maybe_set_timeout(conn, timeout, is_postgres=is_postgres)
        cursor = await conn.execute(statement, params or {})
        return list(cursor.mappings().all())


async def run_execute(
    conn: AsyncConnection,
    statement: Statement,
    params: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None,
    *,
    timeout: float | None,
    is_postgres: bool,
) -> CursorResult[Any]:
    """Execute a write/DDL statement and return the cursor result (for rowcount/returning)."""
    async with timeout_scope(timeout):
        await _maybe_set_timeout(conn, timeout, is_postgres=is_postgres)
        return await conn.execute(statement, params or {})


class AsyncConnectionScope:
    """A held connection exposing the fetch/execute family (§11.2).

    Obtained via ``async with db.connection(target) as conn:``. All operations run on the same
    physical connection; use this when several statements must share connection state.
    """

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
        self._conn = conn
        self._is_postgres = is_postgres
        self._default_timeout = default_timeout
        self._database = database
        self._shard_id = shard_id
        self._role = role

    @property
    def raw(self) -> AsyncConnection:
        """The underlying SQLAlchemy connection (escape hatch, §7.3)."""
        return self._conn

    def _resolve(
        self, query: object, params: Mapping[str, Any] | None
    ) -> tuple[Statement, Query | None, float | None, str]:
        statement = coerce_statement(query)
        q = query if isinstance(query, Query) else None
        timeout = q.timeout if (q and q.timeout is not None) else self._default_timeout
        name = q.name if q else "adhoc"
        return statement, q, timeout, name

    async def _fetch(
        self, query: object, params: Mapping[str, Any] | None
    ) -> tuple[list[RowMapping], str]:
        """Run a read and return ``(rows, query_name)`` (name reused for cardinality checks)."""
        statement, _q, timeout, name = self._resolve(query, params)
        try:
            rows = await run_fetch(
                self._conn, statement, params, timeout=timeout, is_postgres=self._is_postgres
            )
        except Exception as exc:
            raise classify(
                exc, query_name=name, database_name=self._database, role=self._role
            ) from exc
        return rows, name

    async def fetch_all(
        self, query: object, params: Mapping[str, Any] | None = None, *, map_to: Any = None
    ) -> list[Any]:
        rows, _name = await self._fetch(query, params)
        return result_mod.map_rows(rows, map_to)

    async def fetch_one(
        self, query: object, params: Mapping[str, Any] | None = None, *, map_to: Any = None
    ) -> Any:
        rows, name = await self._fetch(query, params)
        return result_mod.enforce_one(rows, name, map_to)

    async def fetch_optional(
        self, query: object, params: Mapping[str, Any] | None = None, *, map_to: Any = None
    ) -> Any | None:
        rows, name = await self._fetch(query, params)
        return result_mod.enforce_optional(rows, name, map_to)

    async def fetch_value(self, query: object, params: Mapping[str, Any] | None = None) -> Any:
        rows, name = await self._fetch(query, params)
        return result_mod.enforce_value(rows, name)

    async def fetch_values(
        self, query: object, params: Mapping[str, Any] | None = None
    ) -> list[Any]:
        rows, _name = await self._fetch(query, params)
        return result_mod.extract_values(rows)

    async def execute(self, query: object, params: Mapping[str, Any] | None = None) -> int:
        statement, _q, timeout, name = self._resolve(query, params)
        try:
            cursor = await run_execute(
                self._conn, statement, params, timeout=timeout, is_postgres=self._is_postgres
            )
            return cursor.rowcount
        except Exception as exc:
            raise classify(
                exc, query_name=name, database_name=self._database, role=self._role
            ) from exc
