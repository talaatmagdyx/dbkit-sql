# This file is GENERATED from ../_async/connection.py by tools/run_unasync.py.
# Do not edit by hand. Run `make unasync` after changing the async source.

"""Low-level execution primitives and the explicit connection scope (§11.1-11.2).

The primitives take an already-acquired SQLAlchemy connection and run one statement. Cardinality
(exactly-one / at-most-one / scalar) is enforced by SQLAlchemy's own native ``Result.one()`` /
``.one_or_none()`` / ``.scalar_one()`` / ``.scalars().all()`` — not reimplemented here — with
``NoResultFound`` / ``MultipleResultsFound`` translated to :class:`DatabaseResultError`. Higher
layers (the facade, the connection scope, the transaction scope) reuse these primitives so
mapping, timeouts, and error classification live in exactly one place.
"""

from __future__ import annotations

import contextlib
import re
from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import RowMapping, text
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import MultipleResultsFound, NoResultFound
from sqlalchemy import Connection

from .._core import result as result_mod
from .._core.errors import (
    DatabaseProgrammingError,
    DatabaseResultError,
    DatabaseUnsupportedOperationError,
    classify,
)
from .._core.query import Query, Statement, coerce_statement
from ._compat import IS_ASYNC, pipeline_scope, timeout_scope


def apply_statement_timeout_sql(seconds: float | None) -> str | None:
    """Return a ``SET LOCAL statement_timeout`` statement for PostgreSQL, or ``None``.

    The value is our own integer (milliseconds), never caller input, so inlining it is safe.
    """
    if seconds is None or seconds <= 0:
        return None
    ms = max(int(seconds * 1000), 1)
    return f"SET LOCAL statement_timeout = {ms}"


_SETTING_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


def _apply_local_settings(
    conn: Connection, settings: Mapping[str, str] | None, *, is_postgres: bool
) -> None:
    """Apply ``Query.settings`` with transaction-local scope (§12.4).

    Uses parameterized ``set_config(name, value, true)`` so values are never
    interpolated; setting names are whitelist-validated. PostgreSQL only.
    """
    if not settings:
        return
    if not is_postgres:
        raise DatabaseUnsupportedOperationError("Query.settings requires PostgreSQL")
    for name, value in settings.items():
        if not _SETTING_NAME_RE.fullmatch(name):
            raise DatabaseProgrammingError(f"invalid setting name in Query.settings: {name!r}")
        conn.execute(
            text("SELECT set_config(:setting_name, :setting_value, true)"),
            {"setting_name": name, "setting_value": str(value)},
        )


def _maybe_set_timeout(
    conn: Connection, seconds: float | None, *, is_postgres: bool
) -> None:
    # The async frontend bounds a single statement with a real client-side deadline
    # (``asyncio.timeout``), which cancels the driver — so the extra server-side
    # ``SET statement_timeout`` round trip is pure overhead and is skipped. The sync frontend
    # has no client-side timeout primitive, so it relies on the server-side setting (§12.1).
    if IS_ASYNC or not is_postgres:
        return
    stmt = apply_statement_timeout_sql(seconds)
    if stmt is not None:
        conn.execute(text(stmt))


def run_fetch(
    conn: Connection,
    statement: Statement,
    params: Mapping[str, Any] | None,
    *,
    timeout: float | None,
    is_postgres: bool,
) -> list[RowMapping]:
    """Execute a read and return every row mapping, no cardinality constraint."""
    with timeout_scope(timeout):
        _maybe_set_timeout(conn, timeout, is_postgres=is_postgres)
        cursor = conn.execute(statement, params or {})
        return list(cursor.mappings().all())


def run_fetch_one(
    conn: Connection,
    statement: Statement,
    params: Mapping[str, Any] | None,
    *,
    timeout: float | None,
    is_postgres: bool,
) -> RowMapping:
    """Execute a read expecting exactly one row (``Result.one()``, §8.1)."""
    with timeout_scope(timeout):
        _maybe_set_timeout(conn, timeout, is_postgres=is_postgres)
        cursor = conn.execute(statement, params or {})
        return cursor.mappings().one()


def run_fetch_optional(
    conn: Connection,
    statement: Statement,
    params: Mapping[str, Any] | None,
    *,
    timeout: float | None,
    is_postgres: bool,
) -> RowMapping | None:
    """Execute a read expecting zero or one row (``Result.one_or_none()``, §8.1)."""
    with timeout_scope(timeout):
        _maybe_set_timeout(conn, timeout, is_postgres=is_postgres)
        cursor = conn.execute(statement, params or {})
        return cursor.mappings().one_or_none()


def run_fetch_value(
    conn: Connection,
    statement: Statement,
    params: Mapping[str, Any] | None,
    *,
    timeout: float | None,
    is_postgres: bool,
) -> Any:
    """Execute a read expecting exactly one row and take its first column
    (``Result.scalar_one()``, §8.1)."""
    with timeout_scope(timeout):
        _maybe_set_timeout(conn, timeout, is_postgres=is_postgres)
        cursor = conn.execute(statement, params or {})
        return cursor.scalar_one()


def run_fetch_values(
    conn: Connection,
    statement: Statement,
    params: Mapping[str, Any] | None,
    *,
    timeout: float | None,
    is_postgres: bool,
) -> list[Any]:
    """Execute a read and take the first column of every row (``Result.scalars().all()``)."""
    with timeout_scope(timeout):
        _maybe_set_timeout(conn, timeout, is_postgres=is_postgres)
        cursor = conn.execute(statement, params or {})
        return list(cursor.scalars().all())


def run_execute(
    conn: Connection,
    statement: Statement,
    params: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None,
    *,
    timeout: float | None,
    is_postgres: bool,
) -> CursorResult[Any]:
    """Execute a write/DDL statement and return the cursor result (for rowcount/returning)."""
    with timeout_scope(timeout):
        _maybe_set_timeout(conn, timeout, is_postgres=is_postgres)
        return conn.execute(statement, params or {})


class ConnectionScope:
    """A held connection exposing the fetch/execute family (§11.2).

    Obtained via ``with db.connection(target) as conn:``. All operations run on the same
    physical connection; use this when several statements must share connection state.
    """

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
        """Wrap an already-acquired connection; not constructed directly by applications."""
        self._conn = conn
        self._is_postgres = is_postgres
        self._default_timeout = default_timeout
        self._database = database
        self._shard_id = shard_id
        self._role = role

    @property
    def raw(self) -> Connection:
        """The underlying SQLAlchemy connection — an explicit "you now own error handling"
        escape hatch (§7.3).

        Anything executed via ``.raw`` bypasses dbkit entirely: no error classification (you'll
        see raw driver/SQLAlchemy exceptions, not :class:`~dbkit.errors.DatabaseError`
        subclasses — an ``except DatabaseError`` handler will not catch them), no metrics, no
        tracing, and no retry/circuit-breaker/commit-unknown handling. Use it only for the two
        things dbkit doesn't itself implement (:meth:`pipeline`, :meth:`~dbkit.Database.
        copy_records`'s underlying driver connection) or a genuine one-off that needs raw driver
        access — not as a routine way to run queries.
        """
        return self._conn

    def pipeline(self) -> contextlib.AbstractContextManager[None]:
        """Enter psycopg pipeline mode (§7.3): statements issued in this block are sent to the
        server without waiting for each response before the next — one round trip for several
        dependent-but-batchable statements (e.g. a business write plus its inbox record, §28).

        Usage::

            with db.transaction(target=t) as tx, tx.pipeline():
                tx.execute(INSERT_ORDER, params)
                tx.execute(INSERT_INBOX, params)

        PostgreSQL + psycopg only; raises :class:`DatabaseUnsupportedOperationError` otherwise.
        Ordinary ``tx.execute(...)`` calls work unchanged inside the block — SQLAlchemy's
        cursor execution transparently syncs the pipeline whenever a result is fetched.
        """
        return pipeline_scope(self._conn)

    def _resolve(
        self, query: object, params: Mapping[str, Any] | None
    ) -> tuple[Statement, Query | None, float | None, str]:
        statement = coerce_statement(query)
        q = query if isinstance(query, Query) else None
        timeout = q.timeout if (q and q.timeout is not None) else self._default_timeout
        name = q.name if q else "adhoc"
        return statement, q, timeout, name

    def fetch_all(
        self, query: object, params: Mapping[str, Any] | None = None, *, map_to: Any = None
    ) -> list[Any]:
        """Run a read and return every row, mapped to ``map_to`` (no cardinality check)."""
        statement, _q, timeout, name = self._resolve(query, params)
        try:
            _apply_local_settings(
                self._conn, _q.settings if _q else None, is_postgres=self._is_postgres
            )
            rows = run_fetch(
                self._conn, statement, params, timeout=timeout, is_postgres=self._is_postgres
            )
        except Exception as exc:
            raise classify(
                exc, query_name=name, database_name=self._database, role=self._role
            ) from exc
        return result_mod.map_rows(rows, map_to)

    def fetch_one(
        self, query: object, params: Mapping[str, Any] | None = None, *, map_to: Any = None
    ) -> Any:
        """Run a read expecting exactly one row; raises :class:`DatabaseResultError` otherwise."""
        statement, _q, timeout, name = self._resolve(query, params)
        try:
            _apply_local_settings(
                self._conn, _q.settings if _q else None, is_postgres=self._is_postgres
            )
            row = run_fetch_one(
                self._conn, statement, params, timeout=timeout, is_postgres=self._is_postgres
            )
        except NoResultFound as exc:
            raise DatabaseResultError(
                f"query {name!r} returned no rows, expected exactly one",
                query_name=name,
                database_name=self._database,
                role=self._role,
            ) from exc
        except MultipleResultsFound as exc:
            raise DatabaseResultError(
                f"query {name!r} returned more than one row, expected exactly one",
                query_name=name,
                database_name=self._database,
                role=self._role,
            ) from exc
        except Exception as exc:
            raise classify(
                exc, query_name=name, database_name=self._database, role=self._role
            ) from exc
        return result_mod.build_mapper(map_to)(row)

    def fetch_optional(
        self, query: object, params: Mapping[str, Any] | None = None, *, map_to: Any = None
    ) -> Any | None:
        """Run a read expecting zero or one row; ``None`` if empty, else the mapped row."""
        statement, _q, timeout, name = self._resolve(query, params)
        try:
            _apply_local_settings(
                self._conn, _q.settings if _q else None, is_postgres=self._is_postgres
            )
            row = run_fetch_optional(
                self._conn, statement, params, timeout=timeout, is_postgres=self._is_postgres
            )
        except MultipleResultsFound as exc:
            raise DatabaseResultError(
                f"query {name!r} returned more than one row, expected at most one",
                query_name=name,
                database_name=self._database,
                role=self._role,
            ) from exc
        except Exception as exc:
            raise classify(
                exc, query_name=name, database_name=self._database, role=self._role
            ) from exc
        if row is None:
            return None
        return result_mod.build_mapper(map_to)(row)

    def fetch_value(self, query: object, params: Mapping[str, Any] | None = None) -> Any:
        """Run a read expecting exactly one row and return its first column."""
        statement, _q, timeout, name = self._resolve(query, params)
        try:
            _apply_local_settings(
                self._conn, _q.settings if _q else None, is_postgres=self._is_postgres
            )
            return run_fetch_value(
                self._conn, statement, params, timeout=timeout, is_postgres=self._is_postgres
            )
        except NoResultFound as exc:
            raise DatabaseResultError(
                f"query {name!r} returned no rows, expected a single scalar value",
                query_name=name,
                database_name=self._database,
                role=self._role,
            ) from exc
        except MultipleResultsFound as exc:
            raise DatabaseResultError(
                f"query {name!r} returned more than one row, expected a single scalar value",
                query_name=name,
                database_name=self._database,
                role=self._role,
            ) from exc
        except Exception as exc:
            raise classify(
                exc, query_name=name, database_name=self._database, role=self._role
            ) from exc

    def fetch_values(
        self, query: object, params: Mapping[str, Any] | None = None
    ) -> list[Any]:
        """Run a read and return the first column of every row."""
        statement, _q, timeout, name = self._resolve(query, params)
        try:
            _apply_local_settings(
                self._conn, _q.settings if _q else None, is_postgres=self._is_postgres
            )
            return run_fetch_values(
                self._conn, statement, params, timeout=timeout, is_postgres=self._is_postgres
            )
        except Exception as exc:
            raise classify(
                exc, query_name=name, database_name=self._database, role=self._role
            ) from exc

    def execute(self, query: object, params: Mapping[str, Any] | None = None) -> int:
        """Run a write/DDL statement and return the affected row count."""
        statement, _q, timeout, name = self._resolve(query, params)
        try:
            _apply_local_settings(
                self._conn, _q.settings if _q else None, is_postgres=self._is_postgres
            )
            cursor = run_execute(
                self._conn, statement, params, timeout=timeout, is_postgres=self._is_postgres
            )
            return cursor.rowcount
        except Exception as exc:
            raise classify(
                exc, query_name=name, database_name=self._database, role=self._role
            ) from exc
