"""Translate SQLAlchemy / driver / network / timeout exceptions into dbkit errors (§13).

Order of precedence (SQLSTATE-first, message-matching only as a last resort):

1. Already a :class:`DatabaseError` — return unchanged.
2. ``asyncio.TimeoutError`` / ``TimeoutError`` — client-side deadline.
3. Extract a SQLSTATE from the DBAPI ``orig`` and map it (``sqlstate.py``).
4. Fall back to the SQLAlchemy exception *type* (OperationalError, IntegrityError, ...).
5. Fall back to plain OS/connection errors.
6. Anything else -> generic :class:`DatabaseError`.
"""

from __future__ import annotations

import asyncio
from typing import Any

from . import base as e
from .redaction import sanitize_message
from .sqlstate import error_class_for_sqlstate

try:  # SQLAlchemy is a hard dependency, but keep the import defensive for pure-core tests.
    from sqlalchemy import exc as sa_exc
except Exception:  # pragma: no cover
    sa_exc = None  # type: ignore[assignment]


def _extract_sqlstate(orig: BaseException | None) -> str | None:
    """Pull a 5-char SQLSTATE from a DBAPI exception (psycopg3, asyncpg, generic)."""
    if orig is None:
        return None
    # psycopg3: exc.sqlstate ; also .diag.sqlstate
    for attr in ("sqlstate", "pgcode"):
        code = getattr(orig, attr, None)
        if isinstance(code, str) and len(code) == 5:
            return code
    diag = getattr(orig, "diag", None)
    if diag is not None:
        code = getattr(diag, "sqlstate", None)
        if isinstance(code, str) and len(code) == 5:
            return code
    return None


def _from_sqlalchemy_type(exc: Any) -> type[e.DatabaseError] | None:
    """Map by SQLAlchemy exception class when no SQLSTATE is available."""
    if sa_exc is None:
        return None
    if isinstance(exc, sa_exc.IntegrityError):
        return e.DatabaseIntegrityError
    if isinstance(exc, sa_exc.ProgrammingError):
        return e.DatabaseProgrammingError
    if isinstance(exc, sa_exc.OperationalError):
        # Operational errors are typically transient connectivity/availability issues.
        return e.DatabaseConnectionError
    if isinstance(exc, sa_exc.DisconnectionError):
        return e.DatabaseConnectionError
    if isinstance(exc, sa_exc.TimeoutError):
        # SQLAlchemy raises this on pool checkout timeout.
        return e.DatabasePoolTimeoutError
    if isinstance(exc, sa_exc.DBAPIError):
        return e.DatabaseError
    return None


def classify(
    exc: BaseException,
    *,
    query_name: str | None = None,
    database_name: str | None = None,
    shard_id: str | None = None,
    role: str | None = None,
) -> e.DatabaseError:
    """Normalize any exception into a :class:`DatabaseError` with context attached."""
    if isinstance(exc, e.DatabaseError):
        return exc.with_context(
            query_name=query_name,
            database_name=database_name,
            shard_id=shard_id,
            role=role,
        )

    # Client-side deadline (asyncio.timeout / wait_for).
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return e.DatabaseQueryTimeoutError(
            "operation exceeded its client-side deadline",
            original=exc,
            query_name=query_name,
            database_name=database_name,
            shard_id=shard_id,
            role=role,
        )

    orig = getattr(exc, "orig", None) or (exc if _looks_like_dbapi(exc) else None)
    sqlstate = _extract_sqlstate(orig) or _extract_sqlstate(exc)

    cls = error_class_for_sqlstate(sqlstate)
    if cls is None:
        cls = _from_sqlalchemy_type(exc)
    if cls is None and isinstance(exc, (ConnectionError, OSError)):
        cls = e.DatabaseConnectionError
    if cls is None:
        cls = e.DatabaseError

    connection_invalidated = bool(getattr(exc, "connection_invalidated", False))
    message = sanitize_message(str(exc)) or cls.code

    err = cls(
        message,
        sqlstate=sqlstate,
        original=exc,
        query_name=query_name,
        database_name=database_name,
        shard_id=shard_id,
        role=role,
    )
    if connection_invalidated:
        err.connection_invalidated = True
    return err


def _looks_like_dbapi(exc: BaseException) -> bool:
    return hasattr(exc, "sqlstate") or hasattr(exc, "pgcode") or hasattr(exc, "diag")
