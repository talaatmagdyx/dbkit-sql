"""psycopg3 pipeline mode — batch several dependent statements into one round trip (§7.3).

In pipeline mode the client sends statements to the server without waiting for each response
before sending the next, then results are processed together. This is a raw-driver escape
hatch (like COPY): PostgreSQL + psycopg only, reached through
:meth:`AsyncConnectionScope.pipeline` / the sync equivalent, and useful for a handful of
dependent-but-batchable statements in the same transaction (e.g. a business write plus its
inbox record, §28).

Because SQLAlchemy's cursor execution transparently triggers a pipeline sync whenever a result
is actually fetched (rowcount, ``RETURNING``, a `SELECT``), ordinary ``tx.execute(...)`` calls
inside the block still work — the win comes from the driver not waiting for each response
before sending the next statement, not from changing the calling code.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator, Iterator
from typing import Any

from ..errors import DatabaseUnsupportedOperationError


def _require_psycopg(driver_conn: Any) -> None:
    if not hasattr(driver_conn, "pipeline"):
        raise DatabaseUnsupportedOperationError(
            "pipeline mode requires the psycopg driver; this target uses a different driver"
        )


@contextlib.asynccontextmanager
async def pipeline_scope_async(sa_conn: Any) -> AsyncIterator[None]:
    """Enter psycopg pipeline mode on ``sa_conn``'s raw driver connection (async)."""
    raw = await sa_conn.get_raw_connection()
    driver_conn = raw.driver_connection
    _require_psycopg(driver_conn)
    async with driver_conn.pipeline():
        yield


@contextlib.contextmanager
def pipeline_scope_sync(sa_conn: Any) -> Iterator[None]:
    """Enter psycopg pipeline mode on ``sa_conn``'s raw driver connection (sync)."""
    driver_conn = sa_conn.connection.driver_connection
    _require_psycopg(driver_conn)
    with driver_conn.pipeline():
        yield
