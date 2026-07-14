"""PostgreSQL COPY via the psycopg3 raw-driver escape hatch (§7.3, §19.2).

COPY is the fastest bulk-ingest path. It is PostgreSQL + psycopg specific, so it lives here and
is reached only through :meth:`AsyncDatabase.copy_records` / :meth:`Database.copy_records`,
which check the dialect/driver first. Both an async and a sync implementation are provided
(this module is not unasync-generated); the frontends dispatch via ``_compat``.

Memory stays bounded: rows are written one at a time to the COPY stream and never fully
buffered. The identifiers (table/columns) come from the application, not end users — callers
must not pass untrusted identifiers (§18.5).
"""

from __future__ import annotations

from collections.abc import AsyncIterable, Iterable, Sequence
from typing import Any

from ..errors import DatabaseUnsupportedOperationError


def _copy_sql(table: str, columns: Sequence[str]) -> str:
    cols = ", ".join(f'"{c}"' for c in columns)
    return f'COPY "{table}" ({cols}) FROM STDIN'


def _require_psycopg(driver_conn: Any) -> None:
    # Both psycopg and asyncpg's raw connections expose a `cursor` method (incompatible
    # signatures/purposes — asyncpg's is for server-side result cursors, not COPY), so `cursor`
    # alone can't distinguish them. `pipeline` is psycopg-only (mirrors postgres/pipeline.py's
    # own check) and reliably identifies a real psycopg connection.
    if not hasattr(driver_conn, "pipeline"):
        raise DatabaseUnsupportedOperationError(
            "COPY requires the psycopg driver; this target uses a different driver"
        )


async def copy_records_async(
    sa_conn: Any,
    table: str,
    columns: Sequence[str],
    records: Iterable[Sequence[Any]] | AsyncIterable[Sequence[Any]],
) -> int:
    """Stream ``records`` into ``table`` via COPY on the async psycopg connection. Returns the
    number of rows written."""
    raw = await sa_conn.get_raw_connection()
    driver_conn = raw.driver_connection
    _require_psycopg(driver_conn)
    stmt = _copy_sql(table, columns)
    written = 0
    async with driver_conn.cursor() as cur, cur.copy(stmt) as copy:
        if hasattr(records, "__aiter__"):
            async for row in records:
                await copy.write_row(row)
                written += 1
        else:
            for row in records:
                await copy.write_row(row)
                written += 1
    return written


def copy_records_sync(
    sa_conn: Any,
    table: str,
    columns: Sequence[str],
    records: Iterable[Sequence[Any]],
) -> int:
    """Stream ``records`` into ``table`` via COPY on the sync psycopg connection."""
    driver_conn = sa_conn.connection.driver_connection
    _require_psycopg(driver_conn)
    stmt = _copy_sql(table, columns)
    written = 0
    with driver_conn.cursor() as cur, cur.copy(stmt) as copy:
        for row in records:
            copy.write_row(row)
            written += 1
    return written
