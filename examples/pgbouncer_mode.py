"""PgBouncer-compatible pooling mode (§10). Run:

    DBKIT_DSN=postgresql+psycopg://localhost/postgres python examples/pgbouncer_mode.py

Under PgBouncer's *transaction* pooling, a client connection may land on a different physical
PostgreSQL backend each transaction — so a server-side prepared statement id from a prior
transaction may not exist there. ``pool.pgbouncer_compatible`` disables driver-side
autoprepare (psycopg's ``prepare_threshold``, asyncpg's ``statement_cache_size``) so this never
happens. dbkit already scopes every session setting with ``SET LOCAL``/per-transaction, never
a bare session-level ``SET``, so no other change is needed.
"""

from __future__ import annotations

import asyncio
import os

from dbkit import AsyncDatabase, DatabaseTarget, sql

DSN = os.environ.get("DBKIT_DSN", "postgresql+psycopg://localhost/postgres")
TARGET = DatabaseTarget(database="app", role="read")


async def main() -> None:
    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": DSN}}},
            "defaults": {"pool": {"pgbouncer_compatible": True}},
        }
    )
    await db.start()
    try:
        # Verify the flag actually reached the driver connection.
        entry = next(iter(db._registry._entries.values()))
        async with entry.engine.connect() as conn:
            raw = await conn.get_raw_connection()
            print("psycopg prepare_threshold:", raw.driver_connection.prepare_threshold)

        # Fully functional — repeated identical statements never get server-side prepared,
        # which is exactly the point under transaction pooling.
        for i in range(5):
            value = await db.fetch_value(sql("SELECT :n"), {"n": i}, target=TARGET)
            assert value == i
        print("5 repeated statements with pgbouncer_compatible=True: OK")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
