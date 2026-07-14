"""PostgreSQL COPY — the fastest bulk-ingest path (§19.2). Run:

DBKIT_DSN=postgresql+psycopg://localhost/postgres python examples/copy_ingest.py
"""

from __future__ import annotations

import asyncio
import os
import time

from dbkit import AsyncDatabase, DatabaseTarget, sql

DSN = os.environ.get("DBKIT_DSN", "postgresql+psycopg://localhost/postgres")
TARGET = DatabaseTarget(database="app", role="write")

N = 50_000


async def main() -> None:
    db = AsyncDatabase.from_config({"databases": {"app": {"primary": {"url": DSN}}}})
    await db.start()
    try:
        await db.execute(
            sql("CREATE TABLE IF NOT EXISTS dbkit_copy_demo (id int, payload text)"), target=TARGET
        )
        await db.execute(sql("TRUNCATE dbkit_copy_demo"), target=TARGET)

        # COPY streams rows to the server without materializing them all in memory —
        # records can be any iterable (generator, async generator, list) of row tuples.
        records = ((i, f"payload-{i}") for i in range(N))
        start = time.monotonic()
        result = await db.copy_records("dbkit_copy_demo", ["id", "payload"], records, target=TARGET)
        elapsed = time.monotonic() - start
        print(
            f"COPY: wrote {result.row_count:,} rows in {elapsed * 1000:.1f}ms "
            f"({result.row_count / elapsed:,.0f} rows/s)"
        )

        # Compare against per-row inserts for the same data.
        await db.execute(sql("TRUNCATE dbkit_copy_demo"), target=TARGET)
        start = time.monotonic()
        async with db.transaction(target=TARGET) as tx:
            for i in range(2000):  # smaller N — per-row is much slower
                await tx.execute(
                    sql("INSERT INTO dbkit_copy_demo (id, payload) VALUES (:id, :p)"),
                    {"id": i, "p": f"payload-{i}"},
                )
        elapsed_per_row = time.monotonic() - start
        per_row_rate = 2000 / elapsed_per_row
        copy_rate = result.row_count / elapsed
        print(f"per-row insert: {per_row_rate:,.0f} rows/s")
        print(f"COPY is ~{copy_rate / per_row_rate:.0f}x faster than per-row inserts")

        count = await db.fetch_value(sql("SELECT count(*) FROM dbkit_copy_demo"), target=TARGET)
        print(f"final row count: {count}")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
