"""Consumer micro-batching: aggregate many concurrent writers into one bulk write (§17.1). Run:

DBKIT_DSN=postgresql+psycopg://localhost/postgres python examples/batch_collector.py
"""

from __future__ import annotations

import asyncio
import os

from dbkit import AsyncDatabase, DatabaseTarget, sql
from dbkit.integrations import BatchCollector

DSN = os.environ.get("DBKIT_DSN", "postgresql+psycopg://localhost/postgres")
TARGET = DatabaseTarget(database="app", role="write")


async def main() -> None:
    db = AsyncDatabase.from_config({"databases": {"app": {"primary": {"url": DSN}}}})
    await db.start()
    try:
        await db.execute(
            sql("CREATE TABLE IF NOT EXISTS dbkit_events (id int, kind text)"), target=TARGET
        )
        await db.execute(sql("TRUNCATE dbkit_events"), target=TARGET)

        flush_count = 0

        async def flush(items: list[tuple[int, str]]) -> None:
            nonlocal flush_count
            flush_count += 1
            await db.copy_records("dbkit_events", ["id", "kind"], items, target=TARGET)
            print(f"  flush #{flush_count}: wrote {len(items)} events in one COPY")

        # Buffer up to 200 items, or flush after 30ms — whichever comes first.
        collector: BatchCollector = BatchCollector(flush, max_size=200, max_delay_ms=30)

        # Simulate 25 concurrent "message handlers" each producing one event.
        async def handler(i: int) -> None:
            await asyncio.sleep((i % 5) * 0.01)  # stagger arrivals
            await collector.add((i, "click" if i % 2 == 0 else "view"))

        await asyncio.gather(*[handler(i) for i in range(500)])
        await collector.close()  # flush anything still buffered

        count = await db.fetch_value(sql("SELECT count(*) FROM dbkit_events"), target=TARGET)
        print(f"\n500 concurrent handlers -> {flush_count} database writes, {count} rows persisted")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
