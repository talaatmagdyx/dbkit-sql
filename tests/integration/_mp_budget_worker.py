"""Standalone subprocess worker for ``test_multiprocess_connection_budget.py`` (task #92,
performance review §10.3 connection-budget formula). Not collected by pytest — invoked as a
real OS process via ``sys.executable``, so the connections it opens are genuinely independent
of the parent test process's own pool, exactly like separate application replicas would be.

Drives its pool to full capacity (``size + max_overflow`` concurrent holds), prints ``READY``
once every connection is actually checked out, waits to be told to release, then closes cleanly.

    python -m tests.integration._mp_budget_worker <dsn> --size 3 --overflow 2 --hold 2.0
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from dbkit import AsyncDatabase, DatabaseTarget, sql

TARGET = DatabaseTarget(database="app", role="write")


async def _main(dsn: str, size: int, overflow: int, hold: float) -> None:
    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": dsn}}},
            "defaults": {
                "query_timeout_seconds": hold + 10.0,
                "pool": {"size": size, "max_overflow": overflow, "timeout_seconds": 10.0},
                "observability": {"metrics": False, "slow_query_ms": 1e9},
            },
        }
    )
    await db.start()
    capacity = size + overflow

    async def _hold_one() -> None:
        await db.fetch_value(sql("SELECT pg_sleep(:s)"), {"s": hold}, target=TARGET)

    tasks = [asyncio.create_task(_hold_one()) for _ in range(capacity)]
    while db.pool_status()[0].checked_out < capacity:
        await asyncio.sleep(0.02)
    print("READY", flush=True)

    await asyncio.gather(*tasks)
    await db.close()
    print("DONE", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dsn")
    parser.add_argument("--size", type=int, required=True)
    parser.add_argument("--overflow", type=int, required=True)
    parser.add_argument("--hold", type=float, required=True)
    args = parser.parse_args()
    asyncio.run(_main(args.dsn, args.size, args.overflow, args.hold))
    sys.exit(0)
