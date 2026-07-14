"""Pool exhaustion under concurrent load (§17, production-readiness review §4).

Measures what actually happens when concurrent demand exceeds ``pool.size + max_overflow``:
every request beyond capacity should fail fast with a classified ``DatabasePoolTimeoutError``
within roughly ``pool.timeout_seconds`` — not hang indefinitely, and not surface a raw
SQLAlchemy ``TimeoutError``. Requires a real PostgreSQL instance. Run:

    DBKIT_TEST_DSN=postgresql+psycopg://dbkit:dbkit@localhost:55432/dbkit \\
        uv run python -m benchmarks.bench_pool_exhaustion
"""

from __future__ import annotations

import asyncio
import os
import time

from dbkit import AsyncDatabase, DatabaseTarget, sql
from dbkit.errors import DatabasePoolTimeoutError

DSN = os.environ.get("DBKIT_TEST_DSN", "postgresql+psycopg://dbkit:dbkit@localhost:55432/dbkit")
TARGET = DatabaseTarget(database="app", role="write")
POOL_SIZE = 2
MAX_OVERFLOW = 1
CAPACITY = POOL_SIZE + MAX_OVERFLOW
POOL_TIMEOUT_SECONDS = 1.0
CONCURRENCY = 10
HOLD_SECONDS = 2.0


async def hold_connection(db: AsyncDatabase, i: int) -> tuple[int, str, float]:
    start = time.monotonic()
    try:
        async with db.connection(target=TARGET) as conn:
            await conn.execute(sql("SELECT pg_sleep(:s)"), {"s": HOLD_SECONDS})
        return i, "ok", time.monotonic() - start
    except DatabasePoolTimeoutError:
        return i, "pool_timeout", time.monotonic() - start


async def main(dsn: str | None = None) -> None:
    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": dsn or DSN}}},
            "defaults": {
                "query_timeout_seconds": HOLD_SECONDS + 3.0,
                "pool": {
                    "size": POOL_SIZE,
                    "max_overflow": MAX_OVERFLOW,
                    "timeout_seconds": POOL_TIMEOUT_SECONDS,
                },
            },
        }
    )
    await db.start()
    try:
        results = await asyncio.gather(*(hold_connection(db, i) for i in range(CONCURRENCY)))
        ok = [r for r in results if r[1] == "ok"]
        timed_out = [r for r in results if r[1] == "pool_timeout"]

        print(f"capacity: {CAPACITY} connections (size={POOL_SIZE} + max_overflow={MAX_OVERFLOW})")
        print(f"concurrent requests: {CONCURRENCY}, each holding a connection {HOLD_SECONDS}s")
        print(f"succeeded: {len(ok)} (expect {CAPACITY})")
        print(
            f"classified DatabasePoolTimeoutError: {len(timed_out)} (expect {CONCURRENCY - CAPACITY})"
        )
        for i, status, elapsed in sorted(results, key=lambda r: r[0]):
            print(f"  request {i}: {status} after {elapsed:.2f}s")

        assert len(ok) == CAPACITY, "exactly `capacity` requests should get a connection"
        assert len(timed_out) == CONCURRENCY - CAPACITY, "the rest should time out, not hang"
        assert all(elapsed < POOL_TIMEOUT_SECONDS + 1.0 for _, _, elapsed in timed_out), (
            "a pool timeout should fire near pool.timeout_seconds, not hang indefinitely"
        )
        print(
            "\nPASS: excess demand fails fast with a classified DatabasePoolTimeoutError; "
            "capacity requests succeed. No hang, no raw unclassified exception."
        )
    finally:
        await db.close()


def run_all(dsn: str) -> dict[str, float]:
    """SUITES adapter (§14): this is a pass/fail scenario assertion, not a rate-producing
    benchmark, so it contributes no metrics — it just raises if the pool-exhaustion contract
    (fail fast, classified, no hang) breaks."""
    asyncio.run(main(dsn))
    return {}


if __name__ == "__main__":
    asyncio.run(main())
