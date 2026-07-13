"""Latency — per-operation P50/P95/P99 for a small read, open-loop paced (async).

Open-loop pacing (fire on an absolute schedule, don't wait for the previous op) exposes tail
latency that a closed loop hides. A warmup window is discarded before percentiles.
"""

from __future__ import annotations

import asyncio
import time

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from dbkit import AsyncDatabase, DatabaseTarget, Query, sql

from . import _common, _results, _stats

N = 4000
WARMUP = 200
RATE = 2000  # target ops/s pacing
TARGET = DatabaseTarget(database="app", role="read")
GET = Query(name="bench.get", statement=sql("SELECT v FROM dbkit_bench WHERE id = 1"))


async def _setup(dsn: str) -> None:
    engine = create_async_engine(dsn)
    async with engine.begin() as conn:
        await conn.execute(
            text("CREATE TABLE IF NOT EXISTS dbkit_bench (id int PRIMARY KEY, v int)")
        )
        await conn.execute(text("INSERT INTO dbkit_bench VALUES (1, 42) ON CONFLICT DO NOTHING"))
    await engine.dispose()


async def _run_async(dsn: str) -> dict[str, float]:
    await _setup(dsn)
    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": dsn}}},
            "defaults": {
                "pool": {"size": 20, "max_overflow": 0},
                "observability": {"metrics": False, "slow_query_ms": 1e9},
            },
        }
    )
    await db.start()
    latencies: list[float] = []
    try:
        t0 = time.monotonic()
        for i in range(N):
            due = t0 + i / RATE
            now = time.monotonic()
            if due > now:
                await asyncio.sleep(due - now)
            op_start = time.monotonic()
            await db.fetch_value(GET, target=TARGET)
            latencies.append((time.monotonic() - op_start) * 1000)  # ms
    finally:
        await db.close()

    pct = _stats.percentiles(latencies[WARMUP:], points=(50, 95, 99))
    _common.rule("latency — small read (async, paced)")
    for k, v in pct.items():
        print(f"  {k:>4} {v:>8.3f} ms")
    return {
        "lat_read_p50_ms": pct.get("p50", 0.0),
        "lat_read_p95_ms": pct.get("p95", 0.0),
        "lat_read_p99_ms": pct.get("p99", 0.0),
        "lat_read_max_ms": pct.get("max", 0.0),
    }


def run_all(dsn: str) -> dict[str, float]:
    return asyncio.run(_run_async(dsn))


def main(dsn: str | None = None) -> dict[str, float]:
    with _common.dsn_context(dsn) as resolved:
        return run_all(resolved)


if __name__ == "__main__":
    import sys

    _results.save(main(sys.argv[1] if len(sys.argv) > 1 else None), _stats.env_fingerprint())
