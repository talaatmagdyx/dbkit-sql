"""Concurrency-scaling load test — real capacity data, not an estimate (performance review §1,
§15 test #1/#3). Runs a simple indexed read at increasing concurrency against live PostgreSQL,
under two pool configurations, to find where throughput actually saturates and why.

    DBKIT_TEST_DSN=postgresql+psycopg://dbkit:dbkit@localhost:55432/dbkit \\
        uv run python -m benchmarks.bench_concurrency_scaling
"""

from __future__ import annotations

import asyncio
import os
import time

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from dbkit import AsyncDatabase, DatabaseTarget, Query, sql

from . import _common, _stats

DSN = os.environ.get("DBKIT_TEST_DSN", "postgresql+psycopg://dbkit:dbkit@localhost:55432/dbkit")
TARGET = DatabaseTarget(database="app", role="read")
GET = Query(name="bench.concurrency.get", statement=sql("SELECT v FROM dbkit_bench WHERE id = 1"))
CONCURRENCY_LEVELS = (1, 10, 50, 100, 250, 500)
WINDOW_SECONDS = 2.0


async def _setup(dsn: str) -> None:
    engine = create_async_engine(dsn)
    async with engine.begin() as conn:
        await conn.execute(
            text("CREATE TABLE IF NOT EXISTS dbkit_bench (id int PRIMARY KEY, v int)")
        )
        await conn.execute(text("INSERT INTO dbkit_bench VALUES (1, 42) ON CONFLICT DO NOTHING"))
    await engine.dispose()


async def _worker(
    db: AsyncDatabase, stop_at: float, latencies: list[float], errors: list[str]
) -> int:
    completed = 0
    while time.monotonic() < stop_at:
        start = time.monotonic()
        try:
            await db.fetch_value(GET, target=TARGET, timeout=10.0)
            latencies.append((time.monotonic() - start) * 1000)
            completed += 1
        except Exception as exc:
            errors.append(type(exc).__name__)
    return completed


async def _sample_pool_utilization(db: AsyncDatabase, stop_at: float, peak: list[float]) -> None:
    """Samples pool utilization *while the load is running*, not after — a one-shot
    ``pool_status()`` call taken after ``asyncio.gather`` returns always reads 0%, since every
    connection has already been returned to the pool by then."""
    while time.monotonic() < stop_at:
        pool = db.pool_status()
        if pool:
            peak.append(pool[0].utilization)
        await asyncio.sleep(0.05)


async def _run_level(db: AsyncDatabase, concurrency: int) -> dict[str, float]:
    latencies: list[float] = []
    errors: list[str] = []
    utilization_samples: list[float] = []
    stop_at = time.monotonic() + WINDOW_SECONDS
    start = time.monotonic()
    sampler = asyncio.create_task(_sample_pool_utilization(db, stop_at, utilization_samples))
    results = await asyncio.gather(
        *(_worker(db, stop_at, latencies, errors) for _ in range(concurrency))
    )
    await sampler
    elapsed = time.monotonic() - start
    total = sum(results)
    pct = _stats.percentiles(latencies, points=(50, 95, 99)) if latencies else {}
    return {
        "concurrency": concurrency,
        "throughput_ops_s": total / elapsed,
        "p50_ms": pct.get("p50", 0.0),
        "p95_ms": pct.get("p95", 0.0),
        "p99_ms": pct.get("p99", 0.0),
        "error_count": len(errors),
        "pool_utilization_peak": max(utilization_samples) if utilization_samples else 0.0,
    }


async def _run_pool_config(dsn: str, *, size: int, max_overflow: int) -> list[dict[str, float]]:
    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": dsn}}},
            "defaults": {
                "pool": {"size": size, "max_overflow": max_overflow, "timeout_seconds": 15.0},
                "observability": {"metrics": False, "slow_query_ms": 1e9},
            },
        }
    )
    await db.start()
    rows: list[dict[str, float]] = []
    try:
        await db.fetch_value(GET, target=TARGET)  # warmup
        for c in CONCURRENCY_LEVELS:
            row = await _run_level(db, c)
            rows.append(row)
            print(
                f"    concurrency={c:>5}  throughput={row['throughput_ops_s']:>9,.0f} ops/s  "
                f"p50={row['p50_ms']:>6.2f}ms  p99={row['p99_ms']:>7.2f}ms  "
                f"errors={row['error_count']:>4}  peak_pool_util={row['pool_utilization_peak']:.0%}"
            )
    finally:
        await db.close()
    return rows


async def _run_async(dsn: str) -> dict[str, float]:
    await _setup(dsn)
    _common.rule("concurrency scaling — default pool (size=10, max_overflow=5, capacity=15)")
    default_rows = await _run_pool_config(dsn, size=10, max_overflow=5)
    _common.rule("concurrency scaling — large pool (size=50, max_overflow=50, capacity=100)")
    large_rows = await _run_pool_config(dsn, size=50, max_overflow=50)

    default_peak = max(default_rows, key=lambda r: r["throughput_ops_s"])
    large_peak = max(large_rows, key=lambda r: r["throughput_ops_s"])
    print(
        f"\n  default-pool peak: {default_peak['throughput_ops_s']:,.0f} ops/s "
        f"at concurrency={default_peak['concurrency']:.0f}"
    )
    print(
        f"  large-pool peak:   {large_peak['throughput_ops_s']:,.0f} ops/s "
        f"at concurrency={large_peak['concurrency']:.0f}"
    )

    metrics: dict[str, float] = {
        "concurrency_default_pool_peak_ops_s": default_peak["throughput_ops_s"]
    }
    metrics["concurrency_large_pool_peak_ops_s"] = large_peak["throughput_ops_s"]
    for row in default_rows:
        metrics[f"concurrency_default_pool_c{int(row['concurrency'])}_ops_s"] = row[
            "throughput_ops_s"
        ]
    for row in large_rows:
        metrics[f"concurrency_large_pool_c{int(row['concurrency'])}_ops_s"] = row[
            "throughput_ops_s"
        ]
    return metrics


def run_all(dsn: str) -> dict[str, float]:
    return asyncio.run(_run_async(dsn))


def main(dsn: str | None = None) -> dict[str, float]:
    with _common.dsn_context(dsn) as resolved:
        return run_all(resolved)


if __name__ == "__main__":
    import sys

    main(sys.argv[1] if len(sys.argv) > 1 else None)
