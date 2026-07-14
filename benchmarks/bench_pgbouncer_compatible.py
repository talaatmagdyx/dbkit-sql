"""Cost of ``pgbouncer_compatible=True`` (production-readiness review §4).

``pgbouncer_compatible=True`` disables driver-side prepared-statement autoprep (psycopg's
``prepare_threshold``, asyncpg's ``statement_cache_size``) — required under PgBouncer
*transaction* pooling, where a logical connection may hit a different physical backend every
transaction, so a client-cached prepared statement can target the wrong one. This measures what
that costs: the same repeated small read, paced identically, with the setting on vs off. Requires
a real PostgreSQL instance (no PgBouncer process needed — the setting only changes client-side
driver behavior, which is the same whether or not a real proxy sits in front). Run:

    DBKIT_TEST_DSN=postgresql+psycopg://dbkit:dbkit@localhost:55432/dbkit \\
        uv run python -m benchmarks.bench_pgbouncer_compatible
"""

from __future__ import annotations

import asyncio
import time

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from dbkit import AsyncDatabase, DatabaseTarget, Query, sql

from . import _common, _stats

N = 2000
WARMUP = 200
RATE = 2000
TARGET = DatabaseTarget(database="app", role="read")
GET = Query(name="bench.pgbouncer.get", statement=sql("SELECT v FROM dbkit_bench WHERE id = 1"))


async def _setup(dsn: str) -> None:
    engine = create_async_engine(dsn)
    async with engine.begin() as conn:
        await conn.execute(
            text("CREATE TABLE IF NOT EXISTS dbkit_bench (id int PRIMARY KEY, v int)")
        )
        await conn.execute(text("INSERT INTO dbkit_bench VALUES (1, 42) ON CONFLICT DO NOTHING"))
    await engine.dispose()


async def _run_one(dsn: str, *, pgbouncer_compatible: bool) -> dict[str, float]:
    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": dsn}}},
            "defaults": {
                "pool": {
                    "size": 10,
                    "max_overflow": 0,
                    "pgbouncer_compatible": pgbouncer_compatible,
                },
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
            latencies.append((time.monotonic() - op_start) * 1000)
    finally:
        await db.close()
    return _stats.percentiles(latencies[WARMUP:], points=(50, 95, 99))


async def _run_async(dsn: str) -> dict[str, dict[str, float]]:
    await _setup(dsn)
    off = await _run_one(dsn, pgbouncer_compatible=False)
    on = await _run_one(dsn, pgbouncer_compatible=True)
    return {
        "autoprep_on (pgbouncer_compatible=False)": off,
        "autoprep_off (pgbouncer_compatible=True)": on,
    }


def main(dsn: str | None = None) -> dict[str, dict[str, float]]:
    with _common.dsn_context(dsn) as resolved:
        results = asyncio.run(_run_async(resolved))
        _common.rule("pgbouncer_compatible cost — small read, paced, repeated on one connection")
        for label, pct in results.items():
            print(f"  {label}:")
            for k, v in pct.items():
                print(f"    {k:>4} {v:>8.3f} ms")
        p50_off = results["autoprep_on (pgbouncer_compatible=False)"].get("p50", 0.0)
        p50_on = results["autoprep_off (pgbouncer_compatible=True)"].get("p50", 0.0)
        delta = p50_on - p50_off
        print(
            f"\n  p50 delta (pgbouncer_compatible=True vs False): {delta:+.3f} ms "
            f"({'higher' if delta > 0 else 'lower or equal'} — this is the per-query cost of "
            "disabling client-side autoprep, on localhost)"
        )
        return results


def run_all(dsn: str) -> dict[str, float]:
    """SUITES adapter (§14): flattens the two nested percentile dicts into the flat
    ``{metric_name: float}`` shape ``_results.save``/the regression-delta printer expect."""
    results = main(dsn)
    flat: dict[str, float] = {}
    for label, pct in results.items():
        prefix = "pgbouncer_autoprep_off" if "True" in label else "pgbouncer_autoprep_on"
        for point, value in pct.items():
            flat[f"{prefix}_{point}_ms"] = value
    return flat


if __name__ == "__main__":
    import sys

    main(sys.argv[1] if len(sys.argv) > 1 else None)
