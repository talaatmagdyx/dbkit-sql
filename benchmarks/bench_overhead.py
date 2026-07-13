"""Overhead A/B: dbkit vs the raw driver and raw SQLAlchemy Core (§33.1-33.2).

Measures the per-operation cost dbkit adds on top of what it wraps, for a small indexed
read. Three lanes, run interleaved rep-by-rep so runner drift biases all equally:

* ``raw_psycopg``   — one persistent async psycopg connection (the driver floor).
* ``raw_sqlalchemy``— SQLAlchemy Core, ``engine.connect()`` per op (dbkit's checkout model).
* ``dbkit``         — ``AsyncDatabase.fetch_value`` per op.

Headline: ``overhead_pct`` = dbkit vs raw_sqlalchemy (the fair comparison — same pool model).
"""

from __future__ import annotations

import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from dbkit import AsyncDatabase, DatabaseTarget, Query, sql

from . import _common, _results, _stats

N = 2000
REPS = 5
TARGET = DatabaseTarget(database="app", role="read")
GET = Query(name="bench.get", statement=sql("SELECT v FROM dbkit_bench WHERE id = 1"), timeout=5.0)


async def _setup(dsn: str) -> None:
    engine = create_async_engine(dsn)
    async with engine.begin() as conn:
        await conn.execute(
            text("CREATE TABLE IF NOT EXISTS dbkit_bench (id int PRIMARY KEY, v int)")
        )
        await conn.execute(
            text("INSERT INTO dbkit_bench (id, v) VALUES (1, 42) ON CONFLICT DO NOTHING")
        )
    await engine.dispose()


async def _raw_psycopg(dsn: str) -> float:
    import time

    import psycopg

    raw = dsn.replace("postgresql+psycopg://", "postgresql://")
    aconn = await psycopg.AsyncConnection.connect(raw)
    try:
        # warmup
        async with aconn.cursor() as cur:
            await cur.execute("SELECT v FROM dbkit_bench WHERE id = 1")
            await cur.fetchone()
        start = time.monotonic()
        for _ in range(N):
            async with aconn.cursor() as cur:
                await cur.execute("SELECT v FROM dbkit_bench WHERE id = 1")
                await cur.fetchone()
        elapsed = time.monotonic() - start
    finally:
        await aconn.close()
    return N / elapsed


async def _raw_sqlalchemy(engine) -> float:
    import time

    async with engine.connect() as c:  # warmup
        await c.execute(text("SELECT v FROM dbkit_bench WHERE id = 1"))
    start = time.monotonic()
    for _ in range(N):
        async with engine.connect() as c:
            res = await c.execute(text("SELECT v FROM dbkit_bench WHERE id = 1"))
            res.first()
    return N / (time.monotonic() - start)


async def _dbkit(db: AsyncDatabase) -> float:
    import time

    await db.fetch_value(GET, target=TARGET)  # warmup
    start = time.monotonic()
    for _ in range(N):
        await db.fetch_value(GET, target=TARGET)
    return N / (time.monotonic() - start)


async def _run_async(dsn: str) -> dict[str, float]:
    await _setup(dsn)
    engine = create_async_engine(dsn, pool_size=5, max_overflow=0)
    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": dsn}}},
            "defaults": {"observability": {"metrics": False, "slow_query_ms": 1e9}},
        }
    )
    await db.start()
    try:
        raw_pg, raw_sa, kit = [], [], []
        for _ in range(REPS):
            raw_pg.append(await _raw_psycopg(dsn))
            raw_sa.append(await _raw_sqlalchemy(engine))
            kit.append(await _dbkit(db))
    finally:
        await db.close()
        await engine.dispose()

    s_pg, s_sa, s_kit = _stats.robust(raw_pg), _stats.robust(raw_sa), _stats.robust(kit)
    overhead = (s_sa["median"] - s_kit["median"]) / s_sa["median"] * 100 if s_sa["median"] else 0.0

    _common.rule("overhead — small indexed read (async)")
    print(f"  raw psycopg     {_stats.fmt_rate(s_pg)}")
    print(f"  raw sqlalchemy  {_stats.fmt_rate(s_sa)}")
    print(f"  dbkit           {_stats.fmt_rate(s_kit)}")
    print(f"  dbkit overhead vs raw sqlalchemy: {overhead:+.1f}%")

    return {
        "overhead_raw_psycopg_ops_s": s_pg["median"],
        "overhead_raw_sqlalchemy_ops_s": s_sa["median"],
        "overhead_dbkit_ops_s": s_kit["median"],
        "overhead_vs_sqlalchemy_pct": overhead,
    }


def run_all(dsn: str) -> dict[str, float]:
    return asyncio.run(_run_async(dsn))


def main(dsn: str | None = None) -> dict[str, float]:
    with _common.dsn_context(dsn) as resolved:
        return run_all(resolved)


if __name__ == "__main__":
    import sys

    metrics = main(sys.argv[1] if len(sys.argv) > 1 else None)
    _results.save(metrics, _stats.env_fingerprint())
