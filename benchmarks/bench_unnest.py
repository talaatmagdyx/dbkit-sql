"""``execute_many`` vs ``strategy="unnest"`` insert throughput (§19, performance review §9).

Prior to this benchmark existing, docs/CHANGELOG cited a "~32x faster than execute_many at
20k rows" figure for the ``unnest`` strategy with **no committed benchmark backing it** — not
even at a different row count. This file exists to make that claim reproducible: run it and
compare against the number in ``docs/roadmap.md``/``CHANGELOG.md``.
"""

from __future__ import annotations

import asyncio
import time

from sqlalchemy import BigInteger, Column, Integer, MetaData, Table, text
from sqlalchemy.ext.asyncio import create_async_engine

from dbkit import AsyncDatabase, DatabaseTarget

from . import _common, _results, _stats

ROWS = 20_000
REPS = 3
TARGET = DatabaseTarget(database="app", role="write")

_md = MetaData()
TABLE = Table(
    "dbkit_bench_unnest",
    _md,
    Column("id", BigInteger, primary_key=True),
    Column("v", Integer),
)


async def _setup(dsn: str) -> None:
    engine = create_async_engine(dsn)
    async with engine.begin() as conn:
        await conn.execute(
            text("CREATE TABLE IF NOT EXISTS dbkit_bench_unnest (id bigint PRIMARY KEY, v int)")
        )
    await engine.dispose()


async def _execute_many(db: AsyncDatabase) -> float:
    await db.execute(text("TRUNCATE dbkit_bench_unnest"), target=TARGET)
    rows = [{"id": i, "v": i} for i in range(ROWS)]
    start = time.monotonic()
    await db.insert_many(TABLE, rows, target=TARGET, strategy="execute_many", mode="atomic")
    return ROWS / (time.monotonic() - start)


async def _unnest(db: AsyncDatabase) -> float:
    await db.execute(text("TRUNCATE dbkit_bench_unnest"), target=TARGET)
    rows = [{"id": i, "v": i} for i in range(ROWS)]
    start = time.monotonic()
    await db.insert_many(TABLE, rows, target=TARGET, strategy="unnest", mode="atomic")
    return ROWS / (time.monotonic() - start)


async def _run_async(dsn: str) -> dict[str, float]:
    await _setup(dsn)
    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": dsn}}},
            "defaults": {
                "observability": {"metrics": False, "slow_query_ms": 1e9},
                "transaction_timeout_seconds": 60.0,
                "query_timeout_seconds": 60.0,
            },
        }
    )
    await db.start()
    try:
        many = [await _execute_many(db) for _ in range(REPS)]
        unnest = [await _unnest(db) for _ in range(REPS)]
    finally:
        await db.close()
    sm, su = _stats.robust(many), _stats.robust(unnest)
    _common.rule(f"unnest vs execute_many — {ROWS:,} rows")
    print(f"  execute_many   {_stats.fmt_rate(sm, 'rows/s')}")
    print(f"  unnest         {_stats.fmt_rate(su, 'rows/s')}")
    if sm["median"]:
        print(f"  unnest speedup: {su['median'] / sm['median']:.1f}x")
    return {
        "unnest_execute_many_rows_s": sm["median"],
        "unnest_unnest_rows_s": su["median"],
    }


def run_all(dsn: str) -> dict[str, float]:
    return asyncio.run(_run_async(dsn))


def main(dsn: str | None = None) -> dict[str, float]:
    with _common.dsn_context(dsn) as resolved:
        return run_all(resolved)


if __name__ == "__main__":
    import sys

    _results.save(main(sys.argv[1] if len(sys.argv) > 1 else None), _stats.env_fingerprint())
