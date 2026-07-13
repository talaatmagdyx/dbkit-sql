"""Throughput — sustained ops/s for small reads and single-row inserts (async + sync)."""

from __future__ import annotations

import asyncio
import time

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from dbkit import AsyncDatabase, Database, DatabaseTarget, Query, sql

from . import _common, _results, _stats

N_READ = 5000
N_WRITE = 3000
REPS = 3
TARGET = DatabaseTarget(database="app", role="write")

GET = Query(name="bench.get", statement=sql("SELECT v FROM dbkit_bench WHERE id = 1"))
INSERT = Query(
    name="bench.insert",
    statement=sql("INSERT INTO dbkit_tp (id, v) VALUES (:id, :v)"),
    operation="write",
)


async def _setup(dsn: str) -> None:
    engine = create_async_engine(dsn)
    async with engine.begin() as conn:
        await conn.execute(
            text("CREATE TABLE IF NOT EXISTS dbkit_bench (id int PRIMARY KEY, v int)")
        )
        await conn.execute(text("INSERT INTO dbkit_bench VALUES (1, 42) ON CONFLICT DO NOTHING"))
        await conn.execute(
            text("CREATE TABLE IF NOT EXISTS dbkit_tp (id bigint PRIMARY KEY, v int)")
        )
    await engine.dispose()


def _cfg(dsn: str) -> dict:
    return {
        "databases": {"app": {"primary": {"url": dsn}}},
        "defaults": {
            "pool": {"size": 10, "max_overflow": 0},
            "observability": {"metrics": False, "slow_query_ms": 1e9},
        },
    }


async def _async_read(db: AsyncDatabase) -> float:
    await db.fetch_value(GET, target=TARGET)
    start = time.monotonic()
    for _ in range(N_READ):
        await db.fetch_value(GET, target=TARGET)
    return N_READ / (time.monotonic() - start)


async def _async_write(db: AsyncDatabase) -> float:
    await db.execute(sql("TRUNCATE dbkit_tp"), target=TARGET)
    start = time.monotonic()
    for i in range(N_WRITE):
        await db.execute(INSERT, {"id": i, "v": i}, target=TARGET)
    return N_WRITE / (time.monotonic() - start)


async def run_async(dsn: str) -> dict[str, float]:
    await _setup(dsn)
    db = AsyncDatabase.from_config(_cfg(dsn))
    await db.start()
    try:
        reads = [await _async_read(db) for _ in range(REPS)]
        writes = [await _async_write(db) for _ in range(REPS)]
    finally:
        await db.close()
    sr, sw = _stats.robust(reads), _stats.robust(writes)
    _common.rule("throughput (async)")
    print(f"  read  {_stats.fmt_rate(sr)}")
    print(f"  write {_stats.fmt_rate(sw)}")
    return {"tp_async_read_ops_s": sr["median"], "tp_async_write_ops_s": sw["median"]}


def _sync_read(db: Database) -> float:
    db.fetch_value(GET, target=TARGET)
    start = time.monotonic()
    for _ in range(N_READ):
        db.fetch_value(GET, target=TARGET)
    return N_READ / (time.monotonic() - start)


def run_sync(dsn: str) -> dict[str, float]:
    db = Database.from_config(_cfg(dsn))
    db.start()
    try:
        reads = [_sync_read(db) for _ in range(REPS)]
    finally:
        db.close()
    sr = _stats.robust(reads)
    _common.rule("throughput (sync)")
    print(f"  read  {_stats.fmt_rate(sr)}")
    return {"tp_sync_read_ops_s": sr["median"]}


def run_all(dsn: str) -> dict[str, float]:
    metrics = asyncio.run(run_async(dsn))
    metrics.update(run_sync(dsn))
    return metrics


def main(dsn: str | None = None) -> dict[str, float]:
    with _common.dsn_context(dsn) as resolved:
        return run_all(resolved)


if __name__ == "__main__":
    import sys

    _results.save(main(sys.argv[1] if len(sys.argv) > 1 else None), _stats.env_fingerprint())
