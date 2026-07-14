"""Batch writes — per-row execute vs execute_many (rows/s). Shows the batching win (§19)."""

from __future__ import annotations

import asyncio
import time

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from dbkit import AsyncDatabase, DatabaseTarget, Query, sql

from . import _common, _results, _stats

ROWS = 5000
BATCH = 1000
REPS = 3
TARGET = DatabaseTarget(database="app", role="write")
INSERT = Query(
    name="bench.batch_insert",
    statement=sql("INSERT INTO dbkit_batch (id, v) VALUES (:id, :v)"),
    operation="write",
)


async def _setup(dsn: str) -> None:
    engine = create_async_engine(dsn)
    async with engine.begin() as conn:
        await conn.execute(
            text("CREATE TABLE IF NOT EXISTS dbkit_batch (id bigint PRIMARY KEY, v int)")
        )
    await engine.dispose()


async def _per_row(db: AsyncDatabase) -> float:
    await db.execute(sql("TRUNCATE dbkit_batch"), target=TARGET)
    start = time.monotonic()
    async with db.transaction(target=TARGET) as tx:
        for i in range(ROWS):
            await tx.execute(INSERT, {"id": i, "v": i})
    return ROWS / (time.monotonic() - start)


async def _execute_many(db: AsyncDatabase) -> float:
    await db.execute(sql("TRUNCATE dbkit_batch"), target=TARGET)
    rows = [{"id": i, "v": i} for i in range(ROWS)]
    start = time.monotonic()
    for off in range(0, ROWS, BATCH):
        await db.execute_many(INSERT, rows[off : off + BATCH], target=TARGET)
    return ROWS / (time.monotonic() - start)


async def _copy(db: AsyncDatabase) -> float:
    await db.execute(sql("TRUNCATE dbkit_batch"), target=TARGET)
    records = [(i, i) for i in range(ROWS)]
    start = time.monotonic()
    await db.copy_records("dbkit_batch", ["id", "v"], records, target=TARGET)
    return ROWS / (time.monotonic() - start)


async def _run_async(dsn: str) -> dict[str, float]:
    await _setup(dsn)
    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": dsn}}},
            "defaults": {
                "observability": {"metrics": False, "slow_query_ms": 1e9},
                "transaction_timeout_seconds": 30.0,
                "query_timeout_seconds": 30.0,
            },
        }
    )
    await db.start()
    try:
        per_row = [await _per_row(db) for _ in range(REPS)]
        many = [await _execute_many(db) for _ in range(REPS)]
        copy = [await _copy(db) for _ in range(REPS)]
    finally:
        await db.close()
    sp, sm, sc = _stats.robust(per_row), _stats.robust(many), _stats.robust(copy)
    _common.rule("batch insert (async)")
    print(f"  per-row        {_stats.fmt_rate(sp, 'rows/s')}")
    print(f"  execute_many   {_stats.fmt_rate(sm, 'rows/s')}")
    print(f"  COPY           {_stats.fmt_rate(sc, 'rows/s')}")
    if sp["median"]:
        print(
            f"  execute_many speedup: {sm['median'] / sp['median']:.1f}x  "
            f"COPY speedup: {sc['median'] / sp['median']:.1f}x"
        )
    return {
        "batch_per_row_rows_s": sp["median"],
        "batch_execute_many_rows_s": sm["median"],
        "batch_copy_rows_s": sc["median"],
    }


def run_all(dsn: str) -> dict[str, float]:
    return asyncio.run(_run_async(dsn))


def main(dsn: str | None = None) -> dict[str, float]:
    with _common.dsn_context(dsn) as resolved:
        return run_all(resolved)


if __name__ == "__main__":
    import sys

    _results.save(main(sys.argv[1] if len(sys.argv) > 1 else None), _stats.env_fingerprint())
