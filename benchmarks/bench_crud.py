"""CRUD benchmark — INSERT, SELECT (point + range), UPDATE, UPSERT, DELETE.

One clean report covering the operations every application actually does, each measured for
both throughput (ops/s) and per-operation latency (P50/P95/P99), on the async frontend and
(for the two most common operations) the sync frontend too.

    python -m benchmarks --only crud --dsn postgresql+psycopg://localhost/postgres
"""

from __future__ import annotations

import asyncio
import time

from sqlalchemy import BigInteger, Column, Integer, MetaData, Table, text
from sqlalchemy.ext.asyncio import create_async_engine

from dbkit import AsyncDatabase, Database, DatabaseTarget, Query, sql

from . import _common, _results, _stats

_metadata = MetaData()
CRUD_TABLE = Table(
    "dbkit_crud", _metadata, Column("id", BigInteger, primary_key=True), Column("v", Integer)
)

N = 3000
WARMUP = 100
BASE_ROWS = 5000  # pre-populated pool that SELECT/UPDATE/UPSERT operate against
RANGE_WIDTH = 100
TARGET = DatabaseTarget(database="app", role="write")

SELECT_POINT = Query(
    name="crud.select_point", statement=sql("SELECT v FROM dbkit_crud WHERE id = :id")
)
SELECT_RANGE = Query(
    name="crud.select_range",
    statement=sql("SELECT count(*), sum(v) FROM dbkit_crud WHERE id BETWEEN :lo AND :hi"),
)
UPDATE = Query(
    name="crud.update",
    statement=sql("UPDATE dbkit_crud SET v = :v WHERE id = :id"),
    operation="write",
)
UPSERT = Query(
    name="crud.upsert",
    statement=sql(
        "INSERT INTO dbkit_crud (id, v) VALUES (:id, :v) "
        "ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v"
    ),
    operation="write",
)
INSERT = Query(
    name="crud.insert",
    statement=sql("INSERT INTO dbkit_crud (id, v) VALUES (:id, :v)"),
    operation="write",
)
DELETE = Query(
    name="crud.delete", statement=sql("DELETE FROM dbkit_crud WHERE id = :id"), operation="write"
)

# id ranges that never collide with each other during a single run.
INSERT_START = 1_000_000
DELETE_START = 2_000_000


async def _setup(dsn: str) -> None:
    engine = create_async_engine(dsn)
    async with engine.begin() as conn:
        await conn.execute(
            text("CREATE TABLE IF NOT EXISTS dbkit_crud (id bigint PRIMARY KEY, v int)")
        )
        await conn.execute(text("TRUNCATE dbkit_crud"))
    await engine.dispose()


async def _measure(op, cfg: dict) -> tuple[float, dict[str, float]]:
    """Run ``op(i)`` WARMUP+N times; return (ops_per_second, latency_percentiles_ms)."""
    for i in range(WARMUP):
        await op(i)
    latencies: list[float] = []
    start = time.monotonic()
    for i in range(WARMUP, WARMUP + N):
        t0 = time.monotonic()
        await op(i)
        latencies.append((time.monotonic() - t0) * 1000)
    elapsed = time.monotonic() - start
    return N / elapsed, _stats.percentiles(latencies, points=(50, 95, 99))


async def _populate_base_rows(db: AsyncDatabase) -> None:
    await db.execute(sql("TRUNCATE dbkit_crud"), target=TARGET)
    records = ((i, i) for i in range(BASE_ROWS))
    await db.copy_records("dbkit_crud", ["id", "v"], records, target=TARGET)


async def run_async(dsn: str) -> dict[str, float]:
    await _setup(dsn)
    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": dsn}}},
            "defaults": {
                "pool": {"size": 10, "max_overflow": 0},
                "observability": {"metrics": False, "slow_query_ms": 1e9},
            },
        }
    )
    await db.start()
    metrics: dict[str, float] = {}
    try:
        await _populate_base_rows(db)

        async def select_point(i: int) -> None:
            await db.fetch_value(SELECT_POINT, {"id": i % BASE_ROWS}, target=TARGET)

        async def select_range(i: int) -> None:
            lo = (i * RANGE_WIDTH) % (BASE_ROWS - RANGE_WIDTH)
            await db.fetch_one(SELECT_RANGE, {"lo": lo, "hi": lo + RANGE_WIDTH}, target=TARGET)

        async def update(i: int) -> None:
            await db.execute(UPDATE, {"id": i % BASE_ROWS, "v": i}, target=TARGET)

        async def upsert(i: int) -> None:
            # cycles through BASE_ROWS (update branch) — the common dedup/idempotent-write case.
            await db.execute(UPSERT, {"id": i % BASE_ROWS, "v": i}, target=TARGET)

        async def insert(i: int) -> None:
            await db.execute(INSERT, {"id": INSERT_START + i, "v": i}, target=TARGET)

        for label, fn in (
            ("select", select_point),
            ("select_range", select_range),
            ("update", update),
            ("upsert", upsert),
            ("insert", insert),
        ):
            ops_s, pct = await _measure(fn, {})
            metrics[f"crud_{label}_ops_s"] = ops_s
            metrics[f"crud_{label}_p50_ms"] = pct.get("p50", 0.0)
            metrics[f"crud_{label}_p99_ms"] = pct.get("p99", 0.0)

        # delete: insert throwaway rows (untimed), then time deleting exactly those ids.
        await db.execute(sql("TRUNCATE dbkit_crud"), target=TARGET)
        await _populate_base_rows(db)
        delete_rows = [{"id": DELETE_START + i, "v": 0} for i in range(WARMUP + N)]
        await db.insert_many(CRUD_TABLE, delete_rows, target=TARGET, batch_size=2000)

        async def delete(i: int) -> None:
            await db.execute(DELETE, {"id": DELETE_START + i}, target=TARGET)

        ops_s, pct = await _measure(delete, {})
        metrics["crud_delete_ops_s"] = ops_s
        metrics["crud_delete_p50_ms"] = pct.get("p50", 0.0)
        metrics["crud_delete_p99_ms"] = pct.get("p99", 0.0)
    finally:
        await db.close()

    _common.rule("CRUD (async) — ops/s and latency")
    print(f"  {'operation':<14} {'ops/s':>12}   {'p50 ms':>8}   {'p99 ms':>8}")
    for label in ("select", "select_range", "update", "upsert", "insert", "delete"):
        print(
            f"  {label:<14} {metrics[f'crud_{label}_ops_s']:>12,.0f}   "
            f"{metrics[f'crud_{label}_p50_ms']:>8.3f}   {metrics[f'crud_{label}_p99_ms']:>8.3f}"
        )
    return metrics


def _measure_sync(op, cfg: dict) -> tuple[float, dict[str, float]]:
    for i in range(WARMUP):
        op(i)
    latencies: list[float] = []
    start = time.monotonic()
    for i in range(WARMUP, WARMUP + N):
        t0 = time.monotonic()
        op(i)
        latencies.append((time.monotonic() - t0) * 1000)
    elapsed = time.monotonic() - start
    return N / elapsed, _stats.percentiles(latencies, points=(50, 95, 99))


def run_sync(dsn: str) -> dict[str, float]:
    db = Database.from_config(
        {
            "databases": {"app": {"primary": {"url": dsn}}},
            "defaults": {
                "pool": {"size": 10, "max_overflow": 0},
                "observability": {"metrics": False, "slow_query_ms": 1e9},
            },
        }
    )
    db.start()
    metrics: dict[str, float] = {}
    try:
        db.execute(sql("TRUNCATE dbkit_crud"), target=TARGET)
        db.copy_records(
            "dbkit_crud", ["id", "v"], ((i, i) for i in range(BASE_ROWS)), target=TARGET
        )

        def select_point(i: int) -> None:
            db.fetch_value(SELECT_POINT, {"id": i % BASE_ROWS}, target=TARGET)

        def insert(i: int) -> None:
            db.execute(INSERT, {"id": INSERT_START + i, "v": i}, target=TARGET)

        for label, fn in (("select", select_point), ("insert", insert)):
            ops_s, pct = _measure_sync(fn, {})
            metrics[f"crud_sync_{label}_ops_s"] = ops_s
            metrics[f"crud_sync_{label}_p50_ms"] = pct.get("p50", 0.0)
            metrics[f"crud_sync_{label}_p99_ms"] = pct.get("p99", 0.0)
    finally:
        db.close()

    _common.rule("CRUD (sync) — ops/s and latency")
    print(f"  {'operation':<14} {'ops/s':>12}   {'p50 ms':>8}   {'p99 ms':>8}")
    for label in ("select", "insert"):
        print(
            f"  {label:<14} {metrics[f'crud_sync_{label}_ops_s']:>12,.0f}   "
            f"{metrics[f'crud_sync_{label}_p50_ms']:>8.3f}   "
            f"{metrics[f'crud_sync_{label}_p99_ms']:>8.3f}"
        )
    return metrics


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
