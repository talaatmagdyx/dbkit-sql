"""High shard-cardinality load test — real concurrent traffic, not just a synthetic unit test
(performance review §10, §15 test #15/#16). A single PostgreSQL instance stands in for physical
shard topology here (the same pattern ``tests/integration/test_sharding_and_replicas.py`` uses)
— the point is to prove dbkit's engine-registry/LRU-eviction behavior at high shard cardinality
under real concurrent load, not to benchmark physical multi-node sharding infrastructure this
session doesn't have.

Two scenarios, discovered by running this benchmark and investigating an initial failure:

- ``supported``: ``concurrency <= max_engines``. Even at 10x total shard-count oversubscription
  (200 distinct shard keys through a 20-engine cache), this is clean: zero errors. This is the
  documented, validated operating point.
- ``beyond_boundary``: ``concurrency > max_engines`` (deliberately). Root-caused via a dedicated
  debug reproduction (not assumed): every error is a genuine ``psycopg.OperationalError: ...
  FATAL: sorry, too many clients already`` — real PostgreSQL ``max_connections`` exhaustion, not
  a dbkit hang, corruption, or silent failure. Mechanism: the LRU cache bounds *steady-state*
  engine count, but not the number of *concurrent engine-creation-in-flight* events. When more
  concurrent requests are in flight than ``max_engines`` slots, and those requests fan out across
  more distinct shards than the cache holds, the cache thrashes continuously — every lookup can
  miss, evict, and create — and overlapping create/dispose churn transiently opens more physical
  connections than ``max_engines * pool_capacity`` accounts for. This scenario does NOT assert
  zero errors (that would be the wrong contract); it asserts the failure is always the classified
  ``DatabaseConnectionError``, never a hang or an unclassified exception, and that the registry's
  own steady-state bound (``len(engines) <= max_engines``) still holds even while this happens.

Operational takeaway (documented in PERFORMANCE_REVIEW.md): size ``max_engines`` to comfortably
exceed expected *concurrent* distinct-shard fan-out, not just total shard cardinality — the two
are independent axes and only the former determines connection-churn safety.

    DBKIT_TEST_DSN=postgresql+psycopg://dbkit:dbkit@localhost:55432/dbkit \\
        uv run python -m benchmarks.bench_shard_cardinality
"""

from __future__ import annotations

import asyncio
import os
import random
import time

from dbkit import AsyncDatabase, DatabaseTarget, HashShardResolver, sql
from dbkit.errors import DatabaseConnectionError

from . import _common, _stats

DSN = os.environ.get("DBKIT_TEST_DSN", "postgresql+psycopg://dbkit:dbkit@localhost:55432/dbkit")
WINDOW_SECONDS = 3.0


async def _worker(
    db: AsyncDatabase, num_shards: int, stop_at: float, latencies: list[float], errors: list[str]
) -> int:
    completed = 0
    while time.monotonic() < stop_at:
        shard_key = f"tenant-{random.randrange(num_shards)}"
        target = DatabaseTarget(database="app", role="write", shard_key=shard_key)
        start = time.monotonic()
        try:
            await db.fetch_value(sql("SELECT 1"), target=target, timeout=10.0)
            latencies.append((time.monotonic() - start) * 1000)
            completed += 1
        except Exception as exc:
            errors.append(type(exc).__name__)
            if not isinstance(exc, DatabaseConnectionError):
                raise AssertionError(
                    f"expected only classified DatabaseConnectionError under connection "
                    f"exhaustion, got unclassified {type(exc).__name__}: {exc}"
                ) from exc
    return completed


async def _run_scenario(
    dsn: str, *, label: str, num_shards: int, max_engines: int, concurrency: int
) -> dict[str, float]:
    resolver = HashShardResolver(num_shards)
    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": dsn}}},
            "max_engines": max_engines,
            "evict_lru_engines": True,
            "defaults": {
                "pool": {"size": 2, "max_overflow": 1},
                "observability": {"metrics": False, "slow_query_ms": 1e9},
            },
        },
        shard_resolver=resolver,
    )
    await db.start()
    oversubscription = num_shards / max_engines
    _common.rule(
        f"shard-cardinality load [{label}] — {num_shards} shards / {max_engines}-engine cache "
        f"({oversubscription:.0f}x), concurrency={concurrency}"
    )
    latencies: list[float] = []
    errors: list[str] = []
    try:
        stop_at = time.monotonic() + WINDOW_SECONDS
        start = time.monotonic()
        results = await asyncio.gather(
            *(_worker(db, num_shards, stop_at, latencies, errors) for _ in range(concurrency))
        )
        elapsed = time.monotonic() - start
        engine_count_at_end = db.pool_status()
    finally:
        await db.close()

    total = sum(results)
    pct = _stats.percentiles(latencies, points=(50, 95, 99, 99.9)) if latencies else {}
    print(f"  total ops: {total:,}  ({total / elapsed:,.0f} ops/s)")
    print(f"  errors: {len(errors)} (all classified DatabaseConnectionError: {bool(errors)})")
    print(f"  live engines at end: {len(engine_count_at_end)} (bound: {max_engines})")
    for k, v in pct.items():
        print(f"  {k:>5}: {v:>8.2f} ms")

    assert len(engine_count_at_end) <= max_engines, "engine registry exceeded its configured bound"

    return {
        f"shard_cardinality_{label}_ops_s": total / elapsed,
        f"shard_cardinality_{label}_p50_ms": pct.get("p50", 0.0),
        f"shard_cardinality_{label}_errors": float(len(errors)),
    }


async def _run_async(dsn: str) -> dict[str, float]:
    metrics: dict[str, float] = {}
    metrics.update(
        await _run_scenario(dsn, label="supported", num_shards=200, max_engines=20, concurrency=20)
    )
    assert metrics["shard_cardinality_supported_errors"] == 0.0, (
        "the supported operating point (concurrency <= max_engines) must be error-free"
    )
    metrics.update(
        await _run_scenario(
            dsn, label="beyond_boundary", num_shards=500, max_engines=20, concurrency=50
        )
    )
    print(
        "\n  finding: concurrency > max_engines causes real PostgreSQL max_connections "
        "exhaustion via engine-cache thrash (errors above are expected, and correctly "
        "classified — see module docstring). Mitigation: size max_engines to comfortably "
        "exceed expected concurrent distinct-shard fan-out."
    )
    return metrics


def run_all(dsn: str) -> dict[str, float]:
    return asyncio.run(_run_async(dsn))


def main(dsn: str | None = None) -> dict[str, float]:
    with _common.dsn_context(dsn) as resolved:
        return run_all(resolved)


if __name__ == "__main__":
    import sys

    main(sys.argv[1] if len(sys.argv) > 1 else None)
