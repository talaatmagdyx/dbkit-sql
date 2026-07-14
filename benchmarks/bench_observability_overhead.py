"""OTel tracing vs Prometheus metrics vs OTel metrics vs no observability — real load test
(performance review §11/§15 test #18/#19). Measures throughput/latency at a fixed concurrency
chosen to stay within the default pool's capacity (so pool contention doesn't confound the
comparison — see bench_concurrency_scaling.py, which found the pool ~67% utilized at
concurrency=10 with the default pool).

    DBKIT_TEST_DSN=postgresql+psycopg://dbkit:dbkit@localhost:55432/dbkit \\
        uv run python -m benchmarks.bench_observability_overhead
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
GET = Query(name="bench.observability.get", statement=sql("SELECT v FROM dbkit_bench WHERE id = 1"))
CONCURRENCY = 10
WINDOW_SECONDS = 3.0


async def _setup(dsn: str) -> None:
    engine = create_async_engine(dsn)
    async with engine.begin() as conn:
        await conn.execute(
            text("CREATE TABLE IF NOT EXISTS dbkit_bench (id int PRIMARY KEY, v int)")
        )
        await conn.execute(text("INSERT INTO dbkit_bench VALUES (1, 42) ON CONFLICT DO NOTHING"))
    await engine.dispose()


async def _worker(db: AsyncDatabase, stop_at: float, latencies: list[float]) -> int:
    completed = 0
    while time.monotonic() < stop_at:
        start = time.monotonic()
        await db.fetch_value(GET, target=TARGET)
        latencies.append((time.monotonic() - start) * 1000)
        completed += 1
    return completed


async def _run_lane(dsn: str, *, label: str, config_extra: dict, metrics=None) -> dict[str, float]:
    cfg = {
        "databases": {"app": {"primary": {"url": dsn}}},
        "defaults": {
            "pool": {"size": 20, "max_overflow": 10},
            "observability": {"slow_query_ms": 1e9, **config_extra},
        },
    }
    db = (
        AsyncDatabase.from_config(cfg, metrics=metrics)
        if metrics
        else AsyncDatabase.from_config(cfg)
    )
    await db.start()
    latencies: list[float] = []
    try:
        await db.fetch_value(GET, target=TARGET)  # warmup
        stop_at = time.monotonic() + WINDOW_SECONDS
        start = time.monotonic()
        results = await asyncio.gather(
            *(_worker(db, stop_at, latencies) for _ in range(CONCURRENCY))
        )
        elapsed = time.monotonic() - start
    finally:
        await db.close()
    total = sum(results)
    pct = _stats.percentiles(latencies, points=(50, 95, 99))
    print(
        f"  {label:<22} throughput={total / elapsed:>9,.0f} ops/s  "
        f"p50={pct.get('p50', 0.0):>6.3f}ms  p99={pct.get('p99', 0.0):>7.3f}ms"
    )
    return {
        "throughput_ops_s": total / elapsed,
        "p50_ms": pct.get("p50", 0.0),
        "p99_ms": pct.get("p99", 0.0),
    }


async def _run_async(dsn: str) -> dict[str, float]:
    await _setup(dsn)
    _common.rule(f"observability overhead — concurrency={CONCURRENCY}, {WINDOW_SECONDS}s window")

    baseline = await _run_lane(
        dsn, label="baseline (none)", config_extra={"metrics": False, "tracing": False}
    )

    tracing = await _run_lane(
        dsn, label="OTel tracing only", config_extra={"metrics": False, "tracing": True}
    )

    try:
        from prometheus_client import CollectorRegistry

        from dbkit.observability.prometheus import PrometheusMetrics

        prom_sink = PrometheusMetrics(namespace="bench_observability", registry=CollectorRegistry())
        prometheus = await _run_lane(
            dsn,
            label="Prometheus metrics",
            config_extra={"metrics": True, "tracing": False},
            metrics=prom_sink,
        )
    except ImportError:
        print("  Prometheus metrics    (skipped — prometheus_client not installed)")
        prometheus = {"throughput_ops_s": 0.0, "p50_ms": 0.0, "p99_ms": 0.0}

    try:
        from dbkit.observability.otel_metrics import OTelMetrics

        otel_sink = OTelMetrics(meter_name="bench_observability")
        otel_metrics = await _run_lane(
            dsn,
            label="OTel metrics",
            config_extra={"metrics": True, "tracing": False},
            metrics=otel_sink,
        )
    except (ImportError, RuntimeError):
        print("  OTel metrics          (skipped — opentelemetry not installed)")
        otel_metrics = {"throughput_ops_s": 0.0, "p50_ms": 0.0, "p99_ms": 0.0}

    print(f"\n  baseline p50: {baseline['p50_ms']:.3f}ms")
    for label, row in (
        ("OTel tracing", tracing),
        ("Prometheus metrics", prometheus),
        ("OTel metrics", otel_metrics),
    ):
        if row["p50_ms"]:
            delta_pct = (row["p50_ms"] - baseline["p50_ms"]) / baseline["p50_ms"] * 100
            print(f"  {label} p50 delta vs baseline: {delta_pct:+.1f}%")

    return {
        "observability_baseline_p50_ms": baseline["p50_ms"],
        "observability_tracing_p50_ms": tracing["p50_ms"],
        "observability_prometheus_p50_ms": prometheus["p50_ms"],
        "observability_otel_metrics_p50_ms": otel_metrics["p50_ms"],
    }


def run_all(dsn: str) -> dict[str, float]:
    return asyncio.run(_run_async(dsn))


def main(dsn: str | None = None) -> dict[str, float]:
    with _common.dsn_context(dsn) as resolved:
        return run_all(resolved)


if __name__ == "__main__":
    import sys

    main(sys.argv[1] if len(sys.argv) > 1 else None)
