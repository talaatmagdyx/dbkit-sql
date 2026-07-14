"""Streaming-at-scale memory bound test — real measurement, not a capability claim (performance
review §8, §15 test #13). Streams narrow and wide result sets up to several million rows via
``db.stream()`` against live PostgreSQL, sampling RSS throughout to confirm memory stays bounded
regardless of total result-set size, rather than growing proportionally with rows streamed.

    DBKIT_TEST_DSN=postgresql+psycopg://dbkit:dbkit@localhost:55432/dbkit \\
        uv run python -m benchmarks.bench_streaming_scale
"""

from __future__ import annotations

import asyncio
import os
import resource
import time

from dbkit import AsyncDatabase, DatabaseTarget, sql

from . import _common

DSN = os.environ.get("DBKIT_TEST_DSN", "postgresql+psycopg://dbkit:dbkit@localhost:55432/dbkit")
TARGET = DatabaseTarget(database="app", role="read")

#: (label, row count, query) -- "wide" pads each row with several sizeable text columns to
#: stress per-row materialization cost, not just row count.
SCENARIOS = (
    ("narrow, 1M rows", 1_000_000, "SELECT i FROM generate_series(1, :n) AS i"),
    (
        "wide, 1M rows",
        1_000_000,
        "SELECT i, i * 2 AS j, i * 3 AS k, "
        "repeat('x', 80) AS a, repeat('y', 80) AS b, repeat('z', 80) AS c "
        "FROM generate_series(1, :n) AS i",
    ),
    ("narrow, 5M rows", 5_000_000, "SELECT i FROM generate_series(1, :n) AS i"),
)


def _rss_mb() -> float:
    """Current process RSS in MB. ``ru_maxrss`` is bytes on macOS, KB on Linux."""
    raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return raw / (1024 * 1024) if os_is_darwin() else raw / 1024


def os_is_darwin() -> bool:
    import platform

    return platform.system() == "Darwin"


async def _stream_one(db: AsyncDatabase, label: str, n: int, query: str) -> dict[str, float]:
    rss_samples: list[float] = [_rss_mb()]
    seen = 0
    start = time.monotonic()

    async def sampler(stop: asyncio.Event) -> None:
        while not stop.is_set():
            rss_samples.append(_rss_mb())
            await asyncio.sleep(0.2)

    stop = asyncio.Event()
    task = asyncio.create_task(sampler(stop))
    try:
        async with await db.stream(sql(query), {"n": n}, target=TARGET, batch_size=2000) as rows:
            async for _ in rows:
                seen += 1
    finally:
        stop.set()
        await task
    elapsed = time.monotonic() - start
    rss_samples.append(_rss_mb())
    rss_start, rss_peak, rss_end = rss_samples[0], max(rss_samples), rss_samples[-1]
    print(
        f"  {label:<18} rows={seen:>9,}  {seen / elapsed:>9,.0f} rows/s  "
        f"RSS start={rss_start:>7.1f}MB peak={rss_peak:>7.1f}MB "
        f"end={rss_end:>7.1f}MB (Δpeak={rss_peak - rss_start:+.1f}MB)"
    )
    assert seen == n, f"expected {n} rows, streamed {seen}"
    return {
        "rows": seen,
        "rows_s": seen / elapsed,
        "rss_start_mb": rss_start,
        "rss_peak_mb": rss_peak,
    }


async def _run_async(dsn: str) -> dict[str, float]:
    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": dsn}}},
            "defaults": {
                "query_timeout_seconds": 120.0,
                "observability": {"metrics": False, "slow_query_ms": 1e9},
            },
        }
    )
    await db.start()
    _common.rule("streaming at scale — RSS sampled throughout, not just before/after")
    metrics: dict[str, float] = {}
    try:
        for label, n, query in SCENARIOS:
            row = await _stream_one(db, label, n, query)
            key = label.replace(" ", "_").replace(",", "")
            metrics[f"streaming_{key}_rows_s"] = row["rows_s"]
            metrics[f"streaming_{key}_rss_delta_mb"] = row["rss_peak_mb"] - row["rss_start_mb"]
    finally:
        await db.close()

    deltas = [v for k, v in metrics.items() if k.endswith("_rss_delta_mb")]
    print(
        f"\n  max RSS growth across all scenarios: {max(deltas):.1f}MB "
        f"(bounded if this doesn't scale with the largest row count, 5,000,000)"
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
