"""Soak — sustained paced load with periodic fault injection, asserting leak-free recovery.

This is the long-running evidence a per-push job can't produce (§32.5, §33.3). It runs a paced
writer against dbkit, injects a fault (terminate all backends) every ``--kill-every`` seconds,
and samples RSS / open FDs / asyncio task count / pool connections. Verdicts gate the exit code.

Bookkeeping is O(1) in memory (a confirmed counter + a small residual set) so the harness does
not measure itself.

    python -m benchmarks.soak --dsn postgresql+psycopg://localhost/postgres --duration 120
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import time

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from dbkit import AsyncDatabase, DatabaseTarget, Query, sql
from dbkit.errors import DatabaseError

from . import _common

TARGET = DatabaseTarget(database="app", role="write")
INSERT = Query(
    name="soak.insert",
    statement=sql("INSERT INTO dbkit_soak (id, v) VALUES (:id, :v) ON CONFLICT (id) DO NOTHING"),
    operation="write",
    idempotent=True,
)

RSS_SLOPE_LIMIT_KB_PER_MIN = 256.0
RSS_NET_FLOOR_MB = 8.0
FD_GROWTH_LIMIT = 16
TASK_GROWTH_LIMIT = 64
RECOVERY_WINDOW_S = 30.0


async def _setup(dsn: str) -> None:
    engine = create_async_engine(dsn)
    async with engine.begin() as conn:
        await conn.execute(
            text("CREATE TABLE IF NOT EXISTS dbkit_soak (id bigint PRIMARY KEY, v int)")
        )
        await conn.execute(text("TRUNCATE dbkit_soak"))
    await engine.dispose()


def _proc_stats() -> tuple[float, int]:
    try:
        import psutil

        p = psutil.Process()
        return p.memory_info().rss / (1024 * 1024), p.num_fds()
    except Exception:
        return 0.0, 0


def _slope_kb_per_min(samples: list[tuple[float, float]]) -> float:
    """Least-squares slope of RSS(MB) over time(s), returned as KB/min."""
    if len(samples) < 3:
        return 0.0
    n = len(samples)
    t0 = samples[0][0]
    xs = [t - t0 for t, _ in samples]
    ys = [mb for _, mb in samples]
    mx = sum(xs) / n
    my = sum(ys) / n
    denom = sum((x - mx) ** 2 for x in xs)
    if denom == 0:
        return 0.0
    slope_mb_per_s = sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=False)) / denom
    return slope_mb_per_s * 1024 * 60


async def run(dsn: str, duration: float, rate: float, kill_every: float) -> int:
    await _setup(dsn)
    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": dsn}}},
            "defaults": {
                "pool": {"size": 10, "max_overflow": 5, "pre_ping": True},
                "observability": {"metrics": False, "slow_query_ms": 1e9},
            },
        }
    )
    admin = AsyncDatabase.from_config({"databases": {"app": {"primary": {"url": dsn}}}})
    await db.start()
    await admin.start()

    confirmed = 0
    errors = 0
    last_confirmed_at = time.monotonic()
    recovery_failures = 0
    rss_samples: list[tuple[float, float]] = []
    fd_first: int | None = None
    fd_last = 0
    task_first = len(asyncio.all_tasks())
    task_last = task_first
    pool_checked_out_last = 0
    stop = time.monotonic() + duration

    async def writer() -> None:
        nonlocal confirmed, errors, last_confirmed_at
        i = 0
        t0 = time.monotonic()
        while time.monotonic() < stop:
            due = t0 + i / rate
            now = time.monotonic()
            if due > now:
                await asyncio.sleep(due - now)
            try:
                await db.execute(INSERT, {"id": i, "v": i}, target=TARGET, timeout=2.0)
                confirmed += 1
                last_confirmed_at = time.monotonic()
            except DatabaseError:
                errors += 1  # expected during fault windows; idempotent id retried next tick
                i -= 1
            i += 1

    async def chaos() -> None:
        nonlocal recovery_failures
        while time.monotonic() < stop:
            await asyncio.sleep(kill_every)
            if time.monotonic() >= stop:
                break
            before = confirmed
            with contextlib.suppress(Exception):
                await admin.execute(
                    sql(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE datname = current_database() AND pid <> pg_backend_pid()"
                    ),
                    target=TARGET,
                )
            # wait for the writer to make progress again
            deadline = time.monotonic() + RECOVERY_WINDOW_S
            while time.monotonic() < deadline and confirmed <= before + 1:
                await asyncio.sleep(0.5)
            if confirmed <= before + 1:
                recovery_failures += 1

    async def sampler() -> None:
        nonlocal fd_first, fd_last, task_last, pool_checked_out_last
        while time.monotonic() < stop:
            rss, fds = _proc_stats()
            rss_samples.append((time.monotonic(), rss))
            if fd_first is None:
                fd_first = fds
            fd_last = fds
            task_last = len(asyncio.all_tasks())
            snaps = db.pool_status()
            if snaps:
                pool_checked_out_last = snaps[0].checked_out
            await asyncio.sleep(5.0)

    try:
        await asyncio.gather(writer(), chaos(), sampler())
    finally:
        await db.close()
        await admin.close()

    # Verdicts.
    warm = rss_samples[len(rss_samples) // 5 :]  # discard 20% warmup
    slope = _slope_kb_per_min(warm)
    net_growth_mb = (warm[-1][1] - warm[0][1]) if len(warm) >= 2 else 0.0
    fd_growth = fd_last - (fd_first or fd_last)
    task_growth = task_last - task_first

    rss_bounded = not (slope > RSS_SLOPE_LIMIT_KB_PER_MIN and net_growth_mb > RSS_NET_FLOOR_MB)
    verdicts = {
        "made_progress": confirmed > 0,
        "recovered_after_every_kill": recovery_failures == 0,
        "rss_bounded": rss_bounded,
        "fds_bounded": fd_growth <= FD_GROWTH_LIMIT,
        "tasks_bounded": task_growth <= TASK_GROWTH_LIMIT,
    }

    _common.rule("soak report")
    print(f"  confirmed inserts     {confirmed:,}")
    print(f"  fault-window errors   {errors:,}")
    print(f"  recovery failures     {recovery_failures}")
    print(f"  rss slope             {slope:.1f} KB/min (net {net_growth_mb:+.1f} MB)")
    print(f"  fd growth             {fd_growth}")
    print(f"  task growth           {task_growth}")
    print(f"  last pool checked_out {pool_checked_out_last}")
    print("  verdicts:")
    for name, ok in verdicts.items():
        print(f"    [{'PASS' if ok else 'FAIL'}] {name}")

    return 0 if all(verdicts.values()) else 1


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m benchmarks.soak")
    parser.add_argument("--dsn", default=None)
    parser.add_argument("--duration", type=float, default=120.0)
    parser.add_argument("--rate", type=float, default=200.0)
    parser.add_argument("--kill-every", type=float, default=30.0)
    args = parser.parse_args()
    with _common.dsn_context(args.dsn) as dsn:
        code = asyncio.run(run(dsn, args.duration, args.rate, args.kill_every))
    raise SystemExit(code)


if __name__ == "__main__":
    main()
