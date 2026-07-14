"""Multi-process connection-budget validation (task #92, performance review §10.3). The
connection-budget formula (``DbkitConfig.connection_budget_report`` /
``max_connections_per_process()``) claims ``cluster_total == per_process * app_replicas``. That
formula has only ever been exercised as pure arithmetic in unit tests -- this test validates it
against real OS processes: several genuinely separate ``python`` subprocesses, each running its
own ``AsyncDatabase`` against the same live PostgreSQL instance, each driven to full pool
capacity concurrently. The real connection count observed via ``pg_stat_activity`` must match
what the formula predicts, and must drop back down once the processes close cleanly.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

from dbkit import AsyncDatabase, DatabaseTarget, sql

pytestmark = pytest.mark.integration

TARGET = DatabaseTarget(database="app", role="write")
WORKER = Path(__file__).parent / "_mp_budget_worker.py"

NUM_PROCESSES = 4
POOL_SIZE = 3
POOL_OVERFLOW = 2
POOL_CAPACITY = POOL_SIZE + POOL_OVERFLOW
HOLD_SECONDS = 8.0


async def _admin_connection_count(admin: AsyncDatabase) -> int:
    return await admin.fetch_value(
        sql(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE datname = current_database() AND pid != pg_backend_pid()"
        ),
        target=TARGET,
    )


async def _wait_for_ready(proc: asyncio.subprocess.Process) -> None:
    assert proc.stdout is not None
    line = await proc.stdout.readline()
    assert line.strip() == b"READY", f"worker did not signal readiness, got {line!r}"


async def test_multiprocess_connection_count_matches_the_documented_budget_formula(
    pg_dsn: str,
) -> None:
    admin = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": pg_dsn}}},
            "defaults": {"observability": {"metrics": False, "slow_query_ms": 1e9}},
        }
    )
    await admin.start()
    try:
        baseline = await _admin_connection_count(admin)

        async def _spawn() -> asyncio.subprocess.Process:
            return await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "tests.integration._mp_budget_worker",
                pg_dsn,
                "--size",
                str(POOL_SIZE),
                "--overflow",
                str(POOL_OVERFLOW),
                "--hold",
                str(HOLD_SECONDS),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(Path(__file__).parents[2]),
            )

        # Spawned concurrently (not one `await` at a time) -- per-process Python/import startup
        # latency is significant enough that sequential creation could let an early process
        # finish its whole hold-and-close cycle before a later one even reports ready.
        procs = await asyncio.gather(*(_spawn() for _ in range(NUM_PROCESSES)))
        try:
            await asyncio.wait_for(
                asyncio.gather(*(_wait_for_ready(p) for p in procs)), timeout=15.0
            )
            await asyncio.sleep(1.0)  # let every process's pool settle at full checkout

            at_peak = await _admin_connection_count(admin)
            observed_from_workers = at_peak - baseline
            documented_ceiling = NUM_PROCESSES * POOL_CAPACITY

            assert observed_from_workers <= documented_ceiling, (
                f"real connections ({observed_from_workers}) exceeded the documented "
                f"per_process*replicas ceiling ({documented_ceiling}) -- the budget formula "
                f"would under-predict real usage"
            )
            # Every process was driven to full capacity concurrently, so usage should land
            # close to the ceiling, not far under it (otherwise the formula would be a poor
            # predictor of realistic multi-replica connection counts, not just a safe bound).
            assert observed_from_workers >= documented_ceiling * 0.8, (
                f"real connections ({observed_from_workers}) were far below the documented "
                f"ceiling ({documented_ceiling}) even at full pool utilization"
            )
        finally:
            for p in procs:
                stdout, stderr = await p.communicate()
                assert p.returncode == 0, (
                    f"worker exited non-zero: stdout={stdout!r} stderr={stderr!r}"
                )

        await asyncio.sleep(0.3)  # let PostgreSQL notice the closed backends
        after_close = await _admin_connection_count(admin)
        assert after_close <= baseline + 1, (
            f"connections did not drop back down after all worker processes closed "
            f"(baseline={baseline}, after_close={after_close})"
        )
    finally:
        await admin.close()
