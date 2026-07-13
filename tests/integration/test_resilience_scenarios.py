"""Resilience / chaos suite (§12, §15, §32.3). Marked ``integration``.

Failures are induced the way rabbitkit's SRE suite does — real infrastructure faults, not
mocks: killing PostgreSQL backends (``pg_terminate_backend``), restarting the container
(``docker restart``), racing a kill against an in-flight commit, and driving many concurrent
operations. Assertions center on: recovery, correct error classification, no connection leaks,
and bounded resources.

Scenarios that only need SQL (backend termination, pool bounds, cancellation, shutdown) run
against ``DBKIT_TEST_DSN`` too. Scenarios that must restart the server require Docker and use
a dedicated testcontainer; they self-skip when Docker is unavailable.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import AsyncIterator

import pytest

from dbkit import AsyncDatabase, DatabaseTarget, Query, sql
from dbkit.errors import DatabaseError

pytestmark = pytest.mark.integration

TARGET = DatabaseTarget(database="app", role="write")

INSERT = Query(
    name="chaos.insert",
    statement=sql("INSERT INTO dbkit_chaos (id, v) VALUES (:id, :v) ON CONFLICT (id) DO NOTHING"),
    operation="write",
    idempotent=True,
)


def _skip_no_docker() -> None:
    try:
        import docker
    except ImportError:
        pytest.skip("docker SDK not installed")
    try:
        docker.from_env().ping()
    except Exception:
        pytest.skip("Docker daemon not reachable")


async def _make_db(dsn: str, **pool: object) -> AsyncDatabase:
    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": dsn}}},
            "defaults": {
                "pool": {"pre_ping": True, "timeout_seconds": 2.0, **pool},
                "observability": {"metrics": False, "slow_query_ms": 1e9},
            },
        }
    )
    await db.start()
    return db


@pytest.fixture
async def chaos_db(base_config: dict) -> AsyncIterator[AsyncDatabase]:
    db = AsyncDatabase.from_config(base_config)
    await db.start()
    await db.execute(
        sql("CREATE TABLE IF NOT EXISTS dbkit_chaos (id bigint PRIMARY KEY, v int)"), target=TARGET
    )
    await db.execute(sql("TRUNCATE dbkit_chaos"), target=TARGET)
    try:
        yield db
    finally:
        await db.close()


# --- SQL-fault scenarios (run against DBKIT_TEST_DSN) ------------------------------- #


async def test_backend_termination_mid_transaction_is_classified(chaos_db: AsyncDatabase) -> None:
    """Killing the backend during a transaction surfaces a classified error, not a raw driver
    exception, and the connection is invalidated (§13, §12.3)."""
    admin = chaos_db  # a second logical connection from the same pool works fine for killing

    with pytest.raises(DatabaseError):
        async with chaos_db.transaction(target=TARGET) as tx:
            await tx.execute(INSERT, {"id": 1, "v": 1})
            # figure out this transaction's backend pid and kill it from another connection
            pid = await tx.fetch_value(sql("SELECT pg_backend_pid()"))
            await admin.execute(
                sql("SELECT pg_terminate_backend(:pid)"), {"pid": pid}, target=TARGET
            )
            # next statement on the dead connection must raise a classified DatabaseError
            await tx.execute(INSERT, {"id": 2, "v": 2})

    # the pool recovers: a fresh operation succeeds
    assert await chaos_db.fetch_value(sql("SELECT 1"), target=TARGET) == 1


async def test_connection_count_stays_bounded_under_concurrency(base_config: dict) -> None:
    """Many concurrent operations through a small pool must not open unbounded connections
    (leak guard, mirrors rabbitkit's bounded-channel test)."""
    db = await _make_db(base_config["databases"]["app"]["primary"]["url"], size=3, max_overflow=2)
    try:
        await db.execute(sql("SELECT 1"), target=TARGET)  # warm

        async def op(i: int) -> int:
            return await db.fetch_value(sql("SELECT :n"), {"n": i}, target=TARGET)

        results = await asyncio.gather(*[op(i) for i in range(200)])
        assert sorted(results) == list(range(200))

        snap = db.pool_status()[0]
        # created connections may never exceed pool capacity (size + overflow)
        assert snap.created <= snap.total_capacity
        assert snap.checked_out == 0  # everything returned
    finally:
        await db.close()


async def test_cancellation_storm_leaves_no_checked_out_connections(
    chaos_db: AsyncDatabase,
) -> None:
    """Cancelling many in-flight ops must return every connection to the pool (§12.3)."""
    tasks = [
        asyncio.create_task(
            chaos_db.fetch_value(sql("SELECT pg_sleep(5)"), target=TARGET, timeout=10)
        )
        for _ in range(10)
    ]
    await asyncio.sleep(0.3)
    for t in tasks:
        t.cancel()
    for t in tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await t
    await asyncio.sleep(0.3)  # allow the pool to reclaim
    assert chaos_db.pool_status()[0].checked_out == 0
    # pool is still usable afterwards
    assert await chaos_db.fetch_value(sql("SELECT 1"), target=TARGET) == 1


async def test_graceful_shutdown_while_operations_in_flight(base_config: dict) -> None:
    """close() during active load completes within its grace period and disposes engines."""
    db = await _make_db(base_config["databases"]["app"]["primary"]["url"], size=5, max_overflow=0)
    await db.execute(sql("SELECT 1"), target=TARGET)

    async def worker() -> None:
        with contextlib.suppress(DatabaseError, asyncio.CancelledError):
            for _ in range(50):
                await db.fetch_value(sql("SELECT 1"), target=TARGET)

    workers = [asyncio.create_task(worker()) for _ in range(10)]
    await asyncio.sleep(0.1)
    start = time.monotonic()
    await db.close(grace_period=5.0)
    assert time.monotonic() - start < 5.0
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)


async def test_commit_unknown_when_backend_dies_during_commit(base_config: dict) -> None:
    """If the connection dies at COMMIT, dbkit reports a distinct commit-unknown outcome and
    never silently reports success (§15). Best-effort: skipped if the kill misses the window."""
    dsn = base_config["databases"]["app"]["primary"]["url"]
    db = await _make_db(dsn, size=2, max_overflow=2)
    admin = await _make_db(dsn, size=1, max_overflow=0)
    from dbkit.errors import DatabaseCommitUnknownError

    try:
        await db.execute(
            sql("CREATE TABLE IF NOT EXISTS dbkit_commit (id int PRIMARY KEY)"), target=TARGET
        )
        outcome = None
        kill_tasks: list[asyncio.Task[None]] = []  # keep strong refs so tasks aren't GC'd
        for attempt in range(20):
            base_id = 1000 + attempt * 10
            try:
                async with db.transaction(target=TARGET) as tx:
                    pid = await tx.fetch_value(sql("SELECT pg_backend_pid()"))
                    await tx.execute(
                        sql("INSERT INTO dbkit_commit (id) VALUES (:id) ON CONFLICT DO NOTHING"),
                        {"id": base_id},
                    )

                    async def _kill(pid: int = pid) -> None:
                        await admin.execute(
                            sql("SELECT pg_terminate_backend(:pid)"),
                            {"pid": pid},
                            target=TARGET,
                        )

                    # race a kill against the imminent commit
                    kill_tasks.append(asyncio.create_task(_kill()))
            except DatabaseCommitUnknownError as exc:
                assert exc.transaction_state_unknown is True
                outcome = "commit_unknown"
                break
            except DatabaseError:
                # kill landed before commit -> normal rollback/connection error; retry
                continue
        if outcome != "commit_unknown":
            pytest.skip("kill never landed inside the commit window in this environment")
    finally:
        for t in kill_tasks:
            t.cancel()
        await asyncio.gather(*kill_tasks, return_exceptions=True)
        await db.close()
        await admin.close()


# --- container-restart scenario (requires Docker) ----------------------------------- #


async def test_recovers_after_full_database_restart() -> None:
    """A full server restart mid-life is recovered transparently on the next operation
    (§10.6). Uses a dedicated container so the restart is isolated."""
    _skip_no_docker()
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16", driver="psycopg") as pg:
        dsn = pg.get_connection_url()
        db = await _make_db(dsn, size=2, max_overflow=2, recycle_seconds=1)
        try:
            assert await db.fetch_value(sql("SELECT 1"), target=TARGET) == 1

            # restart the server (atomic stop+start), then poll for readiness via dbkit
            pg.get_wrapped_container().restart()

            deadline = time.monotonic() + 60
            recovered = False
            last_err: Exception | None = None
            while time.monotonic() < deadline:
                try:
                    if await db.fetch_value(sql("SELECT 1"), target=TARGET) == 1:
                        recovered = True
                        break
                except DatabaseError as exc:  # pre-ping/connect errors during boot are expected
                    last_err = exc
                    await asyncio.sleep(1.0)
            assert recovered, f"did not recover after restart; last error: {last_err}"
        finally:
            await db.close()
