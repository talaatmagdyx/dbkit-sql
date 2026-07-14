"""Async integration tests against a real PostgreSQL (§32.2). Marked ``integration``."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest

from dbkit import AsyncDatabase, DatabaseTarget, Query, sql
from dbkit.errors import (
    DatabasePoolTimeoutError,
    DatabaseQueryTimeoutError,
    DatabaseResultError,
    DatabaseUniqueViolationError,
)

from ..conftest import RecordingMetrics

pytestmark = pytest.mark.integration

TARGET = DatabaseTarget(database="app", role="write")

CREATE = sql(
    """
    CREATE TABLE IF NOT EXISTS dbkit_users (
        id    integer PRIMARY KEY,
        email text NOT NULL UNIQUE
    )
    """
)
TRUNCATE = sql("TRUNCATE dbkit_users")
INSERT = Query(
    name="users.insert",
    statement=sql("INSERT INTO dbkit_users (id, email) VALUES (:id, :email)"),
    operation="write",
)
GET = Query(
    name="users.get",
    statement=sql("SELECT id, email FROM dbkit_users WHERE id = :id"),
    operation="read",
    idempotent=True,
)


@dataclass
class User:
    id: int
    email: str


@pytest.fixture
async def adb(base_config: dict) -> AsyncIterator[AsyncDatabase]:
    db = AsyncDatabase.from_config(base_config)
    await db.start()
    await db.execute(CREATE, target=TARGET)
    await db.execute(TRUNCATE, target=TARGET)
    try:
        yield db
    finally:
        await db.close()


async def test_insert_and_fetch_mapped(adb: AsyncDatabase) -> None:
    result = await adb.execute(INSERT, {"id": 1, "email": "a@x.com"}, target=TARGET)
    assert result.row_count == 1
    user = await adb.fetch_one(GET, {"id": 1}, target=TARGET, map_to=User)
    assert user == User(id=1, email="a@x.com")


async def test_fetch_optional_and_value(adb: AsyncDatabase) -> None:
    assert await adb.fetch_optional(GET, {"id": 999}, target=TARGET) is None
    val = await adb.fetch_value(sql("SELECT abs(:n)"), {"n": -7}, target=TARGET)
    assert val == 7


async def test_custom_function_via_text(adb: AsyncDatabase) -> None:
    await adb.execute(
        sql(
            "CREATE OR REPLACE FUNCTION dbkit_add(a int, b int) "
            "RETURNS int LANGUAGE sql AS 'SELECT a + b'"
        ),
        target=TARGET,
    )
    val = await adb.fetch_value(sql("SELECT dbkit_add(:a, :b)"), {"a": 40, "b": 2}, target=TARGET)
    assert val == 42


async def test_fetch_one_cardinality_violation(adb: AsyncDatabase) -> None:
    await adb.execute(INSERT, {"id": 1, "email": "a@x.com"}, target=TARGET)
    with pytest.raises(DatabaseResultError):
        await adb.fetch_one(GET, {"id": 999}, target=TARGET)


async def test_transaction_commit(adb: AsyncDatabase) -> None:
    async with adb.transaction(target=TARGET) as tx:
        await tx.execute(INSERT, {"id": 10, "email": "t@x.com"})
        await tx.execute(INSERT, {"id": 11, "email": "u@x.com"})
    count = await adb.fetch_value(sql("SELECT count(*) FROM dbkit_users"), target=TARGET)
    assert count == 2


async def test_transaction_metrics_and_long_running_warning(
    base_config: dict, recording_metrics: RecordingMetrics, caplog: pytest.LogCaptureFixture
) -> None:
    cfg = {
        **base_config,
        "defaults": {**base_config["defaults"], "long_transaction_warning_seconds": 0.2},
    }
    db = AsyncDatabase.from_config(cfg, metrics=recording_metrics)
    await db.start()
    try:
        with caplog.at_level(logging.WARNING, logger="dbkit"):
            async with db.transaction(target=TARGET) as tx:
                await tx.execute(sql("SELECT pg_sleep(0.4)"))

        long_running = [
            r for r in caplog.records if r.dbkit["event"] == "database.transaction.long_running"
        ]
        assert len(long_running) == 1
        assert long_running[0].dbkit["duration_ms"] >= 400
        assert long_running[0].dbkit["outcome"] == "commit"

        assert recording_metrics.count("db_transaction_total") == 1
        assert len(recording_metrics.observe_calls) >= 1
        tx_duration = [
            v
            for n, v, _ in recording_metrics.observe_calls
            if n == "db_transaction_duration_seconds"
        ]
        assert tx_duration == [pytest.approx(tx_duration[0])]
        assert tx_duration[0] >= 0.4
        assert recording_metrics.count("db_transaction_rollback_total") == 0
        assert recording_metrics.count("db_commit_unknown_total") == 0
    finally:
        await db.close()


async def test_transaction_rollback_increments_rollback_metric_exactly_once(
    base_config: dict, recording_metrics: RecordingMetrics
) -> None:
    db = AsyncDatabase.from_config(base_config, metrics=recording_metrics)
    await db.start()
    try:
        await db.execute(CREATE, target=TARGET)
        with pytest.raises(RuntimeError):
            async with db.transaction(target=TARGET) as tx:
                await tx.execute(INSERT, {"id": 900, "email": "x@x.com"})
                raise RuntimeError("boom")

        assert recording_metrics.count("db_transaction_total") == 1
        assert recording_metrics.count("db_transaction_rollback_total") == 1
        assert recording_metrics.count("db_commit_unknown_total") == 0
    finally:
        await db.close()


async def test_transaction_rollback_on_error(adb: AsyncDatabase) -> None:
    with pytest.raises(RuntimeError):
        async with adb.transaction(target=TARGET) as tx:
            await tx.execute(INSERT, {"id": 20, "email": "x@x.com"})
            raise RuntimeError("boom")
    count = await adb.fetch_value(sql("SELECT count(*) FROM dbkit_users"), target=TARGET)
    assert count == 0


async def test_savepoint_partial_rollback(adb: AsyncDatabase) -> None:
    async with adb.transaction(target=TARGET) as tx:
        await tx.execute(INSERT, {"id": 30, "email": "keep@x.com"})
        with pytest.raises(RuntimeError):
            async with tx.savepoint():
                await tx.execute(INSERT, {"id": 31, "email": "drop@x.com"})
                raise RuntimeError("rollback savepoint")
    ids = await adb.fetch_values(sql("SELECT id FROM dbkit_users ORDER BY id"), target=TARGET)
    assert ids == [30]


async def test_unique_violation_classified(adb: AsyncDatabase) -> None:
    await adb.execute(INSERT, {"id": 40, "email": "dup@x.com"}, target=TARGET)
    with pytest.raises(DatabaseUniqueViolationError) as info:
        await adb.execute(INSERT, {"id": 41, "email": "dup@x.com"}, target=TARGET)
    assert info.value.sqlstate == "23505"
    assert info.value.retryable is False


async def test_statement_timeout(adb: AsyncDatabase) -> None:
    with pytest.raises(DatabaseQueryTimeoutError):
        await adb.fetch_value(sql("SELECT pg_sleep(3)"), target=TARGET, timeout=0.3)


async def test_pool_exhaustion(base_config: dict) -> None:
    cfg = {
        **base_config,
        "defaults": {
            **base_config["defaults"],
            "pool": {"size": 1, "max_overflow": 0, "timeout_seconds": 0.5},
        },
    }
    db = AsyncDatabase.from_config(cfg)
    await db.start()
    try:
        release = asyncio.Event()

        async def hold() -> None:
            async with db.connection(target=TARGET):
                await release.wait()

        holder = asyncio.create_task(hold())
        await asyncio.sleep(0.2)  # let the holder grab the only connection
        with pytest.raises(DatabasePoolTimeoutError):
            await db.fetch_value(sql("SELECT 1"), target=TARGET)
        release.set()
        await holder
    finally:
        await db.close()


async def test_cancellation_releases_connection(adb: AsyncDatabase) -> None:
    task = asyncio.create_task(
        adb.fetch_value(sql("SELECT pg_sleep(5)"), target=TARGET, timeout=10)
    )
    await asyncio.sleep(0.3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # give the pool a moment to reclaim/close the connection
    await asyncio.sleep(0.2)
    snap = adb.pool_status()[0]
    assert snap.checked_out == 0


async def test_pre_ping_recovers_after_backend_termination(base_config: dict) -> None:
    """Killing the pooled backend mid-life is transparently recovered on next checkout (§10.6)."""
    cfg = {
        **base_config,
        "defaults": {
            **base_config["defaults"],
            "pool": {"size": 1, "max_overflow": 0, "pre_ping": True, "timeout_seconds": 2.0},
        },
    }
    db = AsyncDatabase.from_config(cfg)
    await db.start()
    try:
        pid1 = await db.fetch_value(sql("SELECT pg_backend_pid()"), target=TARGET)
        # Terminate that backend from an independent admin database instance.
        admin = AsyncDatabase.from_config(base_config)
        await admin.start()
        try:
            await admin.fetch_value(
                sql("SELECT pg_terminate_backend(:pid)"), {"pid": pid1}, target=TARGET
            )
        finally:
            await admin.close()
        # The next operation must succeed on a fresh connection — no raw driver error escapes.
        pid2 = await db.fetch_value(sql("SELECT pg_backend_pid()"), target=TARGET)
        assert pid2 != pid1
    finally:
        await db.close()


async def test_health_and_pool_status(adb: AsyncDatabase) -> None:
    report = await adb.health()
    assert report.live is True
    assert report.ready is True
    assert report.targets[0].healthy is True
    snaps = adb.pool_status()
    assert snaps and snaps[0].checked_out == 0
