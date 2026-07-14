"""Async integration tests against a real PostgreSQL (§32.2). Marked ``integration``."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from unittest.mock import patch

import pytest

from dbkit import AsyncDatabase, DatabaseTarget, Query, sql
from dbkit.errors import (
    DatabaseCommitUnknownError,
    DatabaseError,
    DatabaseOverloadedError,
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


async def test_one_shot_calls_resolve_entry_and_labels_exactly_once_each(
    adb: AsyncDatabase,
) -> None:
    """Performance review §4/§11 Finding #5: ``execute_with_resilience`` and ``scope()`` used to
    each independently resolve the engine entry and rebuild an identical labels dict — meaning a
    single ``fetch_value`` call touched both twice. ``scope()`` now accepts the already-resolved
    ``entry``/``labels`` from ``execute_with_resilience`` instead of recomputing them."""
    executor = adb._executor
    entry_calls = 0
    labels_calls = 0
    original_entry = executor.entry
    original_labels = executor.labels

    async def counting_entry(target):
        nonlocal entry_calls
        entry_calls += 1
        return await original_entry(target)

    def counting_labels(entry, query_name, operation):
        nonlocal labels_calls
        labels_calls += 1
        return original_labels(entry, query_name, operation)

    with (
        patch.object(executor, "entry", side_effect=counting_entry),
        patch.object(executor, "labels", side_effect=counting_labels),
    ):
        val = await adb.fetch_value(sql("SELECT 1"), target=TARGET)

    assert val == 1
    assert entry_calls == 1
    assert labels_calls == 1


async def test_expensive_query_tier_bounds_concurrency_independently_of_writes_tier(
    base_config: dict,
) -> None:
    """Performance review §5 Finding #9: ``ConcurrencyConfig.expensive_queries`` was declared
    but never wired into a real semaphore. ``Query(expensive=True)`` now acquires a separate
    tier *in addition to* the normal reads/writes tier, so a small number of known-heavy
    queries can be bounded independently of ordinary traffic."""
    cfg = {
        **base_config,
        "databases": {
            "app": {**base_config["databases"]["app"], "concurrency": {"expensive_queries": 1}}
        },
    }
    db = AsyncDatabase.from_config(cfg)
    await db.start()
    try:
        heavy = Query(
            name="expensive.sleep",
            statement=sql("SELECT pg_sleep(0.3)"),
            operation="read",
            expensive=True,
        )
        light = Query(
            name="expensive.probe", statement=sql("SELECT 1"), operation="read", expensive=True
        )

        # Saturate the single expensive-tier slot with a slow query...
        task = asyncio.create_task(db.fetch_value(heavy, target=TARGET))
        await asyncio.sleep(0.05)
        # ...a second expensive query is bounded by the *same* tier and times out quickly,
        # even though the ordinary "reads" tier is nowhere near saturated.
        with pytest.raises(DatabaseOverloadedError, match="expensive"):
            await db.fetch_value(light, target=TARGET, timeout=0.1)
        await task  # let the first one finish cleanly
    finally:
        await db.close()


async def test_non_expensive_query_is_unaffected_by_the_expensive_tier(
    base_config: dict,
) -> None:
    """A query not marked ``expensive=True`` must never touch the expensive tier at all — it
    should proceed normally even while that tier is fully saturated."""
    cfg = {
        **base_config,
        "databases": {
            "app": {**base_config["databases"]["app"], "concurrency": {"expensive_queries": 1}}
        },
    }
    db = AsyncDatabase.from_config(cfg)
    await db.start()
    try:
        heavy = Query(
            name="expensive.sleep2",
            statement=sql("SELECT pg_sleep(0.3)"),
            operation="read",
            expensive=True,
        )
        task = asyncio.create_task(db.fetch_value(heavy, target=TARGET))
        await asyncio.sleep(0.05)
        # ordinary query, not marked expensive -- unaffected by the saturated expensive tier
        assert await db.fetch_value(sql("SELECT 1"), target=TARGET, timeout=1.0) == 1
        await task
    finally:
        await db.close()


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


async def test_execute_commit_failure_with_broken_connection_raises_commit_unknown(
    adb: AsyncDatabase,
) -> None:
    """``db.execute()``'s auto-commit path must give the same commit-unknown guarantee as an
    explicit ``db.transaction()`` — a connection failure during COMMIT is genuinely ambiguous
    (§15) and must never be silently retried or leaked as a raw, unclassified exception."""
    from sqlalchemy.exc import OperationalError
    from sqlalchemy.ext.asyncio import AsyncConnection

    async def flaky_commit(self: AsyncConnection) -> None:
        raise OperationalError(
            "COMMIT", {}, ConnectionResetError("simulated drop"), connection_invalidated=True
        )

    with patch.object(AsyncConnection, "commit", flaky_commit):
        with pytest.raises(DatabaseCommitUnknownError) as info:
            await adb.execute(INSERT, {"id": 12, "email": "commit-unknown@x.com"}, target=TARGET)
    assert info.value.transaction_state_unknown is True


async def test_execute_commit_failure_without_broken_connection_is_classified(
    adb: AsyncDatabase,
) -> None:
    """A non-connection COMMIT failure must still surface as a normalized ``DatabaseError``,
    not a raw driver/SQLAlchemy exception."""
    from sqlalchemy.ext.asyncio import AsyncConnection

    async def flaky_commit(self: AsyncConnection) -> None:
        raise RuntimeError("some non-connection commit-time failure")

    with patch.object(AsyncConnection, "commit", flaky_commit):
        with pytest.raises(DatabaseError) as info:
            await adb.execute(INSERT, {"id": 13, "email": "classified@x.com"}, target=TARGET)
    assert not isinstance(info.value, DatabaseCommitUnknownError)


async def test_transaction_isolation_read_only_deferrable(adb: AsyncDatabase) -> None:
    async with adb.transaction(
        target=TARGET, isolation="serializable", read_only=True, deferrable=True
    ) as tx:
        row = await tx.fetch_one(
            sql(
                "SELECT current_setting('transaction_isolation') AS iso, "
                "current_setting('transaction_read_only') AS ro, "
                "current_setting('transaction_deferrable') AS defer"
            )
        )
    assert row["iso"] == "serializable"
    assert row["ro"] == "on"
    assert row["defer"] == "on"


async def test_transaction_isolation_repeatable_read(adb: AsyncDatabase) -> None:
    async with adb.transaction(target=TARGET, isolation="repeatable_read") as tx:
        val = await tx.fetch_value(sql("SELECT current_setting('transaction_isolation')"))
    assert val == "repeatable read"


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
