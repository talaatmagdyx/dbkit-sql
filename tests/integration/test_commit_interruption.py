"""Cancellation during a real COMMIT, on PostgreSQL: it propagates unchanged and the connection,
whose transaction outcome is unknown, is invalidated (its backend goes away), not reused.

This checks the end-to-end result. SQLAlchemy also invalidates a connection on such exceptions,
so dbkit's own explicit invalidate is pinned by tests/unit/test_commit_cancellation.py."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import Any

import pytest

from dbkit import AsyncDatabase, DatabaseTarget, sql

pytestmark = pytest.mark.integration

TARGET = DatabaseTarget(database="app", role="write")
SETUP = [
    "CREATE TABLE IF NOT EXISTS dbkit_slow_commit (id integer PRIMARY KEY, pid integer NOT NULL)",
    "TRUNCATE dbkit_slow_commit",
    "CREATE OR REPLACE FUNCTION dbkit_sleep_at_commit() RETURNS trigger LANGUAGE plpgsql AS "
    "$$BEGIN PERFORM pg_sleep(1); RETURN NULL; END$$",
    "DROP TRIGGER IF EXISTS dbkit_sleep_at_commit ON dbkit_slow_commit",
    "CREATE CONSTRAINT TRIGGER dbkit_sleep_at_commit AFTER INSERT ON dbkit_slow_commit "
    "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION dbkit_sleep_at_commit()",
]
INSERT = sql("INSERT INTO dbkit_slow_commit (id, pid) VALUES (:id, pg_backend_pid()) RETURNING pid")
ALIVE = sql("SELECT count(*) AS n FROM pg_stat_activity WHERE pid = :pid")


@pytest.fixture
async def adb(base_config: dict) -> AsyncIterator[AsyncDatabase]:
    db = AsyncDatabase.from_config(base_config)
    await db.start()
    for stmt in SETUP:
        await db.execute(sql(stmt), target=TARGET)
    try:
        yield db
    finally:
        await db.execute(sql("DROP TABLE IF EXISTS dbkit_slow_commit"), target=TARGET)
        await db.close()


async def _in_transaction(db: AsyncDatabase, pids: list[int]) -> None:
    async with db.transaction(target=TARGET) as tx:
        pids.append((await tx.fetch_one(INSERT, {"id": 1}))["pid"])


async def _auto_commit(db: AsyncDatabase, pids: list[int]) -> None:
    async with db.connection(target=TARGET) as conn:  # the auto-commit scope behind db.execute
        pids.append((await conn.fetch_one(INSERT, {"id": 1}))["pid"])


@pytest.mark.parametrize("path", [_in_transaction, _auto_commit])
async def test_cancel_during_commit_propagates_and_discards_the_connection(
    adb: AsyncDatabase, path: Callable[[AsyncDatabase, list[int]], Coroutine[Any, Any, None]]
) -> None:
    pids: list[int] = []
    task = asyncio.create_task(path(adb, pids))
    for _ in range(100):  # until the statement ran and COMMIT is sleeping in the deferred trigger
        if pids:
            break
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    for _ in range(50):  # the invalidated connection is closed, so its backend exits
        if (await adb.fetch_one(ALIVE, {"pid": pids[0]}, target=TARGET))["n"] == 0:
            break
        await asyncio.sleep(0.1)
    assert (await adb.fetch_one(ALIVE, {"pid": pids[0]}, target=TARGET))["n"] == 0
