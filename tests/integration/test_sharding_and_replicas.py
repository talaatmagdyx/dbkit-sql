"""Integration tests for replica routing, read-your-writes, and sharding against real PostgreSQL.

A single PostgreSQL instance stands in for both "primary" and "replica" targets — the point
here is to prove dbkit's *routing* logic (which engine a read/write lands on, and the
read-your-writes override), not physical streaming replication.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from dbkit import AsyncDatabase, DatabaseTarget, HashShardResolver, sql

pytestmark = pytest.mark.integration

WRITE = DatabaseTarget(database="app", role="write")
READ = DatabaseTarget(database="app", role="read")
PRIMARY_ONLY = DatabaseTarget(database="app", role="primary_only")


@pytest.fixture
async def db_with_replica(base_config: dict) -> AsyncIterator[AsyncDatabase]:
    dsn = base_config["databases"]["app"]["primary"]["url"]
    cfg = {
        **base_config,
        "databases": {
            "app": {
                "primary": {"url": dsn},
                "replicas": [{"name": "r1", "url": dsn}],
            }
        },
    }
    d = AsyncDatabase.from_config(cfg)
    await d.start()
    await d.execute(
        sql("CREATE TABLE IF NOT EXISTS dbkit_shard_demo (id int PRIMARY KEY, v int)"),
        target=WRITE,
    )
    await d.execute(sql("TRUNCATE dbkit_shard_demo"), target=WRITE)
    try:
        yield d
    finally:
        await d.close()


async def test_read_role_routes_to_replica_engine(db_with_replica: AsyncDatabase) -> None:
    await db_with_replica.fetch_value(sql("SELECT 1"), target=READ)
    snaps = db_with_replica.pool_status()
    keys = {s.key for s in snaps}
    assert any(":replica:r1:" in k for k in keys), f"expected a replica engine, got {keys}"


async def test_write_role_never_routes_to_replica(db_with_replica: AsyncDatabase) -> None:
    await db_with_replica.execute(
        sql("INSERT INTO dbkit_shard_demo (id, v) VALUES (1, 1)"), target=WRITE
    )
    snaps = db_with_replica.pool_status()
    keys = {s.key for s in snaps}
    assert all(":replica:" not in k for k in keys), f"write must never hit a replica: {keys}"


async def test_primary_only_bypasses_replica_even_when_configured(
    db_with_replica: AsyncDatabase,
) -> None:
    await db_with_replica.fetch_value(sql("SELECT 1"), target=PRIMARY_ONLY)
    snaps = db_with_replica.pool_status()
    keys = {s.key for s in snaps}
    assert all(":replica:" not in k for k in keys)


async def test_read_your_writes_forces_primary(db_with_replica: AsyncDatabase) -> None:
    async with db_with_replica.consistency_scope(mode="read_your_writes"):
        await db_with_replica.execute(
            sql("INSERT INTO dbkit_shard_demo (id, v) VALUES (2, 2)"), target=WRITE
        )
        value = await db_with_replica.fetch_value(
            sql("SELECT v FROM dbkit_shard_demo WHERE id = 2"), target=READ
        )
        assert value == 2

    # Confirm the read really would have preferred a replica outside the scope.
    await db_with_replica.fetch_value(sql("SELECT 1"), target=READ)
    keys = {s.key for s in db_with_replica.pool_status()}
    assert any(":replica:r1:" in k for k in keys)


async def test_consistency_scope_is_task_local(db_with_replica: AsyncDatabase) -> None:
    """The read_your_writes override must not leak into concurrent tasks (§23)."""
    import asyncio

    async def read_outside_scope() -> str:
        await asyncio.sleep(0.05)  # let the other task's scope be active concurrently
        route = db_with_replica._resolve(READ)
        return route.role

    async def with_scope() -> None:
        async with db_with_replica.consistency_scope(mode="read_your_writes"):
            await asyncio.sleep(0.1)

    other_role, _ = await asyncio.gather(read_outside_scope(), with_scope())
    assert other_role == "replica"  # unaffected by the concurrent task's scope


async def test_hash_shard_resolver_routes_deterministically(base_config: dict) -> None:
    dsn = base_config["databases"]["app"]["primary"]["url"]
    resolver = HashShardResolver(4)
    db = AsyncDatabase.from_config(
        {"databases": {"app": {"primary": {"url": dsn}}}}, shard_resolver=resolver
    )
    await db.start()
    try:
        target = DatabaseTarget(database="app", role="write", shard_key="tenant-123")
        await db.fetch_value(sql("SELECT 1"), target=target)
        expected_shard = resolver.resolve("app", "tenant-123")
        keys = {s.key for s in db.pool_status()}
        assert any(f":{expected_shard}:" in k for k in keys)
    finally:
        await db.close()
