"""Integration tests for Phase 3 throughput paths: streaming, bulk, COPY, inbox (§19, §20, §28)."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, Text

from dbkit import AsyncDatabase, DatabaseTarget, sql
from dbkit.integrations import BatchCollector, ack_after_commit, inbox_ddl, process_once

pytestmark = pytest.mark.integration

TARGET = DatabaseTarget(database="app", role="write")
READ = DatabaseTarget(database="app", role="read")

_md = MetaData()
BULK = Table("dbkit_p3_bulk", _md, Column("id", Integer, primary_key=True), Column("v", Text))


@pytest.fixture
async def db(base_config: dict) -> AsyncIterator[AsyncDatabase]:
    d = AsyncDatabase.from_config(base_config)
    await d.start()
    try:
        yield d
    finally:
        await d.close()


# --- streaming ---------------------------------------------------------------------- #


async def test_stream_large_result_bounded(db: AsyncDatabase) -> None:
    seen = 0
    last = 0
    async with await db.stream(
        sql("SELECT i FROM generate_series(1, 20000) AS i"),
        target=READ,
        batch_size=1000,
    ) as rows:
        async for row in rows:
            seen += 1
            last = row["i"]
    assert seen == 20000
    assert last == 20000
    assert db.pool_status()[0].checked_out == 0  # connection released


async def test_stream_map_to(db: AsyncDatabase) -> None:
    async with await db.stream(
        sql("SELECT i FROM generate_series(1, 10) AS i"), target=READ, map_to=dict
    ) as rows:
        collected = [r async for r in rows]
    assert collected[0] == {"i": 1}
    assert len(collected) == 10


# --- bulk --------------------------------------------------------------------------- #


async def test_insert_many_atomic(db: AsyncDatabase) -> None:
    await db.execute(
        sql("CREATE TABLE IF NOT EXISTS dbkit_p3_bulk (id int primary key, v text)"), target=TARGET
    )
    await db.execute(sql("TRUNCATE dbkit_p3_bulk"), target=TARGET)
    result = await db.insert_many(
        BULK, [{"id": i, "v": f"n{i}"} for i in range(3000)], target=TARGET, batch_size=1000
    )
    assert result.row_count == 3000
    count = await db.fetch_value(sql("SELECT count(*) FROM dbkit_p3_bulk"), target=READ)
    assert count == 3000


async def test_insert_many_atomic_rolls_back_on_error(db: AsyncDatabase) -> None:
    from dbkit.errors import DatabaseUniqueViolationError

    await db.execute(sql("TRUNCATE dbkit_p3_bulk"), target=TARGET)
    rows = [{"id": i, "v": "x"} for i in range(50)] + [{"id": 10, "v": "dup"}]
    with pytest.raises(DatabaseUniqueViolationError):
        await db.insert_many(BULK, rows, target=TARGET, mode="atomic")
    # atomic => nothing committed
    assert await db.fetch_value(sql("SELECT count(*) FROM dbkit_p3_bulk"), target=READ) == 0


async def test_insert_many_split_on_failure_isolates_bad_rows(db: AsyncDatabase) -> None:
    await db.execute(sql("TRUNCATE dbkit_p3_bulk"), target=TARGET)
    rows = [{"id": i, "v": "x"} for i in range(100)] + [{"id": 50, "v": "dup"}]
    result = await db.insert_many(
        BULK, rows, target=TARGET, mode="split_on_failure", batch_size=200
    )
    assert result.row_count == 100
    assert await db.fetch_value(sql("SELECT count(*) FROM dbkit_p3_bulk"), target=READ) == 100


async def test_upsert_many_updates_on_conflict(db: AsyncDatabase) -> None:
    await db.execute(sql("TRUNCATE dbkit_p3_bulk"), target=TARGET)
    await db.insert_many(BULK, [{"id": i, "v": "old"} for i in range(10)], target=TARGET)
    result = await db.upsert_many(
        BULK,
        [{"id": i, "v": "new"} for i in range(5, 15)],
        target=TARGET,
        conflict_index_elements=["id"],
        update_columns=["v"],
    )
    assert result.row_count > 0
    assert await db.fetch_value(sql("SELECT v FROM dbkit_p3_bulk WHERE id=7"), target=READ) == "new"
    assert await db.fetch_value(sql("SELECT count(*) FROM dbkit_p3_bulk"), target=READ) == 15


# --- unnest bulk strategy ------------------------------------------------------------- #


async def test_insert_many_unnest_strategy(db: AsyncDatabase) -> None:
    await db.execute(sql("TRUNCATE dbkit_p3_bulk"), target=TARGET)
    result = await db.insert_many(
        BULK,
        [{"id": i, "v": f"n{i}"} for i in range(3000)],
        target=TARGET,
        strategy="unnest",
        batch_size=1000,
    )
    assert result.row_count == 3000
    count = await db.fetch_value(sql("SELECT count(*) FROM dbkit_p3_bulk"), target=READ)
    assert count == 3000
    sample = await db.fetch_value(sql("SELECT v FROM dbkit_p3_bulk WHERE id = 1500"), target=READ)
    assert sample == "n1500"


async def test_upsert_many_unnest_strategy(db: AsyncDatabase) -> None:
    await db.execute(sql("TRUNCATE dbkit_p3_bulk"), target=TARGET)
    await db.insert_many(BULK, [{"id": i, "v": "old"} for i in range(10)], target=TARGET)
    result = await db.upsert_many(
        BULK,
        [{"id": i, "v": "new"} for i in range(5, 15)],
        target=TARGET,
        conflict_index_elements=["id"],
        update_columns=["v"],
        strategy="unnest",
    )
    assert result.row_count == 10
    assert await db.fetch_value(sql("SELECT v FROM dbkit_p3_bulk WHERE id=7"), target=READ) == "new"
    assert await db.fetch_value(sql("SELECT count(*) FROM dbkit_p3_bulk"), target=READ) == 15


async def test_insert_many_unnest_split_on_failure(db: AsyncDatabase) -> None:
    await db.execute(sql("TRUNCATE dbkit_p3_bulk"), target=TARGET)
    rows = [{"id": i, "v": "x"} for i in range(100)] + [{"id": 50, "v": "dup"}]
    result = await db.insert_many(
        BULK, rows, target=TARGET, mode="split_on_failure", strategy="unnest", batch_size=200
    )
    assert result.row_count == 100
    assert await db.fetch_value(sql("SELECT count(*) FROM dbkit_p3_bulk"), target=READ) == 100


async def test_insert_many_unnest_rolls_back_atomically(db: AsyncDatabase) -> None:
    from dbkit.errors import DatabaseUniqueViolationError

    await db.execute(sql("TRUNCATE dbkit_p3_bulk"), target=TARGET)
    rows = [{"id": i, "v": "x"} for i in range(50)] + [{"id": 10, "v": "dup"}]
    with pytest.raises(DatabaseUniqueViolationError):
        await db.insert_many(BULK, rows, target=TARGET, mode="atomic", strategy="unnest")
    assert await db.fetch_value(sql("SELECT count(*) FROM dbkit_p3_bulk"), target=READ) == 0


# --- pipeline mode (psycopg escape hatch) --------------------------------------------- #


async def test_pipeline_mode_dependent_statements(db: AsyncDatabase) -> None:
    await db.execute(
        sql("CREATE TABLE IF NOT EXISTS dbkit_p3_pipe_a (id int primary key, v text)"),
        target=TARGET,
    )
    await db.execute(
        sql("CREATE TABLE IF NOT EXISTS dbkit_p3_pipe_b (id int primary key, order_id int)"),
        target=TARGET,
    )
    await db.execute(sql("TRUNCATE dbkit_p3_pipe_a, dbkit_p3_pipe_b"), target=TARGET)

    async with db.transaction(target=TARGET) as tx, tx.pipeline():
        for i in range(200):
            await tx.execute(
                sql("INSERT INTO dbkit_p3_pipe_a (id, v) VALUES (:id, :v)"),
                {"id": i, "v": f"order-{i}"},
            )
            await tx.execute(
                sql("INSERT INTO dbkit_p3_pipe_b (id, order_id) VALUES (:id, :order_id)"),
                {"id": i, "order_id": i},
            )

    count_a = await db.fetch_value(sql("SELECT count(*) FROM dbkit_p3_pipe_a"), target=READ)
    count_b = await db.fetch_value(sql("SELECT count(*) FROM dbkit_p3_pipe_b"), target=READ)
    assert count_a == 200
    assert count_b == 200


async def test_pipeline_mode_rollback_still_discards(db: AsyncDatabase) -> None:
    await db.execute(sql("TRUNCATE dbkit_p3_pipe_a, dbkit_p3_pipe_b"), target=TARGET)
    with pytest.raises(RuntimeError):
        async with db.transaction(target=TARGET) as tx, tx.pipeline():
            await tx.execute(
                sql("INSERT INTO dbkit_p3_pipe_a (id, v) VALUES (:id, :v)"),
                {"id": 9999, "v": "will vanish"},
            )
            raise RuntimeError("boom")
    exists = await db.fetch_optional(
        sql("SELECT 1 FROM dbkit_p3_pipe_a WHERE id = 9999"), target=READ
    )
    assert exists is None


# --- PgBouncer-compatible pooling mode ------------------------------------------------- #


async def test_pgbouncer_compatible_disables_prepare_threshold(base_config: dict) -> None:
    cfg = {
        **base_config,
        "defaults": {**base_config["defaults"], "pool": {"pgbouncer_compatible": True}},
    }
    db = AsyncDatabase.from_config(cfg)
    await db.start()
    try:
        entry = next(iter(db._registry._entries.values()))
        async with entry.engine.connect() as conn:
            raw = await conn.get_raw_connection()
            assert raw.driver_connection.prepare_threshold is None
        # still fully functional with autoprep disabled
        for i in range(10):
            assert await db.fetch_value(sql("SELECT :n"), {"n": i}, target=READ) == i
    finally:
        await db.close()


async def test_pgbouncer_compatible_off_by_default(base_config: dict) -> None:
    db = AsyncDatabase.from_config(base_config)
    await db.start()
    try:
        entry = next(iter(db._registry._entries.values()))
        async with entry.engine.connect() as conn:
            raw = await conn.get_raw_connection()
            assert raw.driver_connection.prepare_threshold == 5  # psycopg's own default
    finally:
        await db.close()


# --- COPY --------------------------------------------------------------------------- #


async def test_copy_records(db: AsyncDatabase) -> None:
    await db.execute(
        sql("CREATE TABLE IF NOT EXISTS dbkit_p3_copy (id int, v text)"), target=TARGET
    )
    await db.execute(sql("TRUNCATE dbkit_p3_copy"), target=TARGET)
    result = await db.copy_records(
        "dbkit_p3_copy", ["id", "v"], ((i, f"c{i}") for i in range(15000)), target=TARGET
    )
    assert result.row_count == 15000
    assert await db.fetch_value(sql("SELECT count(*) FROM dbkit_p3_copy"), target=READ) == 15000


# --- inbox / idempotency ------------------------------------------------------------ #


async def test_inbox_processes_exactly_once(db: AsyncDatabase) -> None:
    await db.execute(sql(inbox_ddl()), target=TARGET)
    await db.execute(sql("DELETE FROM consumed_messages WHERE consumer_name='t'"), target=TARGET)
    await db.execute(
        sql("CREATE TABLE IF NOT EXISTS dbkit_p3_orders (mid text primary key)"), target=TARGET
    )
    await db.execute(sql("TRUNCATE dbkit_p3_orders"), target=TARGET)

    async def work(tx: object) -> None:
        await tx.execute(sql("INSERT INTO dbkit_p3_orders (mid) VALUES (:m)"), {"m": "msg-1"})

    acks = 0

    async def ack() -> None:
        nonlocal acks
        acks += 1

    for _ in range(3):  # redelivered three times
        processed = await ack_after_commit(
            db, consumer="t", message_id="msg-1", target=TARGET, work=work, ack=ack
        )
        assert processed is True

    assert await db.fetch_value(sql("SELECT count(*) FROM dbkit_p3_orders"), target=READ) == 1
    assert acks == 3  # acked every delivery, but the write happened once


async def test_process_once_reports_duplicate(db: AsyncDatabase) -> None:
    await db.execute(sql(inbox_ddl()), target=TARGET)
    await db.execute(sql("DELETE FROM consumed_messages WHERE consumer_name='t2'"), target=TARGET)
    firsts = []
    for _ in range(2):
        async with process_once(db, consumer="t2", message_id="m", target=TARGET) as (_tx, first):
            firsts.append(first)
    assert firsts == [True, False]


# --- batch collector driving a COPY ------------------------------------------------- #


async def test_batch_collector_drives_copy(db: AsyncDatabase) -> None:
    await db.execute(
        sql("CREATE TABLE IF NOT EXISTS dbkit_p3_batch (id int, v text)"), target=TARGET
    )
    await db.execute(sql("TRUNCATE dbkit_p3_batch"), target=TARGET)

    async def flush(items: object) -> None:
        await db.copy_records("dbkit_p3_batch", ["id", "v"], list(items), target=TARGET)

    bc: BatchCollector = BatchCollector(flush, max_size=500, max_delay_ms=50)
    for i in range(1200):
        await bc.add((i, f"b{i}"))
    await bc.close()
    assert await db.fetch_value(sql("SELECT count(*) FROM dbkit_p3_batch"), target=READ) == 1200
