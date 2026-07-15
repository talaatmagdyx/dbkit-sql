"""dbkit.testing fakes — record calls, return queued rows, mirror both APIs."""

from __future__ import annotations

import pytest

from dbkit import DatabaseTarget, Query, sql
from dbkit.errors import DatabaseResultError
from dbkit.testing import FakeAsyncDatabase, FakeDatabase

TARGET = DatabaseTarget(database="app", role="read")
LIST_USERS = Query(name="users.list", statement=sql("SELECT * FROM users WHERE org = :org"))


async def test_fetch_all_records_and_returns_queued() -> None:
    fake = FakeAsyncDatabase()
    fake.queue_rows([{"id": 1}, {"id": 2}])
    rows = await fake.fetch_all(LIST_USERS, {"org": 7}, target=TARGET)
    assert rows == [{"id": 1}, {"id": 2}]
    call = fake.calls[0]
    assert call.method == "fetch_all"
    assert call.query_name == "users.list"
    assert call.params == {"org": 7}
    assert call.target.database == "app"
    assert ":org" in call.statement


async def test_fetch_one_requires_exactly_one_row() -> None:
    fake = FakeAsyncDatabase()
    fake.queue_rows([])
    with pytest.raises(DatabaseResultError):
        await fake.fetch_one(LIST_USERS, target=TARGET)


async def test_fetch_value_returns_first_column() -> None:
    fake = FakeAsyncDatabase()
    fake.queue_rows([{"count": 42}])
    assert await fake.fetch_value(LIST_USERS, target=TARGET) == 42


async def test_transaction_records_in_transaction_flag() -> None:
    fake = FakeAsyncDatabase()
    fake.queue_rows([{"n": 1}])
    async with fake.transaction(target=TARGET) as tx:
        await tx.fetch_one(LIST_USERS, target=TARGET)
    assert fake.calls[0].in_transaction is True


async def test_settings_recorded() -> None:
    fake = FakeAsyncDatabase()
    q = Query(name="q", statement=sql("SELECT 1"), settings={"jit": "off"})
    await fake.fetch_all(q, target=TARGET)
    assert fake.calls[0].settings == {"jit": "off"}


async def test_dynamic_registration_tracked() -> None:
    fake = FakeAsyncDatabase()
    assert await fake.ensure_database("s1", {"primary": {"url": "x"}}) is True
    assert await fake.ensure_database("s1", {"primary": {"url": "x"}}) is False  # unchanged
    assert await fake.unregister_database("s1") is True
    assert fake.registered == {}


async def test_lifecycle_flags() -> None:
    fake = FakeAsyncDatabase()
    await fake.start()
    await fake.close()
    assert fake.started and fake.closed


def test_sync_fake_mirror() -> None:
    fake = FakeDatabase()
    fake.queue_rows([{"id": 1}])
    with fake.transaction(target=TARGET) as tx:
        rows = tx.fetch_all(LIST_USERS, {"org": 1}, target=TARGET)
    assert rows == [{"id": 1}]
    assert fake.calls[0].in_transaction is True
    assert fake.calls_named("users.list")
