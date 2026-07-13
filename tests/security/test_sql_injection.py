"""Security (integration): parameter binding neutralizes injection attempts (§18.4)."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from dbkit import AsyncDatabase, DatabaseTarget, Query, sql

pytestmark = pytest.mark.integration

TARGET = DatabaseTarget(database="app", role="write")
INSERT = Query(
    name="sec.insert",
    statement=sql("INSERT INTO dbkit_sec (id, name) VALUES (:id, :name)"),
    operation="write",
)
GET = Query(name="sec.get", statement=sql("SELECT name FROM dbkit_sec WHERE id = :id"))


@pytest.fixture
async def db(base_config: dict) -> AsyncIterator[AsyncDatabase]:
    d = AsyncDatabase.from_config(base_config)
    await d.start()
    await d.execute(
        sql("CREATE TABLE IF NOT EXISTS dbkit_sec (id int PRIMARY KEY, name text)"), target=TARGET
    )
    await d.execute(sql("TRUNCATE dbkit_sec"), target=TARGET)
    try:
        yield d
    finally:
        await d.close()


async def test_injection_payload_is_stored_literally(db: AsyncDatabase) -> None:
    payload = "Robert'); DROP TABLE dbkit_sec; --"
    await db.execute(INSERT, {"id": 1, "name": payload}, target=TARGET)
    # The table still exists and the payload was stored verbatim, not executed.
    stored = await db.fetch_value(GET, {"id": 1}, target=TARGET)
    assert stored == payload
    count = await db.fetch_value(sql("SELECT count(*) FROM dbkit_sec"), target=TARGET)
    assert count == 1


async def test_unicode_and_quotes_roundtrip(db: AsyncDatabase) -> None:
    for i, name in enumerate(["O'Brien", 'quote"inside', "セミコロン;", "back\\slash"], start=10):
        await db.execute(INSERT, {"id": i, "name": name}, target=TARGET)
        assert await db.fetch_value(GET, {"id": i}, target=TARGET) == name
