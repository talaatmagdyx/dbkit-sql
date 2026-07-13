"""Sync integration tests — the generated ``Database`` frontend against real PostgreSQL."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

import pytest

from dbkit import Database, DatabaseTarget, Query, sql
from dbkit.errors import DatabaseUniqueViolationError

pytestmark = pytest.mark.integration

TARGET = DatabaseTarget(database="app", role="write")

CREATE = sql(
    """
    CREATE TABLE IF NOT EXISTS dbkit_sync_users (
        id    integer PRIMARY KEY,
        email text NOT NULL UNIQUE
    )
    """
)
INSERT = Query(
    name="users.insert",
    statement=sql("INSERT INTO dbkit_sync_users (id, email) VALUES (:id, :email)"),
    operation="write",
)
GET = Query(
    name="users.get",
    statement=sql("SELECT id, email FROM dbkit_sync_users WHERE id = :id"),
    operation="read",
    idempotent=True,
)


@dataclass
class User:
    id: int
    email: str


@pytest.fixture
def sdb(base_config: dict) -> Iterator[Database]:
    db = Database.from_config(base_config)
    db.start()
    db.execute(CREATE, target=TARGET)
    db.execute(sql("TRUNCATE dbkit_sync_users"), target=TARGET)
    try:
        yield db
    finally:
        db.close()


def test_sync_insert_and_fetch(sdb: Database) -> None:
    sdb.execute(INSERT, {"id": 1, "email": "a@x.com"}, target=TARGET)
    user = sdb.fetch_one(GET, {"id": 1}, target=TARGET, map_to=User)
    assert user == User(id=1, email="a@x.com")


def test_sync_transaction_commit(sdb: Database) -> None:
    with sdb.transaction(target=TARGET) as tx:
        tx.execute(INSERT, {"id": 2, "email": "b@x.com"})
        tx.execute(INSERT, {"id": 3, "email": "c@x.com"})
    count = sdb.fetch_value(sql("SELECT count(*) FROM dbkit_sync_users"), target=TARGET)
    assert count == 2


def test_sync_transaction_rollback(sdb: Database) -> None:
    with pytest.raises(RuntimeError):
        with sdb.transaction(target=TARGET) as tx:
            tx.execute(INSERT, {"id": 4, "email": "d@x.com"})
            raise RuntimeError("boom")
    count = sdb.fetch_value(sql("SELECT count(*) FROM dbkit_sync_users"), target=TARGET)
    assert count == 0


def test_sync_unique_violation(sdb: Database) -> None:
    sdb.execute(INSERT, {"id": 5, "email": "dup@x.com"}, target=TARGET)
    with pytest.raises(DatabaseUniqueViolationError):
        sdb.execute(INSERT, {"id": 6, "email": "dup@x.com"}, target=TARGET)


def test_sync_health(sdb: Database) -> None:
    report = sdb.health()
    assert report.ready is True
    assert sdb.pool_status()[0].checked_out == 0
