"""Sync quickstart — the same program as ``quickstart_async.py`` without ``await``.

    DBKIT_DSN=postgresql+psycopg://localhost/postgres python examples/quickstart_sync.py
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dbkit import Database, DatabaseTarget, Query, sql

DSN = os.environ.get("DBKIT_DSN", "postgresql+psycopg://localhost/postgres")
TARGET = DatabaseTarget(database="app", role="write")

CREATE_MESSAGES = sql(
    "CREATE TABLE IF NOT EXISTS dbkit_sync_messages ("
    " id integer PRIMARY KEY, body text NOT NULL)"
)
INSERT_MESSAGE = Query(
    name="messages.insert",
    statement=sql("INSERT INTO dbkit_sync_messages (id, body) VALUES (:id, :body)"),
    operation="write",
)
GET_MESSAGE = Query(
    name="messages.get_by_id",
    statement=sql("SELECT id, body FROM dbkit_sync_messages WHERE id = :id"),
    operation="read",
    idempotent=True,
    timeout=1.0,
)


@dataclass
class Message:
    id: int
    body: str


def main() -> None:
    db = Database.from_config({"databases": {"app": {"primary": {"url": DSN}}}})
    db.start()
    db.require_ready()
    try:
        db.execute(CREATE_MESSAGES, target=TARGET)
        with db.transaction(target=TARGET) as tx:
            tx.execute(INSERT_MESSAGE, {"id": 1, "body": "hello"})
            tx.execute(INSERT_MESSAGE, {"id": 2, "body": "world"})
        msg = db.fetch_optional(GET_MESSAGE, {"id": 1}, target=TARGET, map_to=Message)
        print("fetched:", msg)
        for snap in db.pool_status():
            print("pool:", snap.to_dict())
    finally:
        db.close(grace_period=5.0)


if __name__ == "__main__":
    main()
