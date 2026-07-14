"""Async quickstart (§35). Run against a local PostgreSQL:

DBKIT_DSN=postgresql+psycopg://localhost/postgres python examples/quickstart_async.py
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass

from dbkit import AsyncDatabase, DatabaseTarget, Query, sql

DSN = os.environ.get("DBKIT_DSN", "postgresql+psycopg://localhost/postgres")
TARGET = DatabaseTarget(database="app", role="write")

CREATE_SLA_FN = sql(
    "CREATE OR REPLACE FUNCTION dbkit_calc_sla(priority int) "
    "RETURNS int LANGUAGE sql AS 'SELECT 24 / priority'"
)
CREATE_MESSAGES = sql(
    "CREATE TABLE IF NOT EXISTS dbkit_messages ( id integer PRIMARY KEY, body text NOT NULL)"
)
INSERT_MESSAGE = Query(
    name="messages.insert",
    statement=sql("INSERT INTO dbkit_messages (id, body) VALUES (:id, :body)"),
    operation="write",
)
GET_MESSAGE = Query(
    name="messages.get_by_id",
    statement=sql("SELECT id, body FROM dbkit_messages WHERE id = :id"),
    operation="read",
    idempotent=True,
    timeout=1.0,
)


@dataclass
class Message:
    id: int
    body: str


async def main() -> None:
    db = AsyncDatabase.from_config({"databases": {"app": {"primary": {"url": DSN}}}})
    await db.start()
    await db.require_ready()
    try:
        await db.execute(CREATE_MESSAGES, target=TARGET)
        await db.execute(CREATE_SLA_FN, target=TARGET)
        await db.execute(sql("TRUNCATE dbkit_messages"), target=TARGET)  # safe to re-run

        # Explicit transaction with two writes.
        async with db.transaction(target=TARGET) as tx:
            await tx.execute(INSERT_MESSAGE, {"id": 1, "body": "hello"})
            await tx.execute(INSERT_MESSAGE, {"id": 2, "body": "world"})

        # Mapped read.
        msg = await db.fetch_optional(GET_MESSAGE, {"id": 1}, target=TARGET, map_to=Message)
        print("fetched:", msg)

        # Custom PostgreSQL function via text().
        sla = await db.fetch_value(
            sql("SELECT dbkit_calc_sla(:priority)"), {"priority": 3}, target=TARGET
        )
        print("sla hours:", sla)

        for snap in db.pool_status():
            print("pool:", snap.to_dict())
    finally:
        await db.close(grace_period=5.0)


if __name__ == "__main__":
    asyncio.run(main())
