"""Transactional outbox: enqueue an event atomically with a business write, then relay it (§28.4).

    DBKIT_DSN=postgresql+psycopg://localhost/postgres python examples/transactional_outbox.py

The event row and the business write commit together, so the event exists iff the change
persisted. A separate relay reads unsent rows and publishes them (here, just prints), marking each
sent. Delivery is at-least-once — pair with the consumer-side inbox for effectively-once.
"""

from __future__ import annotations

import asyncio
import os

from dbkit import AsyncDatabase, DatabaseTarget, sql
from dbkit.integrations import drain, enqueue, outbox_ddl

DSN = os.environ.get("DBKIT_DSN", "postgresql+psycopg://localhost/postgres")
TARGET = DatabaseTarget(database="app", role="write")
OUTBOX = "dbkit_example_outbox"


async def main() -> None:
    db = AsyncDatabase.from_config({"databases": {"app": {"primary": {"url": DSN}}}})
    await db.start()
    try:
        for stmt in outbox_ddl(OUTBOX).split(";"):
            if stmt.strip():
                await db.execute(sql(stmt), target=TARGET)
        await db.execute(sql(f"TRUNCATE {OUTBOX}"), target=TARGET)
        await db.execute(
            sql("CREATE TABLE IF NOT EXISTS dbkit_example_orders (order_id text PRIMARY KEY)"),
            target=TARGET,
        )
        await db.execute(sql("TRUNCATE dbkit_example_orders"), target=TARGET)

        # 1. Business write + event, one atomic transaction.
        async with db.transaction(target=TARGET) as tx:
            await tx.execute(
                sql("INSERT INTO dbkit_example_orders (order_id) VALUES (:o)"), {"o": "order-42"}
            )
            await enqueue(
                tx, topic="order.completed", payload={"order_id": "order-42"}, table=OUTBOX
            )

        # 2. A failed business write takes its event down with it (atomicity).
        try:
            async with db.transaction(target=TARGET) as tx:
                await enqueue(
                    tx, topic="order.completed", payload={"order_id": "ghost"}, table=OUTBOX
                )
                raise RuntimeError("business failure")
        except RuntimeError:
            pass
        pending = await db.fetch_value(sql(f"SELECT count(*) FROM {OUTBOX}"), target=TARGET)
        print(f"pending events after 1 commit + 1 rollback: {pending} (expect 1)")

        # 3. Relay: publish unsent rows, mark them sent.
        async def publish(row: dict) -> None:
            print(f"  -> publish topic={row['topic']} payload={row['payload']}")

        relayed = await drain(db, target=TARGET, publish=publish, table=OUTBOX)
        print(f"relayed: {relayed} (expect 1)")
        again = await drain(db, target=TARGET, publish=publish, table=OUTBOX)
        print(f"second drain: {again} (expect 0)")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
