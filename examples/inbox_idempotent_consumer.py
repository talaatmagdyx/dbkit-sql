"""Exactly-once message processing over an at-least-once delivery (§28). Run:

    DBKIT_DSN=postgresql+psycopg://localhost/postgres python examples/inbox_idempotent_consumer.py

Simulates a message consumer (e.g. RabbitMQ) that may redeliver the same message. The inbox
table and the business write commit in the same transaction, so redelivery is a safe no-op.
"""

from __future__ import annotations

import asyncio
import os

from dbkit import AsyncDatabase, DatabaseTarget, sql
from dbkit.integrations import ack_after_commit, inbox_ddl

DSN = os.environ.get("DBKIT_DSN", "postgresql+psycopg://localhost/postgres")
TARGET = DatabaseTarget(database="app", role="write")


async def main() -> None:
    db = AsyncDatabase.from_config({"databases": {"app": {"primary": {"url": DSN}}}})
    await db.start()
    try:
        await db.execute(sql(inbox_ddl()), target=TARGET)
        await db.execute(sql("DROP TABLE IF EXISTS dbkit_example_orders"), target=TARGET)
        await db.execute(
            sql("CREATE TABLE dbkit_example_orders (order_id text PRIMARY KEY, amount int)"),
            target=TARGET,
        )
        await db.execute(
            sql("DELETE FROM consumed_messages WHERE consumer_name = 'order_processor'"),
            target=TARGET,
        )

        async def process_order(tx: object) -> None:
            # The "business logic" a message handler would run.
            await tx.execute(  # type: ignore[attr-defined]
                sql("INSERT INTO dbkit_example_orders (order_id, amount) VALUES (:oid, :amt)"),
                {"oid": "order-42", "amt": 1999},
            )

        acked = 0

        async def ack() -> None:
            nonlocal acked
            acked += 1
            print(f"  -> broker message acked (ack #{acked})")

        # Simulate the broker redelivering "order-42" three times (network blips, requeues...).
        for delivery in range(1, 4):
            print(f"delivery {delivery}: processing message 'order-42'")
            processed = await ack_after_commit(
                db,
                consumer="order_processor",
                message_id="order-42",
                target=TARGET,
                work=process_order,
                ack=ack,
            )
            print(f"  -> processed={processed}")

        count = await db.fetch_value(
            sql("SELECT count(*) FROM dbkit_example_orders"), target=TARGET
        )
        total_amount = await db.fetch_value(
            sql("SELECT sum(amount) FROM dbkit_example_orders"), target=TARGET
        )
        print(f"\nfinal state: {count} order row (expect 1, not 3), amount={total_amount}")
        print(f"broker saw {acked} acks (expect 3) — redelivery was safe")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
