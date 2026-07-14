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

        print()
        await poison_message_with_attempt_counting(db)
    finally:
        await db.close()


async def poison_message_with_attempt_counting(db: AsyncDatabase) -> None:
    """A message whose ``work()`` always fails ("poison message" — e.g. a malformed payload)
    must eventually stop retrying and go to a dead-letter queue instead of looping forever.

    ``ack_after_commit``'s own ``dead_letter`` callback only fires for a non-retryable
    :class:`~dbkit.errors.DatabaseError` raised by ``work()`` — an *application*-level failure
    (a bad payload, a business-rule violation) isn't a ``DatabaseError`` at all, so it never
    reaches that callback. "Poison after N attempts" is a separate, application-level policy
    layered on top, tracked with a durable attempts counter — durable because the inbox claim
    itself rolls back along with the rest of the transaction when ``work()`` fails, so it
    cannot be used to count failed attempts (only successful, committed ones).
    """
    MAX_ATTEMPTS = 3
    CONSUMER, MESSAGE_ID = "poison_processor", "poison-1"

    await db.execute(
        sql(
            "CREATE TABLE IF NOT EXISTS dbkit_example_attempts ("
            "consumer_name text NOT NULL, message_id text NOT NULL, attempts int NOT NULL, "
            "PRIMARY KEY (consumer_name, message_id))"
        ),
        target=TARGET,
    )
    await db.execute(
        sql("DELETE FROM dbkit_example_attempts WHERE consumer_name = :c"),
        {"c": CONSUMER},
        target=TARGET,
    )
    await db.execute(
        sql("DELETE FROM consumed_messages WHERE consumer_name = :c"),
        {"c": CONSUMER},
        target=TARGET,
    )

    async def note_attempt() -> int:
        # A standalone, immediately-committed transaction — it must durably persist even when
        # the transaction around work() below rolls back. `db.fetch_value()` (no explicit
        # transaction) would NOT do that here: it's dbkit's read path (no commit at all), so a
        # write-with-RETURNING through it is silently rolled back on connection close — an
        # explicit `db.transaction()` is required to both write and read RETURNING back.
        async with db.transaction(target=TARGET) as tx:
            return await tx.fetch_value(
                sql(
                    "INSERT INTO dbkit_example_attempts (consumer_name, message_id, attempts) "
                    "VALUES (:c, :m, 1) ON CONFLICT (consumer_name, message_id) "
                    "DO UPDATE SET attempts = dbkit_example_attempts.attempts + 1 "
                    "RETURNING attempts"
                ),
                {"c": CONSUMER, "m": MESSAGE_ID},
            )

    async def always_fails(tx: object) -> None:
        raise RuntimeError("simulated permanent failure (e.g. a malformed payload)")

    async def ack() -> None:
        print("  -> broker message acked")

    dead_lettered = False
    for delivery in range(1, MAX_ATTEMPTS + 2):
        attempts = await note_attempt()
        print(f"poison delivery {delivery}: attempt #{attempts} for {MESSAGE_ID!r}")
        if attempts > MAX_ATTEMPTS:
            print(f"  -> exceeded {MAX_ATTEMPTS} attempts, routing to dead-letter queue")
            dead_lettered = True
            await ack()  # ack so the broker stops redelivering; the DLQ owns it now
            break
        try:
            await ack_after_commit(
                db,
                consumer=CONSUMER,
                message_id=MESSAGE_ID,
                target=TARGET,
                work=always_fails,
                ack=ack,
            )
        except Exception as exc:
            print(f"  -> attempt failed (not acked, broker will redeliver): {exc}")

    print(f"dead-lettered after {MAX_ATTEMPTS} attempts: {dead_lettered}")


if __name__ == "__main__":
    asyncio.run(main())
