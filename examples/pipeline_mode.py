"""psycopg pipeline mode — batch dependent statements into one round trip (§7.3). Run:

    DBKIT_DSN=postgresql+psycopg://localhost/postgres python examples/pipeline_mode.py

Pipeline mode's benefit is amortizing network round-trip *latency* — it won't show a speedup
against a local database (round trips there are already near-zero), but on a real network hop
it lets the client send several statements without waiting for each response before the next.
"""

from __future__ import annotations

import asyncio
import os

from dbkit import AsyncDatabase, DatabaseTarget, sql

DSN = os.environ.get("DBKIT_DSN", "postgresql+psycopg://localhost/postgres")
TARGET = DatabaseTarget(database="app", role="write")


async def main() -> None:
    db = AsyncDatabase.from_config({"databases": {"app": {"primary": {"url": DSN}}}})
    await db.start()
    try:
        await db.execute(
            sql("CREATE TABLE IF NOT EXISTS dbkit_pipe_orders (id int PRIMARY KEY, note text)"),
            target=TARGET,
        )
        await db.execute(
            sql(
                "CREATE TABLE IF NOT EXISTS dbkit_pipe_inbox "
                "(order_id int PRIMARY KEY, seen_at timestamptz DEFAULT now())"
            ),
            target=TARGET,
        )
        await db.execute(sql("TRUNCATE dbkit_pipe_orders, dbkit_pipe_inbox"), target=TARGET)

        # A business write plus its inbox record — two dependent statements per order,
        # pipelined so the driver doesn't wait for each response before sending the next.
        async with db.transaction(target=TARGET) as tx, tx.pipeline():
            for i in range(100):
                await tx.execute(
                    sql("INSERT INTO dbkit_pipe_orders (id, note) VALUES (:id, :note)"),
                    {"id": i, "note": f"order-{i}"},
                )
                await tx.execute(
                    sql("INSERT INTO dbkit_pipe_inbox (order_id) VALUES (:id)"), {"id": i}
                )

        orders = await db.fetch_value(sql("SELECT count(*) FROM dbkit_pipe_orders"), target=TARGET)
        inbox = await db.fetch_value(sql("SELECT count(*) FROM dbkit_pipe_inbox"), target=TARGET)
        print(f"pipelined: {orders} orders, {inbox} inbox records (expect 100 each)")

        # A failure inside the pipelined block still rolls back the whole transaction.
        try:
            async with db.transaction(target=TARGET) as tx, tx.pipeline():
                await tx.execute(
                    sql("INSERT INTO dbkit_pipe_orders (id, note) VALUES (:id, :note)"),
                    {"id": 9999, "note": "will vanish"},
                )
                raise RuntimeError("simulated failure")
        except RuntimeError:
            pass
        exists = await db.fetch_optional(
            sql("SELECT 1 FROM dbkit_pipe_orders WHERE id = 9999"), target=TARGET
        )
        print(f"after rollback: row exists = {exists is not None} (expect False)")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
