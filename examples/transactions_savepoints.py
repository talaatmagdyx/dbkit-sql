"""Explicit transactions, savepoints, and rollback semantics (§11.3-11.4). Run:

DBKIT_DSN=postgresql+psycopg://localhost/postgres python examples/transactions_savepoints.py
"""

from __future__ import annotations

import asyncio
import os

from dbkit import AsyncDatabase, DatabaseTarget, Query, sql

DSN = os.environ.get("DBKIT_DSN", "postgresql+psycopg://localhost/postgres")
TARGET = DatabaseTarget(database="app", role="write")

CREATE = sql("CREATE TABLE IF NOT EXISTS dbkit_tx_demo (id int PRIMARY KEY, note text)")
INSERT = Query(
    name="tx_demo.insert",
    statement=sql("INSERT INTO dbkit_tx_demo (id, note) VALUES (:id, :note)"),
    operation="write",
)


async def main() -> None:
    db = AsyncDatabase.from_config({"databases": {"app": {"primary": {"url": DSN}}}})
    await db.start()
    try:
        await db.execute(CREATE, target=TARGET)
        await db.execute(sql("TRUNCATE dbkit_tx_demo"), target=TARGET)

        # 1. Explicit transaction: both writes commit together.
        async with db.transaction(target=TARGET) as tx:
            await tx.execute(INSERT, {"id": 1, "note": "first"})
            await tx.execute(INSERT, {"id": 2, "note": "second"})
        count = await db.fetch_value(sql("SELECT count(*) FROM dbkit_tx_demo"), target=TARGET)
        print(f"after commit: {count} rows (expect 2)")

        # 2. An exception inside the block rolls back everything.
        try:
            async with db.transaction(target=TARGET) as tx:
                await tx.execute(INSERT, {"id": 3, "note": "will vanish"})
                raise RuntimeError("simulated business failure")
        except RuntimeError:
            pass
        count = await db.fetch_value(sql("SELECT count(*) FROM dbkit_tx_demo"), target=TARGET)
        print(f"after rollback: {count} rows (expect 2, id=3 never persisted)")

        # 3. Savepoints: a nested failure rolls back only the nested unit of work.
        async with db.transaction(target=TARGET) as tx:
            await tx.execute(INSERT, {"id": 4, "note": "kept"})
            try:
                async with tx.savepoint():
                    await tx.execute(INSERT, {"id": 5, "note": "rolled back"})
                    raise RuntimeError("nested failure")
            except RuntimeError:
                pass
            await tx.execute(INSERT, {"id": 6, "note": "kept"})
        ids = await db.fetch_values(sql("SELECT id FROM dbkit_tx_demo ORDER BY id"), target=TARGET)
        print(f"after savepoint rollback: ids={ids} (expect [1, 2, 4, 6] — 5 is missing)")

        # 4. Isolation level + read-only.
        async with db.transaction(target=TARGET, isolation="serializable", read_only=True) as tx:
            total = await tx.fetch_value(sql("SELECT count(*) FROM dbkit_tx_demo"))
            print(f"serializable read-only transaction saw {total} rows")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
