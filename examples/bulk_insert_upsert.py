"""Bulk insert/upsert with adaptive batching and failure modes (§19). Run:

DBKIT_DSN=postgresql+psycopg://localhost/postgres python examples/bulk_insert_upsert.py
"""

from __future__ import annotations

import asyncio
import os

from sqlalchemy import Column, Integer, MetaData, Table, Text

from dbkit import AsyncDatabase, DatabaseTarget, sql
from dbkit.errors import DatabaseUniqueViolationError

DSN = os.environ.get("DBKIT_DSN", "postgresql+psycopg://localhost/postgres")
TARGET = DatabaseTarget(database="app", role="write")

metadata = MetaData()
users = Table(
    "dbkit_bulk_users",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("email", Text),
)


async def main() -> None:
    db = AsyncDatabase.from_config({"databases": {"app": {"primary": {"url": DSN}}}})
    await db.start()
    try:
        await db.execute(
            sql("CREATE TABLE IF NOT EXISTS dbkit_bulk_users (id int PRIMARY KEY, email text)"),
            target=TARGET,
        )
        await db.execute(sql("TRUNCATE dbkit_bulk_users"), target=TARGET)

        # 1. insert_many: adaptive batching splits 5000 rows into safe-sized batches
        #    automatically (bounded by rows and PostgreSQL's bind-parameter ceiling).
        rows = [{"id": i, "email": f"user{i}@example.com"} for i in range(5000)]
        result = await db.insert_many(users, rows, target=TARGET, batch_size=1000)
        print(f"insert_many: wrote {result.row_count} rows in {result.duration_ms:.1f}ms")

        # 2. upsert_many: ON CONFLICT DO UPDATE for overlapping ids.
        updates = [{"id": i, "email": f"updated{i}@example.com"} for i in range(4900, 5100)]
        result2 = await db.upsert_many(
            users,
            updates,
            target=TARGET,
            conflict_index_elements=["id"],
            update_columns=["email"],
        )
        print(f"upsert_many: touched {result2.row_count} rows (100 updated existing + 100 new)")
        sample = await db.fetch_value(
            sql("SELECT email FROM dbkit_bulk_users WHERE id = 4950"), target=TARGET
        )
        print(f"id=4950 email is now: {sample}")

        total = await db.fetch_value(sql("SELECT count(*) FROM dbkit_bulk_users"), target=TARGET)
        print(f"total rows: {total} (expect 5100)")

        # 3. atomic mode: one bad row rolls back the WHOLE batch (all-or-nothing).
        await db.execute(sql("TRUNCATE dbkit_bulk_users"), target=TARGET)
        bad_rows = [{"id": i, "email": "x"} for i in range(50)] + [{"id": 10, "email": "dup"}]
        try:
            await db.insert_many(users, bad_rows, target=TARGET, mode="atomic")
        except DatabaseUniqueViolationError:
            count = await db.fetch_value(
                sql("SELECT count(*) FROM dbkit_bulk_users"), target=TARGET
            )
            print(f"atomic mode: rolled back entirely, {count} rows persisted (expect 0)")

        # 4. split_on_failure: isolates just the bad row(s), keeps the rest.
        result3 = await db.insert_many(
            users, bad_rows, target=TARGET, mode="split_on_failure", batch_size=200
        )
        count = await db.fetch_value(sql("SELECT count(*) FROM dbkit_bulk_users"), target=TARGET)
        print(f"split_on_failure: wrote {result3.row_count} rows, {count} persisted (expect 50)")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
