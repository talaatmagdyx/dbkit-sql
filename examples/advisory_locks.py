"""Transaction-scoped advisory locks: serialize work on a logical key without locking rows (§11.7).

    DBKIT_DSN=postgresql+psycopg://localhost/postgres python examples/advisory_locks.py

``pg_advisory_xact_lock`` is held until the transaction ends and released automatically — the tool
for serializing a read-modify-write on a logical entity (an order, an engagement) across workers.
``try_advisory_xact_lock`` is the non-blocking variant: skip work another worker already holds.
"""

from __future__ import annotations

import asyncio
import os

from dbkit import AsyncDatabase, DatabaseTarget

DSN = os.environ.get("DBKIT_DSN", "postgresql+psycopg://localhost/postgres")
TARGET = DatabaseTarget(database="app", role="write")


async def main() -> None:
    db = AsyncDatabase.from_config({"databases": {"app": {"primary": {"url": DSN}}}})
    await db.start()
    try:
        # Hold the lock in one transaction; a concurrent transaction can't take the same key.
        async with db.transaction(target=TARGET) as tx1:
            await tx1.advisory_xact_lock("engagement:42")
            async with db.transaction(target=TARGET) as tx2:
                got_same = await tx2.try_advisory_xact_lock("engagement:42")
                got_other = await tx2.try_advisory_xact_lock("engagement:99")
                print(f"same key while held: {got_same} (expect False)")
                print(f"different key:       {got_other} (expect True)")
        # tx1 committed -> the key is free again (xact lock auto-released).
        async with db.transaction(target=TARGET) as tx3:
            freed = await tx3.try_advisory_xact_lock("engagement:42")
            print(f"same key after release: {freed} (expect True)")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
