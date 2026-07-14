"""Streaming large result sets with bounded memory (§20). Run:

DBKIT_DSN=postgresql+psycopg://localhost/postgres python examples/streaming.py
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass

from dbkit import AsyncDatabase, DatabaseTarget, sql

DSN = os.environ.get("DBKIT_DSN", "postgresql+psycopg://localhost/postgres")
TARGET = DatabaseTarget(database="app", role="read")


@dataclass
class Row:
    i: int


async def main() -> None:
    db = AsyncDatabase.from_config({"databases": {"app": {"primary": {"url": DSN}}}})
    await db.start()
    try:
        # Stream 100,000 rows — never fetch_all() an unbounded export query (§20).
        total = 0
        checksum = 0
        async with await db.stream(
            sql("SELECT i FROM generate_series(1, 100000) AS i"),
            target=TARGET,
            batch_size=2000,
            map_to=Row,
        ) as rows:
            async for row in rows:
                total += 1
                checksum += row.i

        print(f"streamed {total:,} rows (mapped to Row dataclass), checksum={checksum:,}")
        # No leaked connection: the stream released it on context exit.
        print(f"pool checked_out after streaming: {db.pool_status()[0].checked_out} (expect 0)")

        # A max_duration guard aborts a runaway stream instead of hanging forever.
        try:
            async with await db.stream(
                sql("SELECT i, pg_sleep(0.05) FROM generate_series(1, 1000) AS i"),
                target=TARGET,
                batch_size=1,
                max_duration=0.2,
            ) as rows:
                async for _ in rows:
                    pass
        except Exception as e:
            print(f"max_duration guard fired: {type(e).__name__}")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
