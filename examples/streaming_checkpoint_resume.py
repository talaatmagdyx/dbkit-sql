"""Checkpoint-and-resume streaming over a keyed table (§20).

``db.stream()`` deliberately has no built-in resilience — the docstring is explicit that "a
partially consumed stream cannot be transparently restarted." That's the correct trade-off (a
retry can't safely re-run arbitrary already-yielded side effects for the caller), but it means
any resume story is entirely the application's responsibility. This example shows the standard
pattern: a keyset predicate (``WHERE id > :last_id ORDER BY id``) plus a durable checkpoint,
persisted in its own committed transaction so it survives a crash mid-stream.

Checkpointing every N rows instead of every row is a deliberate throughput/duplication
trade-off: it means up to ``N - 1`` already-processed rows are legitimately reprocessed after a
crash (the gap between the last saved checkpoint and the crash point), not a bug in the pattern.
Combine this with per-row idempotent work (§14), or checkpoint every row if you need tighter
at-most-once-reprocessing bounds at the cost of a write per row. Run:

    DBKIT_DSN=postgresql+psycopg://localhost/postgres python examples/streaming_checkpoint_resume.py
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass

from dbkit import AsyncDatabase, DatabaseTarget, sql

DSN = os.environ.get("DBKIT_DSN", "postgresql+psycopg://localhost/postgres")
TARGET = DatabaseTarget(database="app", role="write")
CONSUMER = "checkpoint_resume_example"
ROW_COUNT = 10_000
CHECKPOINT_EVERY = 1_000
CRASH_AFTER_ROW = 4_500  # simulates a process death partway through the first attempt


@dataclass
class Row:
    id: int


async def save_checkpoint(db: AsyncDatabase, *, last_id: int) -> None:
    """A standalone, immediately-committed write — durable independent of the stream's own
    outcome, so it survives even if the stream that produced it later crashes."""
    await db.execute(
        sql(
            "INSERT INTO dbkit_example_stream_checkpoints (consumer_name, last_id) "
            "VALUES (:c, :last_id) ON CONFLICT (consumer_name) "
            "DO UPDATE SET last_id = :last_id"
        ),
        {"c": CONSUMER, "last_id": last_id},
        target=TARGET,
    )


async def load_checkpoint(db: AsyncDatabase) -> int:
    row = await db.fetch_optional(
        sql(
            "SELECT last_id FROM dbkit_example_stream_checkpoints WHERE consumer_name = :c",
        ),
        {"c": CONSUMER},
        target=TARGET,
    )
    return int(row.last_id) if row is not None else 0


async def run_one_attempt(db: AsyncDatabase, *, simulate_crash: bool) -> int:
    """Streams rows with ``id > last_id``, checkpointing periodically. If ``simulate_crash`` is
    set, raises partway through — mimicking a process dying mid-stream — to prove the *next*
    attempt resumes from the last saved checkpoint rather than reprocessing everything."""
    last_id = await load_checkpoint(db)
    processed = 0
    async with await db.stream(
        sql("SELECT id FROM dbkit_example_stream_source WHERE id > :last_id ORDER BY id"),
        {"last_id": last_id},
        target=DatabaseTarget(database="app", role="read"),
        batch_size=500,
        map_to=Row,
    ) as rows:
        async for row in rows:
            processed += 1
            last_id = row.id
            if simulate_crash and last_id >= CRASH_AFTER_ROW:
                raise RuntimeError(f"simulated crash after processing id={last_id}")
            if processed % CHECKPOINT_EVERY == 0:
                await save_checkpoint(db, last_id=last_id)
    # Final checkpoint for whatever was processed since the last periodic save.
    await save_checkpoint(db, last_id=last_id)
    return processed


async def main() -> None:
    db = AsyncDatabase.from_config({"databases": {"app": {"primary": {"url": DSN}}}})
    await db.start()
    try:
        await db.execute(
            sql(
                "CREATE TABLE IF NOT EXISTS dbkit_example_stream_checkpoints "
                "(consumer_name text PRIMARY KEY, last_id bigint NOT NULL)"
            ),
            target=TARGET,
        )
        await db.execute(sql("DROP TABLE IF EXISTS dbkit_example_stream_source"), target=TARGET)
        await db.execute(
            sql("CREATE TABLE dbkit_example_stream_source (id bigint PRIMARY KEY)"),
            target=TARGET,
        )
        await db.execute(
            sql("INSERT INTO dbkit_example_stream_source SELECT generate_series(1, :n)"),
            {"n": ROW_COUNT},
            target=TARGET,
        )
        await db.execute(
            sql("DELETE FROM dbkit_example_stream_checkpoints WHERE consumer_name = :c"),
            {"c": CONSUMER},
            target=TARGET,
        )

        print(f"attempt 1: streaming, will simulate a crash at id={CRASH_AFTER_ROW}")
        try:
            await run_one_attempt(db, simulate_crash=True)
        except RuntimeError as exc:
            checkpoint_after_crash = await load_checkpoint(db)
            print(f"  -> crashed: {exc}")
            print(f"  -> last durable checkpoint: id={checkpoint_after_crash} (survives the crash)")

        print("attempt 2: resuming from the last checkpoint, no simulated crash this time")
        processed = await run_one_attempt(db, simulate_crash=False)
        final_checkpoint = await load_checkpoint(db)
        reprocessed = CRASH_AFTER_ROW - checkpoint_after_crash
        print(
            f"  -> processed {processed:,} rows this attempt "
            f"(resumed from id={checkpoint_after_crash})"
        )
        print(f"  -> final checkpoint: id={final_checkpoint} (expect {ROW_COUNT})")
        assert final_checkpoint == ROW_COUNT, "resume must eventually reach the end"
        print(
            f"resume picked up from the last checkpoint, not from scratch — "
            f"{reprocessed} rows between the checkpoint and the crash point were legitimately "
            f"reprocessed (checkpoint granularity is {CHECKPOINT_EVERY} rows); every row from "
            f"1..{ROW_COUNT} was covered at least once"
        )
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
