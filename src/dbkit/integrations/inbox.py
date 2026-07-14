"""Inbox / idempotent-consume helpers for message-driven writes (§28).

These make RabbitMQ (or any broker) message processing safe against redelivery. The inbox row
and the business writes commit in the **same** transaction, so a message is processed
exactly-once even though delivery is at-least-once. Ack the broker message only after the
commit succeeds; on a commit-unknown outcome, do NOT ack — require idempotent replay (§15, §28).

Async helpers targeting :class:`AsyncDatabase`. The DDL is frontend-agnostic.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from .._core.query import sql
from .._core.routing import DatabaseTarget
from ..errors import DatabaseCommitUnknownError, DatabaseError

DEFAULT_INBOX_TABLE = "consumed_messages"


def inbox_ddl(table: str = DEFAULT_INBOX_TABLE) -> str:
    """DDL for the inbox table (§28.3). Time-partition it in production for cheap pruning —
    see :func:`partitioned_inbox_ddl`/:func:`inbox_month_partition_ddl` for that variant."""
    return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            consumer_name TEXT NOT NULL,
            message_id    TEXT NOT NULL,
            processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (consumer_name, message_id)
        )
    """


def partitioned_inbox_ddl(table: str = DEFAULT_INBOX_TABLE) -> str:
    """DDL for a **partitioned** inbox table, range-partitioned by ``processed_at`` (§28.3).

    A single unbounded ``consumed_messages`` table's dedup lookup gets slower and its storage
    grows without bound as message volume accumulates. Range-partitioning by month makes
    pruning old data a cheap ``DROP TABLE`` on a partition instead of a slow ``DELETE``. The
    partition key must be part of the primary key for declarative partitioning, so this variant
    keys on ``(consumer_name, message_id, processed_at)`` instead of just the first two.

    Partitions themselves aren't created automatically — call
    :func:`inbox_month_partition_ddl` for each month you need (typically from a scheduled job
    that creates the next 1-2 months ahead of time; `pg_partman` or a cron-triggered `dbkit`
    script both work). Older partitions can simply be dropped once no longer needed for
    dedup/audit purposes — a message's redelivery window is bounded by the broker's own
    retention, so a partition older than that window is always safe to drop.
    """
    return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            consumer_name TEXT NOT NULL,
            message_id    TEXT NOT NULL,
            processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (consumer_name, message_id, processed_at)
        ) PARTITION BY RANGE (processed_at)
    """


def inbox_month_partition_ddl(year: int, month: int, *, table: str = DEFAULT_INBOX_TABLE) -> str:
    """DDL for one calendar-month partition of :func:`partitioned_inbox_ddl`'s table.

    ``year``/``month`` are passed explicitly (not computed from the current date) so this stays
    a pure function — call it with whatever month you actually need created, e.g. from a
    scheduled job that creates the next month a few days before it starts.
    """
    start = f"{year:04d}-{month:02d}-01"
    next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
    end = f"{next_year:04d}-{next_month:02d}-01"
    partition_name = f"{table}_{year:04d}_{month:02d}"
    return f"""
        CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF {table}
        FOR VALUES FROM ('{start}') TO ('{end}')
    """


async def _claim(tx: Any, consumer: str, message_id: str, table: str) -> bool:
    """Insert the inbox row; return True if this is the first time we've seen the message."""
    row = await tx.fetch_optional(
        sql(
            f"INSERT INTO {table} (consumer_name, message_id) VALUES (:c, :m) "
            "ON CONFLICT (consumer_name, message_id) DO NOTHING RETURNING 1"
        ),
        {"c": consumer, "m": message_id},
    )
    return row is not None


@contextlib.asynccontextmanager
async def process_once(
    db: Any,
    *,
    consumer: str,
    message_id: str,
    target: DatabaseTarget,
    inbox_table: str = DEFAULT_INBOX_TABLE,
) -> AsyncIterator[tuple[Any, bool]]:
    """Open a transaction, claim the message in the inbox, and yield ``(tx, first_time)``.

    Do the business writes on ``tx`` only when ``first_time`` is True; a duplicate delivery
    still commits (a harmless no-op) so the broker message can be acked::

        async with process_once(db, consumer="c", message_id=mid, target=t) as (tx, first):
            if first:
                await tx.execute(INSERT_ORDER, params)
    """
    async with db.transaction(target=target) as tx:
        first = await _claim(tx, consumer, message_id, inbox_table)
        yield tx, first


async def ack_after_commit(
    db: Any,
    *,
    consumer: str,
    message_id: str,
    target: DatabaseTarget,
    work: Callable[[Any], Awaitable[None]],
    ack: Callable[[], Awaitable[None]],
    dead_letter: Callable[[DatabaseError], Awaitable[None]] | None = None,
    retry: Callable[[DatabaseError], Awaitable[None]] | None = None,
    inbox_table: str = DEFAULT_INBOX_TABLE,
) -> bool:
    """Run ``work`` idempotently, then ack — the full §28 consume flow.

    Returns True if the message was processed (or was a duplicate) and acked. Behavior on
    failure:

    * commit-unknown -> re-raised; the message is **not** acked (idempotent replay will dedupe).
    * retryable error -> ``retry`` callback if given, else re-raised (nack/requeue).
    * permanent error -> ``dead_letter`` callback if given, else re-raised (route to DLQ).
    """
    try:
        async with process_once(
            db, consumer=consumer, message_id=message_id, target=target, inbox_table=inbox_table
        ) as (tx, first):
            if first:
                await work(tx)
    except DatabaseCommitUnknownError:
        raise  # never ack on unknown commit outcome
    except DatabaseError as exc:
        if exc.retryable and retry is not None:
            await retry(exc)
            return False
        if not exc.retryable and dead_letter is not None:
            await dead_letter(exc)
            return False
        raise
    await ack()
    return True
