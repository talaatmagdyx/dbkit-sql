"""Transactional-outbox helpers for reliable event publishing (§28).

The classic outbox pattern: instead of writing to the database and then publishing to a broker as
two independent steps (which can leave them inconsistent if the process dies between them), you
INSERT the event into an outbox table **in the same transaction as the business write**. The row
and the write commit atomically — the event exists if and only if the business change persisted. A
separate **relay** then reads unsent rows and publishes them to the broker, marking each sent.

Delivery is **at-least-once** (a crash after the broker accepts a message but before the relay
marks it sent replays it) — pair this with the consumer-side inbox
(:mod:`dbkit.integrations.inbox`) for end-to-end effectively-once. This is a **single-shard**
outbox: the outbox table lives on the same
database as the business write. dbkit does not coordinate cross-shard/distributed transactions —
for multi-shard fan-out, run one outbox per shard.

Async helpers targeting :class:`~dbkit.AsyncDatabase`; the DDL is frontend-agnostic. It is
schema-agnostic: you pass the table name, and the payload is opaque JSON — no dbkit- or
product-specific columns.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from .._core.query import sql
from .._core.routing import DatabaseTarget

DEFAULT_OUTBOX_TABLE = "outbox_messages"


def outbox_ddl(table: str = DEFAULT_OUTBOX_TABLE) -> str:
    """DDL for the outbox table (§28.4).

    Columns: ``id`` (monotonic ``bigserial`` — relay order + cursor), ``topic`` (routing key),
    ``payload`` (opaque ``jsonb`` event body), ``created_at``, and ``sent_at`` (``NULL`` until the
    relay publishes it). A partial index on the unsent rows keeps the relay's "next batch" scan
    cheap regardless of how many already-sent rows remain. Time-partition it in production for cheap
    pruning of sent rows — see :func:`partitioned_outbox_ddl`/:func:`outbox_month_partition_ddl`.
    """
    return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            topic      TEXT NOT NULL,
            payload    JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            sent_at    TIMESTAMPTZ
        );
        CREATE INDEX IF NOT EXISTS {table}_unsent_idx ON {table} (id) WHERE sent_at IS NULL
    """


def partitioned_outbox_ddl(table: str = DEFAULT_OUTBOX_TABLE) -> str:
    """DDL for a **partitioned** outbox table, range-partitioned by ``created_at`` (§28.4).

    An unbounded outbox grows without limit as events accumulate; range-partitioning by month makes
    pruning old (already-sent) data a cheap ``DROP TABLE`` on a partition instead of a slow
    ``DELETE``. The partition key must be part of the primary key for declarative partitioning, so
    this variant keys on ``(id, created_at)``. Create each month's partition with
    :func:`outbox_month_partition_ddl` (typically from a scheduled job a month ahead); drop
    partitions once every row in them has been sent and is past any audit window.
    """
    return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id         BIGINT GENERATED ALWAYS AS IDENTITY,
            topic      TEXT NOT NULL,
            payload    JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            sent_at    TIMESTAMPTZ,
            PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at)
    """


def outbox_month_partition_ddl(year: int, month: int, *, table: str = DEFAULT_OUTBOX_TABLE) -> str:
    """DDL for one calendar-month partition of :func:`partitioned_outbox_ddl`'s table.

    ``year``/``month`` are passed explicitly (not computed from the current date) so this stays a
    pure function — call it with whatever month you need created, e.g. from a scheduled job that
    creates the next month a few days before it starts.
    """
    start = f"{year:04d}-{month:02d}-01"
    next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
    end = f"{next_year:04d}-{next_month:02d}-01"
    partition_name = f"{table}_{year:04d}_{month:02d}"
    return f"""
        CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF {table}
        FOR VALUES FROM ('{start}') TO ('{end}')
    """


async def enqueue(
    tx: Any,
    *,
    topic: str,
    payload: Mapping[str, Any],
    table: str = DEFAULT_OUTBOX_TABLE,
) -> int:
    """Insert one event into the outbox **on the caller's transaction** ``tx`` and return its id.

    Call this inside a ``db.transaction(...)`` block alongside the business writes so the event and
    the change commit atomically::

        async with db.transaction(target=t) as tx:
            await tx.execute(UPDATE_ORDER, params)
            await enqueue(tx, topic="order.completed", payload={"order_id": oid})

    ``payload`` is serialized to ``jsonb`` and bound as a parameter (never interpolated).
    """
    new_id = await tx.fetch_value(
        sql(
            f"INSERT INTO {table} (topic, payload) "
            "VALUES (:topic, CAST(:payload AS jsonb)) RETURNING id"
        ),
        {"topic": topic, "payload": json.dumps(dict(payload))},
    )
    return int(new_id)


async def drain(
    db: Any,
    *,
    target: DatabaseTarget,
    publish: Callable[[Mapping[str, Any]], Awaitable[None]],
    batch_size: int = 100,
    table: str = DEFAULT_OUTBOX_TABLE,
) -> int:
    """Relay a batch of unsent outbox rows to ``publish``, then mark them sent. Returns the count.

    Claims up to ``batch_size`` unsent rows in id order with ``FOR UPDATE SKIP LOCKED`` — so many
    relay workers can run concurrently without blocking or double-claiming — publishes each via the
    ``publish`` callback (receives a mapping with ``id``/``topic``/``payload``/``created_at``), then
    stamps ``sent_at`` for the whole batch and commits. Everything runs in one transaction: if
    ``publish`` raises, the claim rolls back and the rows are retried on the next :func:`drain`.

    Delivery is **at-least-once**: a crash after the broker accepts a message but before commit
    replays it. Make ``publish`` idempotent or dedupe on the consumer
    (:mod:`dbkit.integrations.inbox`). Loop :func:`drain` from a relay worker (a return of 0 means
    the outbox is momentarily empty).
    """
    async with db.transaction(target=target) as tx:
        rows = await tx.fetch_all(
            sql(
                f"SELECT id, topic, payload, created_at FROM {table} "
                "WHERE sent_at IS NULL ORDER BY id FOR UPDATE SKIP LOCKED LIMIT :n"
            ),
            {"n": batch_size},
        )
        if not rows:
            return 0
        for row in rows:
            await publish(dict(row))
        await tx.execute(
            sql(f"UPDATE {table} SET sent_at = now() WHERE id = ANY(:ids)"),
            {"ids": [row["id"] for row in rows]},
        )
    return len(rows)
