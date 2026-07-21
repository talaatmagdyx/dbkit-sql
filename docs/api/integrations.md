# Integrations

Database-side primitives for message-driven consumers (§28) and micro-batching (§17.1).
dbkit is scoped as database-only and does not ship a broker-facing adapter — an application
wires these into its own consumer loop.

::: dbkit.integrations.inbox_ddl

::: dbkit.integrations.partitioned_inbox_ddl

::: dbkit.integrations.inbox_month_partition_ddl

::: dbkit.integrations.process_once

::: dbkit.integrations.ack_after_commit

## Transactional outbox (§28.4)

The mirror of the inbox: enqueue an event **in the same transaction as the business write** so the
event exists iff the change persisted, then relay unsent rows to a broker at-least-once. Single-shard
(the outbox table lives on the same database as the write); pair with the inbox on the consumer for
effectively-once. Schema-agnostic — you pass the table name and an opaque JSON payload.

```python
from dbkit.integrations import outbox_ddl, enqueue, drain

await db.execute(sql(outbox_ddl()), target=t)   # once, at setup (script: split on ';')

async with db.transaction(target=t) as tx:       # atomic: write + event
    await tx.execute(UPDATE_ORDER, params)
    await enqueue(tx, topic="order.completed", payload={"order_id": oid})

# relay loop (separate worker): publish unsent rows, mark them sent
await drain(db, target=t, publish=my_async_publish)
```

::: dbkit.integrations.outbox_ddl

::: dbkit.integrations.partitioned_outbox_ddl

::: dbkit.integrations.outbox_month_partition_ddl

::: dbkit.integrations.enqueue

::: dbkit.integrations.drain

::: dbkit.integrations.BatchCollector

## HTTP APIs (FastAPI / Starlette)

Category-driven RFC 7807 responses for unhandled `DatabaseError`s: overload
(pool timeout, limiter, circuit open, backend unreachable) → **503** with a
`Retry-After` header (derived from the circuit breaker's `open_seconds` when you
pass your facade); query timeouts → **504**; programming/integrity errors stay
**500** and loud. Requires the `dbkit-sql[fastapi]` extra (Starlette only).

```python
from dbkit.integrations.fastapi import install_exception_handlers

install_exception_handlers(app, database=db)
```

::: dbkit.integrations.fastapi.install_exception_handlers

## Per-query session settings

`Query.settings` applies transaction-local PostgreSQL settings right before the
statement — parameterized `set_config(name, value, true)`, names validated:

```python
COUNTS = Query(
    name="inbox.counts",
    statement=sql("SELECT ..."),
    settings={"jit": "off"},          # or {"work_mem": "64MB"} for a heavy aggregate
)
```

The setting never leaks: it resets when the statement's transaction ends, before
the connection returns to the pool.
