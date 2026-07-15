# Integrations

Database-side primitives for message-driven consumers (§28) and micro-batching (§17.1).
dbkit is scoped as database-only and does not ship a broker-facing adapter — an application
wires these into its own consumer loop.

::: dbkit.integrations.inbox_ddl

::: dbkit.integrations.partitioned_inbox_ddl

::: dbkit.integrations.inbox_month_partition_ddl

::: dbkit.integrations.process_once

::: dbkit.integrations.ack_after_commit

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
