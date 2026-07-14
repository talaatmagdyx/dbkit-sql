# dbkit

A thin, high-throughput SQL toolkit built **on top of SQLAlchemy Core** — for APIs, message
consumers, background workers, and CLIs.

- **SQL-first, ORM-free.** Write SQL or SQLAlchemy Core expressions. No models, no sessions.
- **Sync _and_ async.** `Database` and `AsyncDatabase` share one API, one config, one error
  model — the sync frontend is generated from the async one, so they never drift.
- **Safe by default.** Bounded pools, explicit transactions, per-operation timeouts,
  normalized errors (SQLSTATE-aware), and conservative retries.
- **Resilient.** Circuit breaker, idempotency-gated retries, and per-database concurrency
  limits, all opt-in and independently configurable.
- **High-throughput paths.** Bounded-memory streaming, adaptive-batch bulk insert/upsert,
  PostgreSQL COPY, and exactly-once consumer helpers (inbox pattern + micro-batching).
- **Multi-database and sharded.** Named databases, pluggable shard resolvers (hash/range/
  directory/callable), replica routing with read-your-writes, and LRU engine eviction for
  dynamic per-tenant deployments.
- **Fully observable.** Structured logging, a metrics protocol (Prometheus adapter included),
  and OpenTelemetry tracing — statement text and parameters never reach a log, span, or metric
  label.
- **Built on SQLAlchemy.** Pooling, dialects, and driver integration are SQLAlchemy's job —
  dbkit adds routing, resilience, observability, and bulk/streaming ergonomics on top.

PostgreSQL is the first-class optimized target (psycopg 3 default, asyncpg optional); the
dialect-agnostic core runs on any SQLAlchemy backend.

## Install

```bash
pip install "dbkit[psycopg]"                  # async + sync PostgreSQL
pip install "dbkit[psycopg,yaml,prometheus,otel,cli]"
```

## Quickstart

```python
from dbkit import AsyncDatabase, DatabaseTarget, Query, sql

GET_USER = Query(
    name="users.get_by_id",
    statement=sql("SELECT id, email FROM users WHERE id = :id"),
    operation="read",
    idempotent=True,
    timeout=1.0,
)

db = AsyncDatabase.from_config({
    "databases": {"app": {"primary": {"url": "postgresql+psycopg://localhost/app"}}},
})

async def main():
    await db.start()
    try:
        user = await db.fetch_optional(GET_USER, {"id": 1},
                                       target=DatabaseTarget(database="app"))
    finally:
        await db.close()
```

The sync API is identical, without `await`:

```python
from dbkit import Database
db = Database.from_config(...)
db.start()
user = db.fetch_optional(GET_USER, {"id": 1}, target=DatabaseTarget(database="app"))
db.close()
```

See **Examples** in the repository's `examples/` directory for a runnable, idempotent script
covering every feature — transactions & savepoints, retries & the circuit breaker, streaming,
bulk insert/upsert, COPY, exactly-once consumer processing, micro-batching, sharding/replica
routing, and sync/async parity.

## Where to go next

- **[CLI](cli.md)** — `dbkit check` / `health` / `pools` / `engines` / `config-validate` /
  `connection-budget` / `query-list`.
- **[Testing](testing.md)** — unit/property/security tests, the chaos suite, benchmarks, soak.
- **[Roadmap](roadmap.md)** — what's delivered per phase and what's next.
- **[Requirements](requirements.md)** — the full design specification dbkit implements.
