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
  PostgreSQL COPY, and effectively-once consumer helpers (transactional inbox pattern +
  micro-batching) — delivery is at-least-once, processing is deduplicated in the same
  transaction as the business write.
- **Multi-database and sharded.** Named databases, pluggable shard resolvers (hash/range/
  directory/callable), replica routing with a read-your-writes override (pins reads to the
  primary for the scope, not lag-aware replica tracking), and LRU engine eviction for
  dynamic per-tenant deployments. No cross-shard transaction support — use an outbox/saga
  pattern for multi-shard writes, and note that dbkit trusts the `DatabaseTarget`/shard key
  you give it; tenant/shard authorization is your application's responsibility.
- **Fully observable.** Structured logging, a metrics protocol (Prometheus adapter included),
  and OpenTelemetry tracing — statement text and parameters never reach a log, span, or metric
  label.
- **Built on SQLAlchemy.** Pooling, dialects, and driver integration are SQLAlchemy's job —
  dbkit adds routing, resilience, observability, and bulk/streaming ergonomics on top.

PostgreSQL is the first-class optimized target with psycopg 3 (the default driver, and the
only one with a sync API — COPY and pipeline mode also require it). asyncpg is CI-covered for
the async frontend (reads/writes/transactions, resilience/chaos, sharding/replicas, CLI) but is
async-only and has no COPY/pipeline support; the dialect-agnostic core otherwise runs on any
SQLAlchemy backend.

## Install

```bash
pip install "dbkit-sql[psycopg]"                  # async + sync PostgreSQL
pip install "dbkit-sql[psycopg,yaml,prometheus,otel,cli]"
```

> The PyPI distribution is `dbkit-sql` (the name `dbkit` was already taken) — but the import
> stays `import dbkit` regardless of which extras you install.

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
bulk insert/upsert, COPY, effectively-once consumer processing, micro-batching, sharding/replica
routing, and sync/async parity.

## When not to use dbkit

Not an ORM (no relationship loading, sessions, or model layer — pair a real ORM alongside it if
your domain needs that) and not a migration tool (pair it with Alembic or similar). Database-only
by design: no broker/message-queue client ships with it.

## Where to go next

- **[CLI](cli.md)** — `dbkit check` / `health` / `pools` / `engines` / `config-validate` /
  `connection-budget` / `query-list`.
- **[Testing](testing.md)** — unit/property/security tests, the chaos suite, benchmarks, soak.
- **[Roadmap](roadmap.md)** — what's delivered per phase and what's next.
- **[Requirements](requirements.md)** — the full design specification dbkit implements.
