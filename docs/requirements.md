# dbkit — Product & Engineering Requirements

> This is the design specification dbkit implements. It is the async database toolkit
> requirements document, adapted to the `dbkit` name and its dual sync/async, SQLAlchemy-Core
> foundation. The delivery roadmap in `docs/roadmap.md` sequences this into phases.

The toolkit is a **thin resilience and ergonomics layer over SQLAlchemy Core** — it reuses
SQLAlchemy's engines, pooling, dialects, `text()`, and events, and adds routing, bounded
concurrency, retries, error normalization, timeouts, bulk/streaming, and observability.
It supports both a synchronous (`Database`, `Engine`) and an asynchronous (`AsyncDatabase`,
`AsyncEngine`) frontend from a single source of truth. No ORM.

Key guarantees:

- SQL-first (raw SQL via `sql()`/`text()` and Core expressions), never ORM.
- Every operation has a deadline; pools are bounded; transactions are explicit.
- Errors are normalized using SQLSTATE where available; non-idempotent writes are not
  retried automatically; commit-unknown is a distinct outcome.
- Cancellation always cleans up connections (async); pool, query, and transaction metrics
  are exported; sensitive data is redacted.
- Multiple databases, deterministic shard routing, primary/replica routing.
- PostgreSQL is the first-class optimized target (psycopg 3 default, asyncpg optional);
  the dialect-agnostic core runs on any SQLAlchemy backend.

The full 38-section source specification (architecture, module layout, API surface,
pooling, timeouts, error model, retries, circuit breaker, concurrency, bulk/streaming,
multi-db/sharding, replicas, observability, health, startup/shutdown, integrations,
security, config, CLI, testing, performance, delivery phases, definition of done) governs
scope. Sections are referenced throughout the source as `§N`.
