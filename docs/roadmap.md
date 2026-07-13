# Delivery Roadmap

dbkit is delivered in phases. Phase 1 is the current focus; later modules exist as typed
stubs that raise `UnsupportedOperationError` so the public API surface is stable from day one.

See `docs/requirements.md` for the full product/engineering requirements this roadmap implements.

## Phase 1 — Core Runtime (current)

- Configuration model, loaders (dict/env/YAML), validation, connection-budget calculator.
- `Query` + `sql()` wrapper + registry.
- `DatabaseTarget` + named-database routing (primary).
- Typed results, `map_to` mappers, cardinality enforcement.
- Normalized error hierarchy, SQLSTATE-first classification (core codes).
- Engine registry, instrumented connection pooling, leak detection.
- Explicit transactions, savepoints, commit-unknown detection, cancellation cleanup.
- Health checks, graceful startup/shutdown.
- Structured logging + metrics protocol (Prometheus adapter).
- Sync + async facades from one source (unasync generation).

## Phase 2 — Resilience

Full SQLSTATE table + retryability map; retry executor (idempotency-gated, deadline-aware,
exponential + full jitter); circuit breaker (per db+shard+role); concurrency tiers;
long-transaction detection.

## Phase 3 — High throughput

Bulk insert/upsert with adaptive batch sizing; PostgreSQL COPY (both directions);
`unnest()` array inserts; streaming (`yield_per`, idle timeout, guaranteed release);
psycopg pipeline mode; RabbitMQ integration (ack-after-commit, DLQ, commit-unknown handling,
`BatchCollector`, inbox helper); Celery integration.

## Phase 4 — Multi-database & sharding

Shard resolver strategies (hash/range/directory); replica routing + `consistency_scope`;
engine LRU eviction for tenant explosion; per-database budgets.

## Phase 5 — Production hardening & OSS release

OpenTelemetry spans; CLI; failure-injection/load/soak suites; benchmark harness;
PgBouncer-compatible mode; docs site; first PyPI release.
