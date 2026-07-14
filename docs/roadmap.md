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

## Phase 2 — Resilience ✅ (delivered)

- SQLSTATE classification with a retryability map.
- Retry executor (`_async/resilience.py`): idempotency-gated, deadline-aware, exponential +
  full jitter; the decision logic is the pure, property-tested `_core.policies`.
- Circuit breaker (`_core/circuit.py`): per db+shard+role, only infrastructure failures trip
  it; opt-in via `circuit_breaker.enabled`.
- Concurrency tiers (`ConcurrencyLimiter`): per-database/reads/writes/bulk semaphores acquired
  before pool checkout.
- Perf: dropped the redundant per-op `SET statement_timeout` round trip on the async path
  (client-side `asyncio.timeout` covers it), cutting small-read overhead from ~31% to ~6% over
  raw SQLAlchemy Core.

Remaining for a later pass: long-transaction detection surfaced as a metric/warning.

## Phase 3 — High throughput ✅ (delivered)

- Streaming (`db.stream`): server-side cursor / `yield_per`, bounded memory, max-duration guard,
  guaranteed connection release; both frontends.
- Bulk `insert_many` / `upsert_many` (PostgreSQL `ON CONFLICT`) with adaptive batch sizing
  (rows/bind-param ceiling) and `atomic` | `best_effort` | `split_on_failure` modes.
- PostgreSQL `COPY` (`db.copy_records`) via the psycopg raw-driver escape hatch — ~90× faster
  than per-row inserts in-benchmark; bounded memory.
- Consumer integration (`dbkit.integrations`): inbox dedup + `process_once` + `ack_after_commit`
  (§28 exactly-once flow) and `BatchCollector` micro-batching.

Remaining for a later pass: psycopg pipeline mode, `unnest()` array-insert strategy, a
first-class Celery/RabbitMQ subscriber adapter.

## Phase 4 — Multi-database & sharding

Shard resolver strategies (hash/range/directory); replica routing + `consistency_scope`;
engine LRU eviction for tenant explosion; per-database budgets.

## Phase 5 — Production hardening & OSS release

OpenTelemetry spans; CLI; failure-injection/load/soak suites; benchmark harness;
PgBouncer-compatible mode; docs site; first PyPI release.
