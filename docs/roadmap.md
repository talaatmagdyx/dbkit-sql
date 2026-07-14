# Delivery Roadmap

dbkit is delivered in phases. Phases 1–5 are delivered; a handful of stretch items remain
noted per phase, and any interface not yet implemented raises `UnsupportedOperationError` so
the public API surface stays stable.

See `docs/requirements.md` for the full product/engineering requirements this roadmap implements.

## Phase 1 — Core Runtime ✅ (delivered)

- Configuration model, loaders (dict/env/YAML), validation, connection-budget calculator.
- `Query` + `sql()` wrapper + registry.
- `DatabaseTarget` + named-database routing (primary).
- Typed results, `map_to` mappers, cardinality enforcement (SQLAlchemy's own `Result.one()` /
  `.one_or_none()` / `.scalar_one()` / `.scalars().all()`, not reimplemented).
- Normalized error hierarchy, SQLSTATE-first classification (core codes).
- Engine registry, instrumented connection pooling, leak detection.
- Explicit transactions, savepoints, commit-unknown detection, cancellation cleanup.
- Health checks, graceful startup/shutdown.
- Structured logging + metrics protocol (Prometheus **or** OpenTelemetry Metrics adapter) +
  OpenTelemetry tracing (`observability/tracing.py`, graceful no-op when OTel isn't
  installed/enabled; `SpanKind.CLIENT` spans on every read/write/transaction/stream/bulk-write/
  COPY, never carrying SQL text or params; injectable `tracer_provider` for per-tenant/test
  isolation). Log events carry the active span's `trace_id`/`span_id` for trace/log
  correlation.
- Sync + async facades from one source (unasync generation), documented transformation rules
  and a translation-completeness smoke test (`docs/testing.md`,
  `tests/unit/test_unasync_translation.py`).
- `ResilientExecutor` (`_async/executor.py`): connection acquisition, pool-wait/error/slow-query
  instrumentation, and the concurrency-limiter + circuit-breaker + retry-loop orchestration
  extracted from `AsyncDatabase` into a focused collaborator — the facade is a thinner
  dispatcher over fetch/execute/bulk/stream/transaction, each delegating to it.

## Phase 2 — Resilience ✅ (delivered)

- SQLSTATE classification with a retryability map.
- Retry executor (`_async/resilience.py`): idempotency-gated, deadline-aware, exponential +
  full jitter; the decision logic is the pure, property-tested `_core.policies`. A best-effort
  static lint (`_core/idempotency_lint.py`, surfaced via `dbkit query-list`) flags writes marked
  `idempotent=True` with no visible `ON CONFLICT`-style guard in their SQL text.
- Circuit breaker (`_core/circuit.py`): per db+shard+role, only infrastructure failures trip
  it; opt-in via `circuit_breaker.enabled`.
- Concurrency tiers (`ConcurrencyLimiter`): per-database/reads/writes/bulk semaphores acquired
  before pool checkout, bounded by the call's own effective timeout — a saturated tier raises
  `DatabaseOverloadedError` instead of queueing invisibly forever. `db_circuit_breaker_state`
  gauge (0/1/2) makes breaker state directly observable (`docs/observability.md`).
- Perf: dropped the redundant per-op `SET statement_timeout` round trip on the async path
  (client-side `asyncio.timeout` covers it), cutting small-read overhead from ~31% to ~6% over
  raw SQLAlchemy Core.
- Transaction instrumentation: `TX_TOTAL`/`TX_DURATION`/`TX_ROLLBACK`/`COMMIT_UNKNOWN` metrics
  (previously declared but never emitted) plus long-transaction detection — a
  `database.transaction.long_running` warning when a transaction is held open longer than
  `Defaults.long_transaction_warning_seconds` (default 5.0), mirroring the pool's existing
  long-connection-hold warning. Duration is also set as a `dbkit.transaction` span attribute.

## Phase 3 — High throughput ✅ (delivered)

- Streaming (`db.stream`): server-side cursor / `yield_per`, bounded memory, max-duration guard,
  guaranteed connection release; both frontends.
- Bulk `insert_many` / `upsert_many` (PostgreSQL `ON CONFLICT`) with adaptive batch sizing
  (rows/bind-param ceiling) and `atomic` | `best_effort` | `split_on_failure` modes.
- PostgreSQL `COPY` (`db.copy_records`) via the psycopg raw-driver escape hatch — ~90× faster
  than per-row inserts in-benchmark; bounded memory.
- Consumer integration (`dbkit.integrations`): inbox dedup + `process_once` + `ack_after_commit`
  (§28 effectively-once flow — delivery is at-least-once; the inbox row and business write
  commit atomically, so redelivery is a no-op) and `BatchCollector` micro-batching. These are
  the DB-side primitives a message consumer needs; dbkit is scoped as **database-only** and
  deliberately does not ship a broker-facing adapter (no RabbitMQ/Celery client dependency) —
  an application wires these primitives into its own consumer loop. No built-in poison-message/
  max-retry tracking — track attempt counts in the inbox table or via the broker's own
  redelivery count if you need that.
- `unnest()` bulk strategy (`postgres/unnest.py`, `strategy="unnest"` on `insert_many`/
  `upsert_many`): one array-per-column bind instead of one bind per column per row, so batch
  size isn't limited by the 65535 bind-parameter ceiling — ~32× faster than `execute_many` at
  20k rows in-benchmark. PostgreSQL only; each column cast with `CAST(:name AS type[])`
  (`:name::type[]` is silently left unparsed by SQLAlchemy's `text()`).
- psycopg pipeline mode (`postgres/pipeline.py`, `tx.pipeline()`): batches dependent statements
  into one round trip without waiting for each response — mirrors the COPY escape-hatch
  pattern (raw driver connection via `get_raw_connection()`/`driver_connection`). The benefit
  is amortizing network round-trip latency; it shows no speedup on localhost, only over a real
  network hop.

## Phase 4 — Multi-database & sharding ✅ (delivered)

- Shard resolvers (`_core/routing.py`): `HashShardResolver` (SHA-256, deterministic across
  restarts/processes — never Python's randomized `hash()`), `RangeShardResolver`,
  `DirectoryShardResolver` (fails closed on unmapped keys), `CallableShardResolver`.
- Replica routing: `RoundRobinReplicaSelector` / `WeightedReplicaSelector`, wired into the
  facade — reads with `role="read"`/`"prefer_replica"` now actually route to a configured
  replica engine; writes and `role="primary_only"` always hit the primary.
- `db.consistency_scope(mode="read_your_writes")`: a task-local (`contextvars`) override that
  forces reads back to the primary so they observe writes made earlier in the same scope.
- Engine LRU eviction (`AsyncEngineRegistry(evict_lru=True)`): reaching `max_engines` disposes
  the least-recently-used engine instead of failing — for dynamic per-tenant deployments.
  Verified safe under concurrent use: SQLAlchemy's `Engine.dispose()` only closes idle pooled
  connections, so a connection already checked out from the evicted engine keeps working until
  the caller closes it (regression-tested). Default (`evict_lru=False`) keeps the strict
  hard-cap.
- No cross-shard transactions — an application needing atomic multi-shard writes needs its own
  outbox/saga pattern; dbkit also does not authorize `DatabaseTarget`/shard-key values, so
  tenant/shard access control is the calling application's responsibility.
- Per-database connection budgets (`DatabaseConfig.enforce_connection_budget`): a single
  database can fail startup on its own budget, independent of the global one.

## Phase 5 — Production hardening & OSS release ✅ (delivered)

- CLI (`dbkit` console script / `dbkit.cli.main`): `check`, `health`, `pools`, `engines`,
  `config-validate`, `connection-budget`, `query-list` — secret-redacted output, classified
  errors instead of tracebacks, non-zero exit on failure.
- Docs site (`mkdocs.yml` + `docs/`, mkdocs-material): builds clean under `--strict`.
- PyPI release readiness: `python -m build` + `twine check` verified against a real build
  (installed the built wheel into a clean venv and smoke-tested it); `.github/workflows/
  release.yml` (tag-triggered, PyPI trusted publishing via OIDC, not yet used for a release).
- Failure-injection/load/soak suites and the benchmark harness were delivered alongside
  Phases 1–3 (see `docs/testing.md`).
- PgBouncer-compatible pooling mode (`PoolConfig.pgbouncer_compatible`): disables driver-side
  autoprepare (psycopg's `prepare_threshold`, asyncpg's `statement_cache_size`) — required
  correctness fix under PgBouncer's *transaction* pooling, where a connection may hit a
  different physical backend each transaction. Every session setting was already scoped with
  `SET LOCAL`/per-transaction, never a bare session-level `SET`, so no other change was needed.
- Asyncpg CI lane (`.github/workflows/ci.yml`, `integration-asyncpg` job): the async-only
  integration/chaos/sharding/CLI suite runs against asyncpg in addition to psycopg (sync tests
  and the psycopg-only COPY/pipeline/PgBouncer-autoprep tests self-skip via the
  `requires_psycopg` fixture when the DSN isn't psycopg). Found and fixed one real bug in the
  process: `postgres/copy.py`'s driver guard checked for a `cursor` attribute, which asyncpg's
  raw connection also has (an incompatible, unrelated method) — it now checks for `pipeline`
  (psycopg-only, matching `postgres/pipeline.py`'s own guard), so COPY against a non-psycopg
  driver now fails with a clean `DatabaseUnsupportedOperationError` instead of a confusing
  raw `TypeError`.
- A real primary-failover chaos test (`test_recovers_after_primary_failover_to_a_different_
  backend`) alongside the existing same-instance-restart test — two throwaway containers
  behind an in-process proxy, proving recovery lands on a genuinely different backend.
- `docs/security.md`, `docs/observability.md` (example Grafana dashboard + Prometheus alert
  rules), `docs/troubleshooting.md`, and `docs/versioning.md` (SemVer + deprecation policy).
- A 10-minute soak (`benchmarks.soak --duration 600`) and a `BatchCollector` high-fan-in
  benchmark (`benchmarks/bench_batch_collector.py`) as evidence beyond the 60s CI soak gate.

No further stretch items remain from the original spec. dbkit is intentionally scoped as a
database-only toolkit — no broker/message-queue adapter is planned.
