# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed — use SQLAlchemy's native mechanisms instead of reimplementing them
- Cardinality enforcement (`fetch_one`/`fetch_optional`/`fetch_value`/`fetch_values`) now
  calls SQLAlchemy's own `Result.one()` / `.one_or_none()` / `.scalar_one()` /
  `.scalars().all()` directly instead of fetching all rows and enforcing exactly-one/
  at-most-one/scalar semantics by hand; `NoResultFound`/`MultipleResultsFound` are translated
  to the existing `DatabaseResultError`. `_core/result.py` no longer reimplements this — it
  only maps rows to application types, which SQLAlchemy has no opinion on.
- `db.transaction(isolation=..., read_only=..., deferrable=...)` now uses
  `AsyncConnection.execution_options(isolation_level=..., postgresql_readonly=...,
  postgresql_deferrable=...)` instead of raw `SET TRANSACTION ...` SQL, applied *before*
  `BEGIN` (execution options configure the driver connection itself). Added a `deferrable`
  parameter — only meaningful with `isolation="serializable", read_only=True`, letting a
  read-only serializable transaction wait for a safe snapshot instead of risking a
  serialization failure. `statement_timeout`/`lock_timeout` still use `SET LOCAL` (no
  portable execution_option exists for PostgreSQL GUCs).
- Commit-unknown detection (`_is_connection_error`) now prefers SQLAlchemy's own
  `DBAPIError.connection_invalidated` flag (each dialect's own `is_disconnect()` logic) over a
  blanket `isinstance(exc, (InterfaceError, OperationalError))` check — `OperationalError`
  also covers transient-but-not-disconnected conditions (e.g. some lock/resource errors),
  which would otherwise over-classify ordinary failures as commit-unknown.

### Added — unnest() bulk strategy, psycopg pipeline mode, PgBouncer-compatible mode
- `strategy="unnest"` on `insert_many`/`upsert_many` (`postgres/unnest.py`): one array-per-
  column bind instead of one bind per column per row, so batch size isn't limited by
  PostgreSQL's 65535 bind-parameter ceiling — ~32× faster than `execute_many` at 20k rows
  in-benchmark. Each column is cast with `CAST(:name AS type[])` — SQLAlchemy's `text()`
  silently leaves `:name::type` unparsed as a bindparam, so the `::` shorthand can't be used.
  Works with `atomic`/`best_effort`/`split_on_failure` modes and `ON CONFLICT` upserts.
- `tx.pipeline()` (`postgres/pipeline.py`): psycopg pipeline mode as a raw-driver escape
  hatch, mirroring COPY's `get_raw_connection()`/`driver_connection` pattern. Batches
  dependent statements into one round trip without waiting for each response; ordinary
  `tx.execute(...)` calls work unchanged inside the block. PostgreSQL + psycopg only.
- `PoolConfig.pgbouncer_compatible`: disables driver-side autoprepare (psycopg's
  `prepare_threshold`, asyncpg's `statement_cache_size`) — required under PgBouncer
  *transaction* pooling, where a connection may hit a different physical backend each
  transaction. Every session setting was already `SET LOCAL`/per-transaction scoped.
- Examples: `pipeline_mode.py`, `pgbouncer_mode.py`, and an `unnest` section added to
  `bulk_insert_upsert.py`.

### Added — Transaction instrumentation & long-transaction detection
- `TX_TOTAL` / `TX_DURATION` / `TX_ROLLBACK` / `COMMIT_UNKNOWN` metrics are now actually
  emitted on every transaction exit (commit, rollback, cancellation, commit-unknown) — these
  constants existed since Phase 1 but were never wired up.
- `database.transaction.long_running` warning log event when a transaction is held open
  longer than `Defaults.long_transaction_warning_seconds` (default `5.0`), mirroring the
  pool's existing long-connection-hold warning.
- Transaction duration is set as a `dbkit.transaction` span attribute
  (`db.transaction.duration_ms`) on every exit path.

### Added — Phase 1 (Core Runtime)
- Configuration model with dict/env/YAML loaders, `${VAR}` expansion, startup validation,
  connection-budget calculation, and secret-free serialization.
- `Query` object and `sql()` wrapper (the only accepted raw-string path).
- `DatabaseTarget` and named-database routing (primary resolution).
- Typed results (`ExecutionResult`) with `map_to` mappers and cardinality enforcement.
- Normalized error hierarchy with SQLSTATE-first classification.
- Async and sync `AsyncDatabase`/`Database` facades sharing one API.
- Engine registry (one engine per target) and instrumented connection pooling with
  leak detection and long-hold warnings.
- Explicit transactions with savepoints, commit-unknown detection, and cancellation cleanup.
- Health checks (liveness/readiness), graceful startup/shutdown.
- Structured logging and a metrics protocol (Prometheus adapter behind an extra).

### Added — Quality infrastructure
- Resilience / chaos suite (`tests/integration/test_resilience_scenarios.py`): backend
  termination mid-transaction, commit-unknown race, cancellation storm, bounded connections
  under concurrency, graceful shutdown under load, and full server restart recovery
  (Docker-gated). Faults induced with `pg_terminate_backend` and `docker restart`.
- Property-based tests (hypothesis): redaction never leaks, SQLSTATE classification totality,
  timeout-resolution lower-bound, backoff bounds/monotonicity, connection-budget identity.
- Security tests: bare-string SQL rejection, DSN/parameter redaction, secret-free errors and
  logs, and (integration) SQL-injection payloads stored literally via bound parameters.
- Benchmark suite (`python -m benchmarks`): overhead A/B vs raw psycopg and raw SQLAlchemy
  Core, throughput (sync + async), paced latency P50/P95/P99, and batch vs per-row inserts —
  median/CV stats, env fingerprint, JSON persistence with regression deltas.
- Gating soak (`python -m benchmarks.soak`): paced load with periodic fault injection,
  asserting no-loss recovery and bounded RSS / FDs / tasks / pool connections.

### Added — Phase 2 (Resilience)
- Retry executor: idempotency-gated, deadline-aware, exponential + full jitter, wired into
  every `fetch_*`/`execute` call. Non-idempotent writes and commit-unknown outcomes are never
  retried.
- Circuit breaker per db+shard+role (`CircuitBreakerConfig`, opt-in): only infrastructure
  failures (connection/pool/availability/timeout) trip it; integrity/programming errors do not.
- Concurrency limiter: per-database/reads/writes/bulk semaphores acquired before pool checkout.
- New chaos scenarios: serialization failure retried to success, circuit opens under sustained
  connection failure.

### Changed
- Performance: the async path no longer issues a per-operation `SET statement_timeout` round
  trip (client-side `asyncio.timeout` bounds the statement); small-read overhead vs raw
  SQLAlchemy Core dropped from ~31% to ~6%. The sync path keeps the server-side timeout.

### Added — Phase 3 (High throughput)
- `db.stream(...)` — server-side cursor streaming with bounded memory, `max_duration` guard,
  and guaranteed connection release (async: `AsyncConnection.stream`; sync: `yield_per`).
- `db.insert_many(table, rows, ...)` and `db.upsert_many(table, rows, ...)` — adaptive batch
  sizing (bounded by rows and PostgreSQL's 65535 bind-param ceiling) with `atomic`,
  `best_effort`, and `split_on_failure` modes.
- `db.copy_records(table, columns, records, ...)` — PostgreSQL COPY via the psycopg raw-driver
  escape hatch; ~90× faster than per-row inserts in the batch benchmark.
- `dbkit.integrations`: `inbox_ddl` / `process_once` / `ack_after_commit` (exactly-once
  message processing, §28) and `BatchCollector` (consumer micro-batching).
- Config: `BulkConfig` (batch-sizing defaults).
- Benchmarks: batch suite gains a COPY lane; tests cover streaming, all bulk modes, upsert,
  COPY, and inbox idempotency on both frontends.

### Added — Examples & CRUD benchmark
- `examples/`: a runnable, idempotent script for every feature (transactions & savepoints,
  error classification, retries & circuit breaker, streaming, bulk insert/upsert, COPY,
  exactly-once consumer processing, micro-batching, health/pool introspection, sync/async
  parity) plus `run_all.py` to execute them all against one database.
- `benchmarks/bench_crud.py` (`--only crud`): a single report covering INSERT, SELECT (point
  + range), UPDATE, UPSERT, and DELETE — throughput (ops/s) and per-operation P50/P99 latency,
  on both frontends.

### Added — OpenTelemetry tracing
- `observability/tracing.py`: a `Tracer`/`SpanHandle` wrapper that is a full no-op when
  `opentelemetry-api` isn't installed or `observability.tracing` is disabled. Spans on every
  read/write (`dbkit.read`/`dbkit.write`), transaction, stream, bulk write, and COPY, with
  §25.2 attributes (`db.system`, `db.operation.type`, `db.query.name`, `db.namespace`,
  `db.shard.id`, `db.target.role`, `db.pool.wait_ms`, `db.rows_affected`) — SQL text and bound
  parameters never reach a span. Exceptions are recorded and set span status automatically.

### Added — Phase 4 (Multi-database & sharding)
- Shard resolvers (`_core/routing.py`): `HashShardResolver` (deterministic SHA-256, not
  Python's randomized `hash()`), `RangeShardResolver`, `DirectoryShardResolver` (fails closed
  on unmapped keys), `CallableShardResolver`.
- Replica routing wired into the facade: `RoundRobinReplicaSelector` / `WeightedReplicaSelector`
  select a real replica engine for `role="read"`/`"prefer_replica"` targets; writes and
  `role="primary_only"` always use the primary.
- `db.consistency_scope(mode="read_your_writes")`: a task-local override forcing reads to the
  primary within the scope.
- `AsyncEngineRegistry(evict_lru=True)`: LRU eviction of the least-recently-used engine when
  `max_engines` is reached, instead of failing — for dynamic per-tenant deployments. Default
  (`evict_lru=False`) preserves the strict hard-cap.
- `DatabaseConfig.enforce_connection_budget`: a single database can enforce its own connection
  budget independently of the process-wide one.
- Config: `DbkitConfig.max_engines` / `evict_lru_engines`.

### Added — Phase 5 (Production hardening)
- CLI (`dbkit` console script, `dbkit[cli]` extra): `check`, `health`, `pools`, `engines`,
  `config-validate`, `connection-budget`, `query-list`. Secret-redacted output; classified
  errors instead of raw tracebacks; non-zero exit on failure.
- Docs site: `mkdocs.yml` (mkdocs-material) over `docs/`, builds clean under `--strict`.
- PyPI release readiness: verified `python -m build` + `twine check` against a real build
  (wheel installed into a clean venv and smoke-tested); `.github/workflows/release.yml`
  (tag-triggered, PyPI trusted publishing via OIDC).
