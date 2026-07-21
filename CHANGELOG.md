# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.3.0] - 2026-07-21

### Added
- **Transaction-scoped advisory locks** on the transaction scope (§11.7, sync mirror included):
  `await tx.advisory_xact_lock(key)` (blocking, auto-released at commit/rollback — cannot leak) and
  `await tx.try_advisory_xact_lock(key) -> bool` (non-blocking). `key` is an `int` (used directly)
  or a `str` (hashed server-side via `hashtextextended`) — always bound, never interpolated. Waits
  respect the transaction's `lock_timeout` (→ `DatabaseLockTimeoutError`). PostgreSQL only; raises
  `DatabaseUnsupportedOperationError` otherwise. Serializes a read-modify-write on a logical key
  without locking rows.
- **Transactional outbox** helpers in `dbkit.integrations` (the mirror of the inbox, §28.4):
  `outbox_ddl` / `partitioned_outbox_ddl` / `outbox_month_partition_ddl` (schema-agnostic DDL),
  `enqueue(tx, *, topic, payload, table=...)` (insert an event on the caller's transaction, atomic
  with the business write), and `drain(db, *, target, publish, batch_size=..., table=...)` (relay
  unsent rows with `FOR UPDATE SKIP LOCKED`, publish, mark sent — at-least-once). Single-shard;
  pair with the consumer-side inbox for effectively-once. Payload is opaque JSON — no product- or
  dbkit-specific columns. New examples `advisory_locks.py` + `transactional_outbox.py`.

## [0.2.0] - 2026-07-15

### Added
- `AsyncDatabase.ensure_database(name, config)` — idempotent registration: no-op when the
  config is unchanged (lock-free, a few µs), (re)registers when missing or changed. Safe to
  call in front of every query; config changes (including password-only rotations) take
  effect in place with old engines disposed.
- **Dynamic-first configs**: an explicit empty ``databases: {}`` mapping is now valid —
  services that discover every DSN at runtime bootstrap with no static databases. A missing
  ``databases`` key is still rejected.
- **Dynamic database registration**: `AsyncDatabase.register_database(name, config)` /
  `unregister_database(name)` (sync mirror included) — register shard/tenant databases whose
  DSNs are discovered at runtime (e.g. from a service registry), without a service-side engine
  registry. Copy-on-write config swap (readers never see a partial mapping), single-flight
  registration lock, engine + limiter + breaker cleanup on replace/unregister, replica-selector
  updates, and eager engine creation for `required` primaries when already started.
- `AsyncDatabase.database_scope(name, config)` — context-manager form: register for the
  duration of a block (tests, migrations, one-off tenant jobs), auto-unregister + dispose on
  exit. Documented anti-pattern: never scope per request — that defeats pooling; services
  register long-lived shards once.
- `DatabaseConfig.from_dict(...)` — build one database config from the same dict shape as a
  `databases` entry.
- `AsyncEngineRegistry.dispose_database(name)` and `ResilientExecutor.forget_database(name)`.
- `RoundRobinReplicaSelector.set_replicas(...)` / `WeightedReplicaSelector.set_replicas(...)`
  for runtime replica updates.
- `dbkit.errors.OVERLOAD_CATEGORIES` / `TIMEOUT_CATEGORIES` — canonical category groupings for
  HTTP mapping (503-retryable vs 504) so services stop hand-rolling exception lists.
- **`dbkit.integrations.fastapi`**: `install_exception_handlers(app)` maps `DatabaseError` by
  category to RFC 7807 responses — 503 + `Retry-After` for overload, 504 for timeouts, 500 for
  bugs. New optional extra: `dbkit-sql[fastapi]` (Starlette only, imported lazily).
- `py.typed` marker — mypy-strict consumers now get real types instead of `Any`.
- **`Query.settings`** — per-query, transaction-local PostgreSQL settings (e.g.
  ``{"jit": "off"}``, ``{"work_mem": "64MB"}``) applied via parameterized
  ``set_config(name, value, true)`` right before the statement; names validated, values bound,
  never leaks past the statement's transaction.
- **`dbkit.testing`**: `FakeAsyncDatabase` / `FakeDatabase` — in-memory test doubles that
  record every call (name, statement, params, target, settings, in_transaction) and return
  queued rows; dynamic-registration tracking included.
- **`max_databases`** config — LRU cap on dynamically registered databases: beyond it the
  least-recently-ensured dynamic database is fully purged (engines, limiter, breakers,
  selector entry). Static databases are never evicted.
- `install_exception_handlers(..., database=db)` — the 503 ``Retry-After`` header now derives
  from the circuit breaker's ``open_seconds`` when a facade is provided.
- Docs: new "Dynamic Registration" guide; FastAPI/`Query.settings` sections in the
  integrations reference; `dbkit.testing` section in Testing & Benchmarks.

### Fixed
- ``observability.metrics: true`` (the default) now actually wires a Prometheus sink when
  ``prometheus_client`` is installed — previously the default was silently no-op. The sink is a
  process-wide singleton so multiple facades share collectors safely.
- Dynamic registration now enforces the process-wide connection budget at admission time
  (``connection_budget.maximum_per_process`` + ``enforce_at_startup``) — a runtime-registered
  database can no longer push the process past its configured ceiling.

## [0.1.0] - 2026-07-15

First public release on PyPI as `dbkit-sql` (import name: `dbkit`).

### Added — real load-test evidence for the performance review
- Six new benchmark/test scripts run against live PostgreSQL, replacing estimates with measured
  results: `benchmarks/bench_concurrency_scaling.py` (concurrency 1→500, both pool configs —
  found throughput plateaus at ~3.6-3.8k ops/s regardless of pool capacity 15 vs. 100, correcting
  this review's earlier "pool is the first bottleneck" assumption), `benchmarks/
  bench_observability_overhead.py` (OTel tracing +3.2%, Prometheus +5.6%, OTel metrics +0.5% p50
  vs. baseline), `benchmarks/bench_streaming_scale.py` (max RSS growth +3.6MB across 1M/5M-row
  narrow/wide streams), `benchmarks/bench_shard_cardinality.py` (new finding below), and two new
  integration tests: `test_deadlock_storm_is_classified_and_recovers_via_manual_retry` and
  `test_retry_storm_against_intermittently_killed_backends_mostly_recovers` (both in
  `tests/integration/test_resilience_scenarios.py`), plus
  `tests/integration/test_multiprocess_connection_budget.py` — validates the connection-budget
  formula (`per_process * app_replicas`) against 4 real OS subprocesses at full pool utilization:
  exactly the predicted 20 connections observed.
- **New finding, documented (not a bug)**: under load-testing at high shard cardinality, a
  concurrency level *exceeding* `max_engines` causes the engine-cache to thrash continuously and
  transiently exceed PostgreSQL's `max_connections` — the LRU cache bounds steady-state engine
  count but not concurrent engine-creation-in-flight events. Confirmed via detailed exception
  capture that this always surfaces as a classified `DatabaseConnectionError`, never a hang.
  Operational guidance: size `max_engines` to comfortably exceed expected *concurrent*
  distinct-shard fan-out, not just total shard cardinality.

### Added — deep performance review fixes
- **`benchmarks/bench_unnest.py`** (new): the `unnest()` bulk-insert strategy previously had no
  committed benchmark at all backing its documented "~32× faster than `execute_many`" claim.
  Running this new benchmark repeatedly against live PostgreSQL shows ~29× in steady state at
  20,000 rows (a first/cold run measured closer to ~20× — real run-to-run variance, now stated
  honestly instead of a single point estimate). Registered in `python -m benchmarks --only
  unnest`.
- Fixed a genuine gap in the `unasync` code generator surfaced while building the above: the
  `TOKENS` table had no mapping for `contextlib.AsyncExitStack`/`enter_async_context` — any new
  async code using it would silently generate broken sync code (`'AsyncExitStack' object does
  not support the context manager protocol`), only caught because the sync integration suite
  actually ran against a live database. Added the missing token mapping and extended
  `tests/unit/test_unasync_translation.py`'s fixture to cover this construct going forward.

### Added — closing every remaining production-readiness review finding
- **`dbkit metrics`** (new CLI command, requires the `prometheus` extra): runs one health probe
  and prints the resulting metric values in Prometheus text format. Documented as a wiring
  smoke test, not a live incident-triage snapshot — a CLI invocation is a fresh process with an
  empty metrics registry, so it cannot read an already-running application's live counters.
- **`AsyncDatabase.drain_engine(key)` / `Database.drain_engine(key)`** (new, both frontends):
  force-disposes one named engine's idle pooled connections by its `pool_status()` key, so the
  next call routed to it rebuilds fresh connections — useful right before a planned failover.
  Deliberately not exposed as a CLI command (same live-process-boundary reasoning as `dbkit
  metrics`); call it from your own admin endpoint or signal handler instead.
- **`DbkitConfig.budget_enforcement_warnings()` / `tls_warnings()`** (new): `dbkit check`/
  `config-validate` now print a `[WARNING]` when `environment != "development"` and either no
  connection budget is configured/enforced, or a target's DSN has no explicit `sslmode`/`ssl`
  parameter. Purely informational — never fails the command, and the enforcement defaults
  themselves are unchanged.
- **`examples/streaming_checkpoint_resume.py`**: a keyset-checkpoint (`WHERE id > :last_id ORDER
  BY id`) resume pattern for `db.stream()`, run against real PostgreSQL with a simulated
  mid-stream crash — the next attempt resumes from the last durable checkpoint, not from
  scratch, while being explicit about the reprocessing window checkpoint granularity implies.
- **`benchmarks/bench_pool_exhaustion.py`**: verifies that demand beyond `pool.size +
  max_overflow` fails fast with a classified `DatabasePoolTimeoutError`, not a hang.
- **`benchmarks/bench_pgbouncer_compatible.py`**: measures the latency cost of
  `pgbouncer_compatible=True` (disabling driver autoprep) — noise-level on a sub-millisecond
  query in this benchmark's runs.
- **`.raw` escape hatch**: docstring and `docs/api/database.md` now state plainly that using it
  opts a statement out of classification/metrics/tracing/retry entirely.
- The CLI's `dbkit metrics`/lack of a `drain-engine` command/lack of a slow-query-log command are
  now each explicitly reasoned about in `docs/observability.md`/`docs/troubleshooting.md` rather
  than left as unexplained gaps — the CLI is architecturally a fresh, separate process per
  invocation with no channel into an already-running application's live state.

### Added — "before stable 1.0" production-readiness review items
- **Idempotent-write guard rail** (`_core/idempotency_lint.py`): `dbkit query-list` now warns
  on registered writes marked `idempotent=True` whose SQL text has no visible `ON CONFLICT`/
  `WHERE NOT EXISTS`/`MERGE` guard — a best-effort static nudge, not a gate (dbkit cannot see
  your schema's unique constraints). `Query.idempotent` and `RetryConfig` docstrings now state
  explicitly that these flags are trust-based, not verified.
- **Redaction hint-list documented and boundary-tested**: expanded
  `is_sensitive_key`'s hint list with unambiguous PII/payment fragments (`credit_card`,
  `card_number`, `cvv`, `iban`, `dob`, `date_of_birth`, `national_id`, `pin`); the exact catch/
  miss boundary is now a tested contract
  (`tests/property/test_invariants.py::test_hint_list_boundary_is_documented_and_tested`) and
  documented in the new `docs/security.md`.
- **`docs/observability.md`**: an example Grafana dashboard (importable JSON) and Prometheus
  alerting rules (commit-unknown, circuit-open, pool-wait, rollback-rate, connection-hold) for
  every metric dbkit emits, including the new `db_circuit_breaker_state` gauge.
- **A real primary-failover chaos test** (not just a same-instance restart):
  `test_recovers_after_primary_failover_to_a_different_backend` runs two throwaway PostgreSQL
  containers behind an in-process TCP proxy, kills the one currently in use, repoints the
  proxy, and confirms recovery lands on the genuinely different backend (verified via a marker
  row seeded into each). Uses the raw `docker` CLI rather than `testcontainers`, since
  `testcontainers`' Ryuk reaper hits a host-mount issue on some Docker Desktop setups — the
  same reason the existing restart test is environment-sensitive.
- **`docs/troubleshooting.md`**: a symptom → cause → fix guide covering retry/idempotency,
  commit-unknown, pool exhaustion, connection budgets, PgBouncer, asyncpg limitations, the
  circuit breaker, and read-your-writes across a thread boundary.
- **`docs/testing.md`**: documents `tools/run_unasync.py`'s transformation rules and scope
  (what token-substitution can and can't handle, and why `_compat.py` exists), backed by a new
  translation-completeness smoke test (`tests/unit/test_unasync_translation.py`) that feeds the
  transform a deliberately awkward async fixture and asserts the exact expected sync output —
  catching a *silent* mistranslation that `--check` alone cannot (it only proves regeneration
  is deterministic, not that the rules are complete).
- **A 10-minute soak** (`benchmarks.soak --duration 600 --kill-every 45`, up from the 60s CI
  gate): ~120K confirmed inserts, 0 recovery failures, RSS/FD/task growth all bounded — real
  evidence beyond the CI smoke gate (a true multi-hour run remains a deployment-time exercise).
  **`benchmarks/bench_batch_collector.py`**: `BatchCollector.add()` latency at up to 2000
  concurrent producers — throughput holds at ~2M items/s with P99 latency ~0.001ms even at
  2000-way fan-in, refuting the review's "unconfirmed lock-contention ceiling" concern.
- **`docs/versioning.md`**: SemVer policy, what "stable 1.0" requires, and the post-1.0
  deprecation window.
- **`ResilientExecutor`** (`_async/executor.py`, new): extracted the concurrency-limiter +
  circuit-breaker + retry-loop + connection-acquisition orchestration out of `AsyncDatabase`
  into a focused collaborator, mirroring how bulk writes/streaming/transactions already live in
  their own modules — `AsyncDatabase` is now a thinner dispatcher. Zero behavior change,
  regression-tested on both frontends and both drivers.
- **Partitioned inbox table** (`dbkit.integrations.partitioned_inbox_ddl`/
  `inbox_month_partition_ddl`) and a **poison-message attempt-counting example**
  (`examples/inbox_idempotent_consumer.py`) showing why the attempt counter must be a
  standalone, immediately-committed write (an inbox claim rolls back along with the rest of
  the transaction when `work()` fails, so it can't itself count failed attempts) — routing to
  a dead-letter path after a configurable attempt limit, layered on top of
  `ack_after_commit`'s own per-attempt `DatabaseError` routing.
- `db.consistency_scope()`'s docstring and `docs/troubleshooting.md` now document that the
  read-your-writes override doesn't cross a thread boundary (`contextvars` aren't inherited by
  new OS threads), with the fix (capture `contextvars.copy_context()`, or target the primary
  explicitly for that read).

### Fixed — production-readiness review findings
- **`db.execute()`'s auto-commit path could leak a raw, unclassified exception and silently
  skip commit-unknown detection.** A connection failure during the implicit `COMMIT` (e.g. the
  server commits but the client never sees the ack) now raises `DatabaseCommitUnknownError`
  exactly like an explicit `db.transaction()` does, and any other commit failure is now run
  through `classify()` instead of propagating a raw SQLAlchemy/driver exception. Extracted the
  shared `is_connection_error()` check to `_core/errors/classifier.py` so both the transaction
  and one-shot-write paths use the same logic.
- **Concurrency-limiter tiers (`ConcurrencyConfig.reads`/`writes`/`bulk_writes`/`database`) had
  no acquire timeout.** A saturated tier queued callers indefinitely, invisible to dbkit's own
  timeout/deadline machinery. `ConcurrencyLimiter.acquire()` now bounds the wait by the same
  effective timeout the query itself would get, raising `DatabaseOverloadedError` (an
  already-declared but previously unused error class) on saturation. Bulk/COPY paths are
  intentionally left unbounded — they're expected to be slower and have their own retry story.
- **No visibility into circuit breaker state.** Added the `db_circuit_breaker_state` gauge
  (0=closed, 1=half_open, 2=open), emitted on every `CircuitBreaker.state()` check/transition —
  the signal an on-call engineer needs to answer "is the breaker open right now."
- **`postgres/copy.py`'s driver guard could misfire under asyncpg.** It checked for a `cursor`
  attribute to detect psycopg, but asyncpg's raw connection also has a `cursor` method
  (incompatible signature/purpose), so COPY against asyncpg failed with a confusing raw
  `TypeError` instead of the intended `DatabaseUnsupportedOperationError`. Now checks for
  `pipeline` instead (psycopg-only, matching `postgres/pipeline.py`'s own guard).
- Verified (and regression-tested) that engine LRU eviction is safe under concurrent use:
  SQLAlchemy's `Engine.dispose()` only closes idle pooled connections, so a connection already
  checked out from an evicted engine keeps working until the caller closes it.

### Added — asyncpg CI coverage
- New `integration-asyncpg` CI job runs the async-only integration/chaos/sharding/CLI suite
  against asyncpg (previously untested in CI). Sync-only tests and the psycopg-only COPY/
  pipeline/PgBouncer-autoprep tests self-skip via a new `requires_psycopg` fixture when the
  configured driver isn't psycopg, rather than being silently excluded.

### Added — full API reference docs
- `mkdocstrings[python]` wired into the docs site (`docs/api/*.md`): every public class/method
  across the facade, config, query/routing, errors, observability, and integrations modules is
  now rendered from its docstring, always in sync with the code.
- Filled in every previously-undocumented public method/class across the codebase (config
  validation, error classes, shard/replica resolvers, the async/sync facades, transaction and
  connection scopes, engine registry, health checks, streaming, the circuit breaker, pool
  instrumentation, the CLI, and the OTel/Prometheus metrics adapters).

### Added — full OpenTelemetry options (traces, metrics, log correlation)
- Tracing spans now carry `kind=SpanKind.CLIENT`, per the OTel semantic conventions for
  database client operations (previously defaulted to `INTERNAL`).
- `Tracer`/`make_tracer()` accept `tracer_provider`, `schema_url`, and `attributes` — the same
  parameters `opentelemetry.trace.get_tracer()` itself takes — so an application can bind
  dbkit's tracer to a specific (non-global) `TracerProvider`, e.g. for per-tenant tracing or
  test isolation, instead of always using the global one. The tracer now identifies itself as
  the `dbkit` instrumentation scope with its real package version, instead of the previous
  `service_name` parameter (which conflated the instrumentation-scope name with the
  application's own service name — that belongs in the app's `Resource`, not here).
- Structured log events now carry `trace_id`/`span_id` from the currently active OTel span
  (`observability/logging.py`), so a log line can be joined back to the trace that produced it.
  Absent (no extra keys) when OTel isn't installed or no span is active — same
  no-op-by-default posture as the rest of observability.
- `observability.otel_metrics.OTelMetrics` / `try_otel_metrics_sink()`: a `MetricsSink`
  implementation routing dbkit's counters/histograms/gauges through
  `opentelemetry.metrics` instead of Prometheus — an alternative for deployments that export
  metrics via OTLP rather than scraping. Requires `opentelemetry-api>=1.26` (for the
  synchronous Metrics `Gauge` instrument).
- Fixed a hot-path cost in structured logging: the trace/log correlation lookup now checks
  OTel availability once at import time instead of attempting `import opentelemetry` on every
  log call — a real cost when OTel isn't installed, since failed imports aren't cached in
  `sys.modules` and would otherwise re-walk `sys.path` on every query/transaction log event.
- `examples/opentelemetry_observability.py`: wires a real OTel SDK (`TracerProvider` +
  `MeterProvider`, both application-owned and injected into dbkit) and prints finished spans,
  collected metrics, and log lines carrying the matching `trace_id`/`span_id`.

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
  PostgreSQL's 65535 bind-parameter ceiling — ~~~32× faster than `execute_many` at 20k rows
  in-benchmark~~ **correction (see "Unreleased" above): this figure had no committed benchmark
  behind it at all; `benchmarks/bench_unnest.py` now measures it for real — ~29× in steady
  state across repeated runs at 20,000 rows (a first/cold run measured closer to ~20×).** Each
  column is cast with `CAST(:name AS type[])` — SQLAlchemy's `text()`
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
