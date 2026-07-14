# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

## [Unreleased]

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
