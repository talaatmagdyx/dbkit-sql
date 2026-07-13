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
