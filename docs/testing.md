# Testing, chaos, and benchmarks

dbkit has five layers of verification. All commands go through `uv run` (see the `Makefile`).

## 1. Static gate (no database)

```bash
make unasync-check     # generated _sync is in lock-step with _async
make lint              # ruff check + format check (src, tests, benchmarks)
make type              # mypy --strict over src/dbkit
make test              # unit + property + security tests (no DB)
```

`make check` runs all of the above — this is the per-push CI gate.

## 2. Unit / property / security tests

- **Unit** (`tests/unit/`) — pure logic: config, budget math, routing, cardinality, mappers,
  error classification, timeout/backoff policy, circuit breaker, retry executor, concurrency
  limiter, and a dual-API structural check (the generated sync package has no `async`/`await`).
- **Property** (`tests/property/`, hypothesis) — invariants over all inputs: redaction never
  leaks, SQLSTATE classification is total, timeout resolution is a lower bound, backoff is
  bounded/monotonic, the connection budget equals the sum of pool ceilings.
- **Security** (`tests/security/`) — bare-string SQL is rejected, DSNs and sensitive params are
  redacted from errors/logs, and (integration) injection payloads are stored literally.

## 3. Integration tests (real PostgreSQL)

Point dbkit at a database with `DBKIT_TEST_DSN`, or let the suite start a `postgres:16`
container via `testcontainers` (requires Docker).

```bash
export DBKIT_TEST_DSN=postgresql+psycopg://user@localhost:5432/postgres
make integration       # all integration tests (async + sync frontends)
```

Every scenario runs against **both** `AsyncDatabase` and `Database`.

## 4. Chaos / resilience suite

`tests/integration/test_resilience_scenarios.py` induces real faults:

- backend termination mid-transaction (`pg_terminate_backend`) → classified error + recovery
- commit-unknown race (backend killed at COMMIT) → `DatabaseCommitUnknownError`
- serialization failure (SQLSTATE 40001) on an idempotent read → retried and succeeds
- circuit breaker opens under sustained connection failure → fast-fail
- cancellation storm → every connection returns to the pool
- bounded connections under concurrency; graceful shutdown under load
- **full server restart** (`docker restart`) → transparent recovery (requires Docker)

```bash
make chaos             # needs PostgreSQL; the restart test self-skips without Docker
```

## 5. Benchmarks and soak

A custom asyncio/monotonic harness (no external bench framework). Starts one container or
uses `--dsn` / `DBKIT_BENCH_DSN`.

```bash
make bench                                    # overhead, throughput, latency, batch
python -m benchmarks --only overhead --dsn "$DBKIT_TEST_DSN"
```

- **overhead** — A/B vs raw psycopg and raw SQLAlchemy Core (headline overhead %).
- **throughput** — ops/s for small reads and single-row inserts (async + sync).
- **latency** — open-loop paced P50/P95/P99.
- **batch** — `execute_many` vs per-row.

Results persist to `benchmarks/results/run_<ts>.json` and print a regression delta vs the
previous run. Metric keys encode direction (`*_ops_s` higher-better, `*_ms`/`*_pct` lower-better).

The **soak** applies sustained paced load with periodic fault injection and gates on
leak-free recovery (exit code = verdict):

```bash
make soak DURATION=120 KILL_EVERY=30          # or: python -m benchmarks.soak --dsn ...
```

Verdicts: made progress, recovered after every kill, and bounded RSS / FDs / asyncio tasks /
pool connections.

## Local Docker note

The chaos server-restart test and the testcontainers path require a reachable Docker daemon.
If Docker is unavailable locally, set `DBKIT_TEST_DSN` / `DBKIT_BENCH_DSN` to an existing
PostgreSQL — everything except the container-restart test runs against it, and that one test
self-skips. CI provides PostgreSQL as a service and runs the full suite.
