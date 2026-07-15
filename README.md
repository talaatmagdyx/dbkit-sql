<p align="center"><img src="https://raw.githubusercontent.com/talaatmagdyx/dbkit-sql/main/docs/assets/logo.svg" alt="dbkit" width="120"></p>

# dbkit

**SQLAlchemy Core made enjoyable — less pool/retry plumbing, more SQL that matters.**

[![PyPI](https://img.shields.io/pypi/v/dbkit-sql)](https://pypi.org/project/dbkit-sql/)
[![CI](https://github.com/talaatmagdyx/dbkit-sql/actions/workflows/ci.yml/badge.svg)](https://github.com/talaatmagdyx/dbkit-sql/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)](https://github.com/talaatmagdyx/dbkit-sql/blob/main/pyproject.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/talaatmagdyx/dbkit-sql/blob/main/LICENSE)
[![Typed](https://img.shields.io/badge/types-mypy%20--strict-blue)](https://github.com/talaatmagdyx/dbkit-sql/blob/main/pyproject.toml)
[![Style](https://img.shields.io/badge/style-ruff-261230)](https://github.com/talaatmagdyx/dbkit-sql/blob/main/pyproject.toml)

dbkit is a **SQL-first toolkit for Python services built on top of SQLAlchemy Core**. It gives
you one API for sync and async, bounded connection pools, idempotency-gated retries, a circuit
breaker, sharding and replica routing, bounded-memory streaming, adaptive-batch bulk writes, and
structured observability — so your team can focus on what each query *does*, not on rebuilding
pool sizing, retry loops, and error classification in every service.

SQLAlchemy Core is powerful and deliberately unopinionated about all of that. dbkit adds the
missing layer **without hiding SQLAlchemy from you** — no ORM, no sessions, no query builder
magic standing between you and the SQL you wrote.

```python
from dbkit import AsyncDatabase, DatabaseTarget, Query, sql

db = AsyncDatabase.from_config({
    "databases": {"app": {"primary": {"url": "postgresql+psycopg://localhost/app"}}},
})

GET_USER = Query(
    name="users.get_by_id",
    statement=sql("SELECT id, email FROM users WHERE id = :id"),
    operation="read", idempotent=True, timeout=1.0,
)

async def main() -> None:
    await db.start()
    user = await db.fetch_optional(GET_USER, {"id": 1}, target=DatabaseTarget(database="app"))
```

That should feel like application code. The pool sizing, retry/backoff policy, error
classification, transaction lifecycle, and test infrastructure should not be rewritten in every
service that talks to Postgres. **That is what dbkit is for.**

**Contents:**
- [What dbkit believes](#what-dbkit-believes)
- [Why dbkit exists](#why-dbkit-exists)
- [Install](#install)
- [Quick start](#quick-start)
- [Consistency & retry model](#consistency--retry-model)
- [What happens when things fail](#what-happens-when-things-fail)
- [Retry & concurrency policy knobs](#retry--concurrency-policy-knobs)
- [Production profile](#production-profile)
- [Observability](#observability)
- [Sharding & replica routing](#sharding--replica-routing)
- [Building blocks](#building-blocks-batteries-included)
- [Operate it from the terminal](#operate-it-from-the-terminal)
- [Where dbkit fits](#where-dbkit-fits)
- [Performance](#performance)
- [Migrating from raw SQLAlchemy Core](#migrating-from-raw-sqlalchemy-core)
- [Examples](#examples)
- [Architecture](#architecture)
- [Compatibility](#compatibility)
- [Documentation](#documentation)
- [Contributing & security](#contributing--security)
- [License](#license)

---

## What dbkit believes

Most services that talk to PostgreSQL need the same things:

- one API whether the service is sync or async
- a connection pool sized on purpose, not by accident
- retries that only ever touch operations known to be safe to repeat
- errors classified by what actually happened, not a raw driver exception
- a circuit breaker so a downed database doesn't get hammered
- streaming and bulk paths that don't buffer a million rows in memory
- structured logs/metrics/traces with secrets redacted by default
- tests that run against a real database, because SQL correctness is not
  something an in-memory fake can honestly stand in for

dbkit packages those concerns into one focused toolkit. The philosophy:

> **Make SQLAlchemy Core pleasant for developers and predictable for operators.**

Developers get a clean, typed query/config model. Operators get classified errors, bounded
resources, and visibility into what the pool and the retry policy are actually doing.

## Why dbkit exists

Starting with SQLAlchemy Core is easy: `engine.connect()`, `conn.execute(text(...))`. Then
production asks better questions:

- What happens if the client gives up mid-query — does the server keep running it anyway?
- Is that retry actually safe, or did it just duplicate a write?
- Did the connection die *during* commit — did the write happen or not?
- Can 500 concurrent requests open 500 connections and take Postgres down with them?
- Can a shard-routing config balloon connection count in ways nobody sized for?
- Can CI catch a broken transaction rollback without a live Postgres container?

dbkit exists for those questions. Its goal is not to replace SQLAlchemy — it's to make direct
SQLAlchemy Core usage feel like good application code: bounded pools, explicit transactions,
classified failures, real tests, production-ready lifecycle.

**dbkit is:**

- a SQL-first toolkit over SQLAlchemy Core
- a resilience layer (retries, circuit breaker, concurrency limits)
- a sharding/replica-routing layer for multi-database deployments
- a high-throughput layer (streaming, bulk insert/upsert, PostgreSQL COPY)
- an observability layer (structured logs, Prometheus, OpenTelemetry)

**dbkit is not:**

- an ORM — no models, sessions, or relationship loading
- a schema-migration tool — pair it with Alembic
- a message broker client — the transactional-inbox helpers are primitives your
  own consumer loop wires in, not a broker integration
- an exactly-once delivery system — retries are at-least-once, gated by `idempotent=True`

---

## Install

Available on PyPI: **[pypi.org/project/dbkit-sql](https://pypi.org/project/dbkit-sql/)**

```bash
pip install "dbkit-sql[psycopg]"                       # async + sync PostgreSQL
pip install "dbkit-sql[psycopg,yaml,prometheus,otel,cli]"
```

Requires Python ≥ 3.11. The distribution is `dbkit-sql` (the name `dbkit` was already taken on
PyPI) — the import stays `import dbkit` regardless of which extras you install.

## Quick start

### 1. Connect and query

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

### 2. Explicit transactions with real isolation control

```python
async with db.transaction(target=DatabaseTarget(database="app"), isolation="serializable") as tx:
    await tx.execute(sql("UPDATE accounts SET balance = balance - :amt WHERE id = :from_id"),
                      {"amt": 100, "from_id": 1})
    await tx.execute(sql("UPDATE accounts SET balance = balance + :amt WHERE id = :to_id"),
                      {"amt": 100, "to_id": 2})
```

A deadlock or serialization failure here surfaces as a classified, `retryable`
`DatabaseDeadlockError`/`DatabaseSerializationError` — dbkit never auto-retries an explicit
transaction body (that would mean silently re-running your code), so you catch and retry at the
call site, same as any other classified error.

### 3. Retries and a circuit breaker, configured once

```python
db = AsyncDatabase.from_config({
    "databases": {"app": {"primary": {"url": "postgresql+psycopg://localhost/app"}}},
    "defaults": {
        "retry": {"attempts": 3, "retry_reads": True, "maximum_total_ms": 750},
        "circuit_breaker": {"enabled": True, "failure_threshold": 5, "open_seconds": 30},
    },
})
```

Reads marked `idempotent=True` retry transparently on transient failures (serialization
failures, deadlocks, connection drops). Writes only retry if you explicitly opt in with
`retry_writes=True` *and* mark the query `idempotent=True` — dbkit will not guess.

### 4. Test against a real, disposable PostgreSQL

dbkit doesn't ship an in-memory fake database — SQL correctness (isolation levels, deadlocks,
constraint violations, `SELECT ... FOR UPDATE`) is genuinely a property of the real engine, not
something worth faking. Instead, point tests at a disposable container:

```python
import pytest
from dbkit import AsyncDatabase

@pytest.fixture
async def db(pg_dsn):
    database = AsyncDatabase.from_config({"databases": {"app": {"primary": {"url": pg_dsn}}}})
    await database.start()
    yield database
    await database.close()
```

`docs/testing.md` covers the `testcontainers`-based fixture this project's own suite uses.

### 5. Wire into any framework's lifespan

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.start()
    try:
        yield
    finally:
        await db.close()

app = FastAPI(lifespan=lifespan)
```

### Sync example

```python
from dbkit import Database
db = Database.from_config(...)
db.start()
user = db.fetch_optional(GET_USER, {"id": 1}, target=DatabaseTarget(database="app"))
db.close()
```

The sync API is generated from the async one (`tools/run_unasync.py`), so the two never drift —
fits simple workers, scripts, and teams that don't want an asyncio runtime.

---

## Consistency & retry model

dbkit is an **at-least-once** toolkit for retried operations: a network blip right at commit
means the outcome is genuinely unknown, not silently "it worked." dbkit never guesses in that
case — it raises a distinct `DatabaseCommitUnknownError` instead of reporting success or
silently retrying a write that may have already landed.

For anything where a duplicate write would be a real problem: mark the query `idempotent=True`
only when it actually is (`ON CONFLICT`, `WHERE NOT EXISTS`, `MERGE` — the idempotency lint
flags `INSERT`s missing an obvious guard), and keep `retry_writes` off for anything that isn't.
The rule is simple:

> dbkit can help you retry safely. Your write must still be safe to retry.

## What happens when things fail?

| Failure mode | dbkit behavior |
|---|---|
| Client times out mid-query | The driver sends a real server-side cancel — the abandoned query does not keep running (verified against both psycopg3 and asyncpg) |
| Deadlock (`40P01`) | Classified `DatabaseDeadlockError`, `retryable=True`; explicit transactions are not auto-retried — you catch and retry |
| Serialization failure (`40001`) | Classified `DatabaseSerializationError`, transparently retried for idempotent reads |
| Connection dies mid-commit | `DatabaseCommitUnknownError` — never reported as success, never silently retried |
| Pool exhausted | `DatabasePoolTimeoutError` within `timeout_seconds` — a queued wait, not a hang |
| Concurrency-limiter tier saturated | `DatabaseOverloadedError` — fails fast instead of queueing forever |
| Sustained backend failures | Circuit breaker opens; fails fast with `DatabaseCircuitOpenError` instead of hammering a downed database |
| Unique/FK/check constraint violated | `DatabaseUniqueViolationError` / `DatabaseForeignKeyViolationError` / `DatabaseCheckViolationError` — distinct, catchable types |
| Bulk write partially fails (`best_effort`) | Dropped rows/batches are logged and counted (`db_bulk_rows_dropped_total`), never silently discarded |
| High concurrency vs. a small shard-engine cache | Real connection exhaustion, but always the classified `DatabaseConnectionError` — never a hang |

## Retry & concurrency policy knobs

| Knob | What it controls |
|---|---|
| `retry.retry_reads` / `retry.retry_writes` | Whether reads/writes are eligible for automatic retry at all |
| `Query(idempotent=True)` | The gate that actually authorizes a write to be retried — trust-based, lint-assisted |
| `retry.attempts` / `retry.maximum_total_ms` | A hard ceiling on retry attempts *and* total elapsed retry time, whichever hits first |
| `circuit_breaker.failure_threshold` / `open_seconds` | When the breaker trips, and how long it stays open before a trial request |
| `concurrency.*` tiers (`reads`/`writes`/`bulk`/`expensive`) | Independent semaphores acquired before pool checkout — an expensive report query can't starve ordinary reads |
| `pool.size` / `max_overflow` / `timeout_seconds` | The connection budget per database×shard×role, and how long a caller waits for one |

## Production profile

The recommended baseline: `connection_budget.enforce_at_startup=True` so a misconfigured pool
fails at boot instead of exhausting Postgres under load; `max_engines` sized to comfortably
exceed your expected *concurrent* distinct-shard fan-out (not just total shard count — see
`benchmarks/bench_shard_cardinality.py` for why those are different numbers) with
`evict_lru=True` for dynamic per-tenant topologies; `retry_writes` scoped only to genuinely
idempotent queries; TLS/`sslmode` checked via `dbkit check` before every environment promotion.
See [`docs/security.md`](docs/security.md) and [`docs/troubleshooting.md`](docs/troubleshooting.md)
for the full posture and common failure modes.

## Observability

Structured logs carry operation context (query name, database/shard/role, duration, outcome,
retry attempt) with secret redaction on by default. Metrics cover retries, circuit-breaker
state, pool utilization, transaction outcomes (including the distinct commit-unknown case), and
bulk-write drops — both a Prometheus and an OpenTelemetry metrics sink are supported, at a
measured, not assumed, cost (see [Performance](#performance)). Tracing is standard
OpenTelemetry (`pip install dbkit-sql[otel]`), one continuous span tree per operation.

## Sharding & replica routing

```python
from dbkit import AsyncDatabase, DatabaseTarget, HashShardResolver

db = AsyncDatabase.from_config(
    {
        "databases": {"app": {"primary": {"url": "..."}, "replicas": [{"name": "r1", "url": "..."}]}},
        "max_engines": 200,
        "evict_lru_engines": True,
    },
    shard_resolver=HashShardResolver(num_shards=64),
)

target = DatabaseTarget(database="app", role="read", shard_key=f"tenant-{tenant_id}")
```

Pluggable resolvers (hash / range / directory / callable), replica routing with a
read-your-writes override (pins reads to the primary for the scope — not lag-aware replica
tracking), and LRU engine eviction bounded by `max_engines` so a dynamic or unbounded shard-key
space still keeps a bounded number of live connections. No cross-shard transactions — pair with
an outbox/saga pattern for multi-shard writes.

### Dynamic registration (0.2)

Databases whose DSNs are discovered at runtime (per-tenant shards from a service registry,
rotated credentials) register on the fly — no application-side engine registry:

```python
db = AsyncDatabase.from_config({"databases": {}, "max_databases": 500})  # dynamic-first

await db.ensure_database(shard, {"primary": {"url": dsn}})   # lock-free no-op when unchanged
rows = await db.fetch_all(query, params, target=DatabaseTarget(database=shard, role="read"))
```

Changed configs (host moves, password rotations) re-register in place with old engines
disposed; `max_databases` LRU-evicts idle dynamic shards; registration respects the
process-wide connection budget. See the
[Dynamic Registration guide](https://talaatmagdyx.github.io/dbkit-sql/dynamic-registration/).

## Building blocks, batteries included

| Building block | Job |
|---|---|
| `RetryConfig` | Idempotency-gated retries with exponential backoff + jitter and a hard total-time ceiling |
| `CircuitBreaker` | Per database×shard×role blast-radius containment against sustained infra failures |
| `ConcurrencyLimiter` | Independent semaphore tiers (reads/writes/bulk/expensive) acquired before pool checkout |
| `db.stream()` | Server-side-cursor streaming — bounded memory regardless of result-set size (measured, not assumed) |
| `insert_many` / `upsert_many` | Adaptive-batch bulk writes, `max_payload_bytes`-aware |
| `unnest()` bulk strategy | Array-parameter bulk insert/upsert — ~29× a naive `execute_many` loop |
| `db.copy_records` | PostgreSQL COPY for the largest ingest jobs |
| Transactional inbox | Effectively-once consumer processing — dedup and business write share one transaction |
| PgBouncer-compatible pooling | A mode for transaction-pooling PgBouncer deployments |
| psycopg pipeline mode | A raw escape hatch for pipelined round-trips, opt-in |
| `PrometheusMetrics` / OTel metrics & tracing | Structured observability, cost measured per adapter |

## Operate it from the terminal

```bash
pip install "dbkit-sql[cli]"
dbkit config-validate config.yaml     # schema + connection-budget validation
dbkit check config.yaml               # validate + full readiness check (TLS, budget, connectivity)
dbkit pools config.yaml               # warm a connection, print pool status
dbkit engines config.yaml             # live engine registry snapshot
dbkit connection-budget config.yaml   # projected cluster-wide connection budget
dbkit metrics config.yaml             # one-shot Prometheus text-format snapshot
```

See [`docs/cli.md`](docs/cli.md) for the full command reference.

## Where dbkit fits

dbkit sits *above* SQLAlchemy Core — a resilience and ergonomics layer, not a replacement; drop
to `.raw`/the underlying `Connection` any time. It's for teams that use SQLAlchemy Core (or raw
SQL) directly and want production-safe database access without rebuilding pooling policy,
retries, sharding, and observability in every service.

**A good fit when:**

- PostgreSQL is your primary database and you don't want an ORM
- a network blip mid-write must never turn into an ambiguous outcome
- retry and circuit-breaker behavior must be explicit, not implicit in a driver
- you run more than one database, shard, or read replica
- operators need visibility into pool/retry/circuit-breaker state

**Do NOT use dbkit when:**

- **You need an ORM.** Relationship loading, unit-of-work sessions, and a model layer are
  explicitly out of scope — pair a real ORM with dbkit's config/resilience layer instead, or
  use the ORM alone.
- **You need schema migrations.** Pair dbkit with Alembic or your migration tool of choice.
- **You need a message-broker client.** The inbox/batching helpers are primitives your own
  consumer loop wires in — dbkit does not talk to RabbitMQ, Kafka, or SQS.
- **You need exactly-once delivery.** Nothing over PostgreSQL gives you that, including dbkit;
  the idempotency gate gives idempotent *processing*, a different (weaker, honest) guarantee.
- **You need dialects beyond PostgreSQL/psycopg/asyncpg covered by CI.** Other
  SQLAlchemy-supported dialects may work, but aren't tested here.

## Performance

Measured against real PostgreSQL, not estimated — see the benchmark scripts under
`benchmarks/` for methodology and to reproduce:

| Metric | Result | Script |
|---|---|---|
| Concurrency scaling (simple indexed read) | Plateaus ~3.6–3.8k ops/s at concurrency ≥ 10, regardless of pool capacity (15 vs. 100) | `bench_concurrency_scaling.py` |
| Overhead vs. raw SQLAlchemy Core | ~19–22% (session-measured), gated at 40% in CI | `bench_overhead.py`, `check_regression.py` |
| `unnest()` bulk insert vs. `execute_many` | ~29× at steady state, 20,000 rows | `bench_unnest.py` |
| OTel tracing / Prometheus / OTel metrics overhead | +3.2% / +5.6% / +0.5% p50 vs. no observability | `bench_observability_overhead.py` |
| Streaming 1M–5M rows, narrow + wide | RSS growth bounded to +3.6MB regardless of row count | `bench_streaming_scale.py` |
| Connection-budget formula vs. real processes | 4 real OS processes at full pool utilization matched the predicted connection count exactly | `test_multiprocess_connection_budget.py` |

Absolute numbers vary by machine — the ratios and the "bounded, not unbounded" shape are the
stable part. What isn't yet measured: a real multi-node production topology and multi-day
sustained load — every number above comes from a single-node test instance.

## Migrating from raw SQLAlchemy Core

dbkit wraps `create_async_engine`/`create_engine`, so migration is incremental, not a rewrite:

- `engine.connect()` + hand-rolled retry loop → `AsyncDatabase.fetch_*`/`execute` with
  `RetryConfig` — the retry loop, backoff, and idempotency gate disappear from your code.
- Manual `try/except` on driver exceptions → classified `DatabaseError` subclasses
  (`DatabaseDeadlockError`, `DatabaseUniqueViolationError`, …) — catch what actually happened.
- A single hard-coded `engine` per database → named databases, shard resolvers, and replica
  routing via one `DbkitConfig`.
- Raw `conn.execute(text(...))` still works via `.raw` — dbkit is additive, not a cage.

## Examples

**[`examples/`](examples/)** — 16 runnable, idempotent scripts covering every feature, plus a
`run_all.py` that runs them all safely in sequence:

| Want to… | Example |
|---|---|
| See the smallest working consumer | [`quickstart_async.py`](examples/quickstart_async.py) · [`quickstart_sync.py`](examples/quickstart_sync.py) |
| Use explicit transactions & savepoints | [`transactions_savepoints.py`](examples/transactions_savepoints.py) |
| Handle classified errors | [`error_handling.py`](examples/error_handling.py) |
| Wire retries + the circuit breaker | [`retries_and_circuit_breaker.py`](examples/retries_and_circuit_breaker.py) |
| Stream millions of rows | [`streaming.py`](examples/streaming.py) · [`streaming_checkpoint_resume.py`](examples/streaming_checkpoint_resume.py) |
| Bulk insert/upsert and COPY | [`bulk_insert_upsert.py`](examples/bulk_insert_upsert.py) · [`copy_ingest.py`](examples/copy_ingest.py) |
| Effectively-once consumer processing | [`inbox_idempotent_consumer.py`](examples/inbox_idempotent_consumer.py) · [`batch_collector.py`](examples/batch_collector.py) |
| Check health/pool state | [`health_and_pool.py`](examples/health_and_pool.py) |
| Wire OpenTelemetry | [`opentelemetry_observability.py`](examples/opentelemetry_observability.py) |
| Confirm sync/async parity | [`sync_feature_parity.py`](examples/sync_feature_parity.py) |

```bash
export DBKIT_DSN=postgresql+psycopg://localhost/postgres
python examples/quickstart_async.py
python examples/run_all.py            # runs every example, safe to repeat
```

## Architecture

```
dbkit/
  _core/          # config, query/routing, errors, policies, bulk, circuit — pure logic, no I/O
  _async/          # hand-written async runtime (database, connection, transaction, engine, resilience)
  _sync/           # generated from _async/ via tools/run_unasync.py — never edited by hand
  observability/   # structured logging, Prometheus, OpenTelemetry metrics/tracing
  postgres/        # COPY, pipeline mode, unnest() bulk strategy — Postgres-only escape hatches
  integrations/     # transactional inbox (dedup consumer), BatchCollector micro-batching
  cli/             # config-validate, check, pools, engines, connection-budget, metrics
```

The shared `_core/` has **zero** I/O — error classification, config, routing, and policy
decisions are pure and shared by both the sync and async frontends.

## Compatibility

Python 3.11+ (tested: 3.11 / 3.12 / 3.13) · PostgreSQL 16 in CI · SQLAlchemy 2.0.30+ ·
`psycopg[binary] >= 3.2` (default driver, the only one with a sync API — COPY and pipeline mode
also require it) · `asyncpg >= 0.29` (async-only, CI-covered for reads/writes/transactions,
resilience/chaos, sharding/replicas, and the CLI; no COPY/pipeline mode).

## Documentation

**📚 Full rendered docs: [talaatmagdyx.github.io/dbkit-sql](https://talaatmagdyx.github.io/dbkit-sql/)**

- [API reference](docs/api/) — `Database`/`AsyncDatabase`, config, query/routing, errors
- [CLI command reference](docs/cli.md)
- [Dashboards & alert rules](docs/observability.md) (Grafana/Prometheus)
- [Security posture](docs/security.md) — redaction, TLS, connection budgets, idempotency lint
- [Troubleshooting guide](docs/troubleshooting.md)
- [Testing, chaos, and benchmark commands](docs/testing.md)
- [Full spec](docs/requirements.md) and [phased delivery roadmap](docs/roadmap.md)
- [Versioning & deprecation policy](docs/versioning.md)

## Contributing & security

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the ground rules (no ORM, never hand-edit
`_sync/`, pure logic stays in `_core/`) and [`docs/testing.md`](docs/testing.md) for the local
quality gates (`ruff`, `mypy --strict`, the full test/integration suite) to run before opening a
PR. Found a real vulnerability? **Do not open a public issue** — follow
[`SECURITY.md`](SECURITY.md) to report it privately. See [`docs/security.md`](docs/security.md)
for the documented security posture.

## License

[MIT](LICENSE)
