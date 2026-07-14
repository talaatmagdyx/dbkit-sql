<p align="center">
  <img src="docs/assets/logo.svg" alt="dbkit logo" width="88" height="88">
</p>

<h1 align="center">dbkit</h1>

<p align="center">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License: Apache-2.0">
  <img src="https://img.shields.io/badge/python-3.11%2B-blue.svg" alt="Python 3.11+">
  <img src="https://img.shields.io/badge/status-alpha-orange.svg" alt="Status: alpha">
</p>

A thin, high-throughput SQL toolkit built **on top of SQLAlchemy Core** — for APIs,
message consumers, background workers, and CLIs.

- **SQL-first, ORM-free.** Write SQL or SQLAlchemy Core expressions. No models, no sessions.
- **Sync _and_ async.** `Database` and `AsyncDatabase` share one API, one config, one error model.
- **Safe by default.** Bounded pools, explicit transactions, per-operation timeouts,
  normalized errors (SQLSTATE-aware), and conservative retries.
- **Built on SQLAlchemy.** Pooling, dialects, and driver integration are SQLAlchemy's job —
  dbkit adds routing, resilience, observability, and bulk/streaming ergonomics.
- **Multi-database and sharded.** Named databases, pluggable shard resolvers (hash/range/
  directory/callable), replica routing with a read-your-writes override (pins reads to the
  primary for the scope — not lag-aware replica tracking), LRU engine eviction. dbkit does not
  support cross-shard transactions; use an outbox/saga pattern for multi-shard writes.
- **Fully observable.** Structured logging, Prometheus metrics, and OpenTelemetry tracing.
- **PostgreSQL first-class with psycopg 3** (the default driver, and the only one with a sync
  API — COPY and pipeline mode also require it). asyncpg is CI-covered for the async frontend
  (reads/writes/transactions, resilience/chaos, sharding/replicas, CLI) but is async-only and
  has no COPY/pipeline mode.

> Status: **alpha**, no PyPI release yet. Phases 1–5 are delivered: core runtime, resilience
> (retries, circuit breaker, concurrency limits), high-throughput paths (streaming, bulk
> insert/upsert, PostgreSQL COPY, effectively-once consumer helpers via a transactional inbox),
> multi-database & sharding (shard/replica routing, read-your-writes via primary-pinning, engine
> LRU eviction), and production hardening (OpenTelemetry tracing, CLI, docs site, PyPI release
> readiness). See `docs/requirements.md` for the full spec, `docs/roadmap.md` for phased
> delivery, and `docs/testing.md` for the test/chaos/benchmark commands. dbkit trusts the
> `DatabaseTarget`/shard key you give it — tenant/shard authorization is your application's
> responsibility, not dbkit's.

## When not to use dbkit

dbkit is not an ORM — there's no relationship loading, unit-of-work session, or model layer.
If your domain benefits from those, pair a real ORM with dbkit's config/resilience/observability
layer, or use the ORM alone. dbkit also doesn't manage schema migrations; pair it with Alembic
(or your migration tool of choice). It's a database-only toolkit: there's no broker/message-queue
client — the inbox/batching helpers are primitives your own consumer loop wires in.

**Compatibility:** Python 3.11+, SQLAlchemy 2.0.30+. CI runs the full suite against PostgreSQL 16
with psycopg (sync + async) and the async-only integration/chaos/sharding suite with asyncpg
(no sync API, no COPY/pipeline mode — those tests self-skip). Other SQLAlchemy-supported
dialects are not covered by CI.

## Install

```bash
pip install "dbkit[psycopg]"          # async + sync PostgreSQL
pip install "dbkit[psycopg,yaml,prometheus,otel,cli]"
```

## Quickstart (async)

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

The sync API is identical, without `await`:

```python
from dbkit import Database
db = Database.from_config(...)
db.start()
user = db.fetch_optional(GET_USER, {"id": 1}, target=DatabaseTarget(database="app"))
db.close()
```

## Examples

`examples/` has a runnable, idempotent script for every feature — transactions & savepoints,
error classification, retries & the circuit breaker, streaming, bulk insert/upsert, PostgreSQL
COPY, effectively-once consumer processing (transactional inbox pattern), micro-batching,
health/pool introspection, and sync/async parity:

```bash
export DBKIT_DSN=postgresql+psycopg://localhost/postgres
python examples/quickstart_async.py
python examples/run_all.py            # runs every example, safe to repeat
```

## CLI

```bash
pip install "dbkit[cli]"
dbkit config-validate config.yaml
dbkit check config.yaml           # validate + full readiness check
dbkit pools config.yaml           # warm a connection, print pool status
```

See `docs/cli.md` for the full command reference.

## Development

```bash
uv sync --extra dev
make lint type test          # unit suite, no database
docker compose up -d db
make integration             # real PostgreSQL
make unasync                 # regenerate src/dbkit/_sync from src/dbkit/_async
```

`src/dbkit/_sync/` is **generated** from `src/dbkit/_async/` — never edit it by hand.

## License

Apache-2.0.
