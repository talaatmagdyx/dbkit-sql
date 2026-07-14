# dbkit

A thin, high-throughput SQL toolkit built **on top of SQLAlchemy Core** — for APIs,
message consumers, background workers, and CLIs.

- **SQL-first, ORM-free.** Write SQL or SQLAlchemy Core expressions. No models, no sessions.
- **Sync _and_ async.** `Database` and `AsyncDatabase` share one API, one config, one error model.
- **Safe by default.** Bounded pools, explicit transactions, per-operation timeouts,
  normalized errors (SQLSTATE-aware), and conservative retries.
- **Built on SQLAlchemy.** Pooling, dialects, and driver integration are SQLAlchemy's job —
  dbkit adds routing, resilience, observability, and bulk/streaming ergonomics.
- **Multi-database and sharded.** Named databases, pluggable shard resolvers (hash/range/
  directory/callable), replica routing with read-your-writes, LRU engine eviction.
- **Fully observable.** Structured logging, Prometheus metrics, and OpenTelemetry tracing.
- **PostgreSQL first-class** (psycopg 3 default, asyncpg optional), general across any
  SQLAlchemy backend.

> Status: **alpha**. Phases 1–5 are delivered: core runtime, resilience (retries, circuit
> breaker, concurrency limits), high-throughput paths (streaming, bulk insert/upsert,
> PostgreSQL COPY, exactly-once consumer helpers), multi-database & sharding (shard/replica
> routing, read-your-writes, engine LRU eviction), and production hardening (OpenTelemetry
> tracing, CLI, docs site, PyPI release readiness). See `docs/requirements.md` for the full
> spec, `docs/roadmap.md` for phased delivery, and `docs/testing.md` for the test/chaos/
> benchmark commands.

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
COPY, exactly-once consumer processing (inbox pattern), micro-batching, health/pool
introspection, and sync/async parity:

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
