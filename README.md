# dbkit

A thin, high-throughput SQL toolkit built **on top of SQLAlchemy Core** — for APIs,
message consumers, background workers, and CLIs.

- **SQL-first, ORM-free.** Write SQL or SQLAlchemy Core expressions. No models, no sessions.
- **Sync _and_ async.** `Database` and `AsyncDatabase` share one API, one config, one error model.
- **Safe by default.** Bounded pools, explicit transactions, per-operation timeouts,
  normalized errors (SQLSTATE-aware), and conservative retries.
- **Built on SQLAlchemy.** Pooling, dialects, and driver integration are SQLAlchemy's job —
  dbkit adds routing, resilience, observability, and bulk/streaming ergonomics.
- **PostgreSQL first-class** (psycopg 3 default, asyncpg optional), general across any
  SQLAlchemy backend.

> Status: **alpha**. Phases 1–3 are delivered: core runtime (config, pooling, transactions,
> typed results, health, observability), resilience (retries, circuit breaker, concurrency
> limits), and high-throughput paths (streaming, bulk insert/upsert, PostgreSQL COPY, inbox /
> exactly-once consumer helpers). See `docs/requirements.md` for the full spec, `docs/roadmap.md`
> for phased delivery, and `docs/testing.md` for the test/chaos/benchmark commands.

## Install

```bash
pip install "dbkit[psycopg]"          # async + sync PostgreSQL
pip install "dbkit[psycopg,yaml,prometheus]"
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
