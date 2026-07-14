<p align="center">
  <img src="docs/assets/logo.svg" alt="dbkit logo" width="88" height="88">
</p>

<h1 align="center">dbkit</h1>

<p align="center">
  A thin, high-throughput SQL toolkit built <strong>on top of SQLAlchemy Core</strong> — for
  APIs, message consumers, background workers, and CLIs.
</p>

<p align="center">
  <a href="https://github.com/talaatmagdyx/dbkit-sql/actions/workflows/ci.yml">
    <img src="https://github.com/talaatmagdyx/dbkit-sql/actions/workflows/ci.yml/badge.svg" alt="CI">
  </a>
  <a href="https://talaatmagdyx.github.io/dbkit-sql/">
    <img src="https://img.shields.io/badge/docs-mkdocs--material-blue.svg" alt="Docs">
  </a>
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License: Apache-2.0">
  <img src="https://img.shields.io/badge/python-3.11%2B-blue.svg" alt="Python 3.11+">
  <img src="https://img.shields.io/badge/status-alpha-orange.svg" alt="Status: alpha">
</p>

---

## Contents

- [Why dbkit](#why-dbkit)
- [Status & quality bar](#status--quality-bar)
- [When not to use dbkit](#when-not-to-use-dbkit)
- [Compatibility](#compatibility)
- [Install](#install)
- [Quickstart (async)](#quickstart-async)
- [Documentation](#documentation)
- [Examples](#examples)
- [CLI](#cli)
- [Testing, benchmarks & CI](#testing-benchmarks--ci)
- [Development](#development)
- [Versioning](#versioning)
- [Security](#security)
- [Contributing](#contributing)
- [License](#license)

## Why dbkit

- **SQL-first, ORM-free.** Write SQL or SQLAlchemy Core expressions. No models, no sessions.
- **Sync _and_ async.** `Database` and `AsyncDatabase` share one API, one config, one error
  model — the sync frontend is generated from the async one, so they never drift.
- **Safe by default.** Bounded pools, explicit transactions, per-operation timeouts,
  normalized errors (SQLSTATE-aware), and conservative retries.
- **Built on SQLAlchemy.** Pooling, dialects, and driver integration are SQLAlchemy's job —
  dbkit adds routing, resilience, observability, and bulk/streaming ergonomics on top.
- **Multi-database and sharded.** Named databases, pluggable shard resolvers (hash/range/
  directory/callable), replica routing with a read-your-writes override (pins reads to the
  primary for the scope — not lag-aware replica tracking), LRU engine eviction. dbkit does not
  support cross-shard transactions; use an outbox/saga pattern for multi-shard writes.
- **Fully observable.** Structured logging, Prometheus metrics, and OpenTelemetry tracing —
  overhead measured, not assumed (see [Status & quality bar](#status--quality-bar)).
- **PostgreSQL first-class with psycopg 3** (the default driver, and the only one with a sync
  API — COPY and pipeline mode also require it). asyncpg is CI-covered for the async frontend
  (reads/writes/transactions, resilience/chaos, sharding/replicas, CLI) but is async-only and
  has no COPY/pipeline mode.

## Status & quality bar

**Alpha** — no PyPI release yet, no production track record. Phases 1–5 are functionally
complete: core runtime, resilience (retries, circuit breaker, concurrency limits),
high-throughput paths (streaming, bulk insert/upsert, PostgreSQL COPY, effectively-once
consumer helpers via a transactional inbox), multi-database & sharding, and production
hardening (OpenTelemetry tracing, CLI, docs site, release automation).

That completeness claim isn't just asserted — it's backed by two standing internal audits, kept
up to date and re-scored as fixes land, not written once and forgotten:

- [`PRODUCTION_READINESS_REVIEW.md`](PRODUCTION_READINESS_REVIEW.md) — correctness, security,
  and operational-gap review. **9.5 / 10.** Every finding a code or doc change could resolve has
  been fixed and verified against real PostgreSQL; the score is held back by exactly one thing
  no review pass can manufacture — real production runtime.
- [`PERFORMANCE_REVIEW.md`](PERFORMANCE_REVIEW.md) — throughput, latency, memory, and
  failure-mode behavior under load. **9.0 / 10.** Backed by real load tests against live
  PostgreSQL (concurrency scaling, deadlock/retry storms, streaming memory bounds, multi-process
  connection-budget validation), not estimates.

Both documents are blunt about what's still missing (a real multi-node deployment, multi-day
soak load) rather than rounding up. Read them before betting anything important on this.
dbkit trusts the `DatabaseTarget`/shard key you give it — tenant/shard authorization is your
application's responsibility, not dbkit's.

## When not to use dbkit

dbkit is not an ORM — there's no relationship loading, unit-of-work session, or model layer.
If your domain benefits from those, pair a real ORM with dbkit's config/resilience/observability
layer, or use the ORM alone. dbkit also doesn't manage schema migrations; pair it with Alembic
(or your migration tool of choice). It's a database-only toolkit: there's no broker/message-queue
client — the inbox/batching helpers are primitives your own consumer loop wires in.

## Compatibility

Python 3.11+, SQLAlchemy 2.0.30+. CI runs the full suite against PostgreSQL 16 with psycopg
(sync + async) and the async-only integration/chaos/sharding suite with asyncpg (no sync API,
no COPY/pipeline mode — those tests self-skip). Other SQLAlchemy-supported dialects are not
covered by CI.

## Install

```bash
pip install "dbkit-sql[psycopg]"          # async + sync PostgreSQL
pip install "dbkit-sql[psycopg,yaml,prometheus,otel,cli]"
```

> The PyPI distribution is `dbkit-sql` (the name `dbkit` was already taken) — but the import
> stays `import dbkit` regardless of which extras you install.

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

## Documentation

The full docs site (API reference, configuration, CLI, dashboards/alerting, troubleshooting) is
published at **[talaatmagdyx.github.io/dbkit-sql](https://talaatmagdyx.github.io/dbkit-sql/)**. Highlights:

| Topic | Where |
|---|---|
| API reference (`Database`/`AsyncDatabase`, config, query/routing, errors) | [`docs/api/`](docs/api/) |
| CLI command reference | [`docs/cli.md`](docs/cli.md) |
| Dashboards & alert rules (Grafana/Prometheus) | [`docs/observability.md`](docs/observability.md) |
| Security posture (redaction, TLS, connection budgets) | [`docs/security.md`](docs/security.md) |
| Troubleshooting guide | [`docs/troubleshooting.md`](docs/troubleshooting.md) |
| Testing, chaos, and benchmark commands | [`docs/testing.md`](docs/testing.md) |
| Full spec and phased delivery roadmap | [`docs/requirements.md`](docs/requirements.md), [`docs/roadmap.md`](docs/roadmap.md) |
| Versioning & deprecation policy | [`docs/versioning.md`](docs/versioning.md) |

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
pip install "dbkit-sql[cli]"
dbkit config-validate config.yaml
dbkit check config.yaml           # validate + full readiness check
dbkit pools config.yaml           # warm a connection, print pool status
```

See [`docs/cli.md`](docs/cli.md) for the full command reference.

## Testing, benchmarks & CI

Every claim in this README and in the two review documents is backed by a test, benchmark, or
chaos/soak run against real PostgreSQL — not by assertion. CI (`.github/workflows/ci.yml`) runs
on every push/PR: static checks (lint, `mypy --strict`, unasync drift, docs build, package
metadata), the unit suite across Python 3.11–3.13, and two full integration suites (psycopg and
asyncpg) against a real PostgreSQL service container, including chaos/resilience scenarios, a
short soak with fault injection, and a performance-regression gate. See
[`docs/testing.md`](docs/testing.md) for how to run all of it locally.

```bash
make lint type test          # unit suite, no database
docker compose up -d db
make integration              # real PostgreSQL
uv run python -m benchmarks   # full benchmark suite
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

## Versioning

Pre-1.0 (`0.x`), following the policy in [`docs/versioning.md`](docs/versioning.md): breaking
changes are called out in [`CHANGELOG.md`](CHANGELOG.md), not silently introduced. dbkit will
adopt semantic versioning at 1.0.

## Security

See [`docs/security.md`](docs/security.md) for the documented security posture — secret
redaction in logs/errors, TLS/`sslmode` guidance, connection-budget enforcement, and the
idempotent-write guard rail. Found a real vulnerability? Please open a GitHub issue with
minimal reproduction details rather than a public PR with an exploit.

## Contributing

Contributions are welcome — see [`CONTRIBUTING.md`](CONTRIBUTING.md) for the ground rules
(no ORM, never hand-edit `_sync/`, pure logic stays in `_core/`) and
[`docs/testing.md`](docs/testing.md) for how to run the full gate before opening a PR.

## License

[Apache-2.0](LICENSE).
