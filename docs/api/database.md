# Database (async/sync)

`AsyncDatabase` and `Database` share one API, generated from a single async source via
`unasync` (see `docs/testing.md`) — every method below exists identically on both, `async`/
`await` aside.

## The `.raw` escape hatch — you now own error handling

Every connection/transaction scope (returned by `db.connection()`/`db.transaction()`) exposes a
`.raw` property: the underlying SQLAlchemy `Connection`/`AsyncConnection`. It exists for the two
things dbkit doesn't implement itself — `tx.pipeline()` and `db.copy_records()`'s raw driver
access both use it internally — and for genuine one-off raw-driver needs.

**Using `.raw` directly opts you out of dbkit's entire value proposition for that statement:**
no SQLSTATE classification (you get raw `psycopg`/`asyncpg`/SQLAlchemy exceptions, not
`DatabaseError` subclasses — an `except DatabaseError` handler will silently stop catching
them), no metrics, no tracing, no retry/circuit-breaker/commit-unknown handling. This is by
design (dbkit can't classify or retry a statement it never saw), but it's easy to reach for as a
shortcut and not notice you've silently disabled error handling for that code path. Prefer the
normal `db.execute`/`fetch_*`/`stream`/bulk methods for anything that isn't one of the two
documented raw-driver escape hatches.

::: dbkit.AsyncDatabase

::: dbkit.Database
