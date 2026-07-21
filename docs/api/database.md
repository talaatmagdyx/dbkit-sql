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

## Advisory locks (transaction scope)

The scope from `db.transaction(...)` also exposes transaction-scoped PostgreSQL advisory locks —
cooperative locks on a logical key (an order id, an engagement id) rather than on rows:

```python
async with db.transaction(target=t, lock_timeout=3.0) as tx:
    await tx.advisory_xact_lock("engagement:42")   # blocks until granted; auto-released at commit
    ...                                            # read-modify-write, serialized per key
```

`await tx.advisory_xact_lock(key)` blocks until granted and holds until the transaction ends
(there is no manual unlock — it can't leak); a wait beyond the transaction's `lock_timeout` raises
`DatabaseLockTimeoutError`. `await tx.try_advisory_xact_lock(key) -> bool` is the non-blocking
variant (`False` if another transaction holds it). `key` is an `int` (used directly) or a `str`
(hashed server-side); it is always bound, never interpolated. PostgreSQL only.

::: dbkit.AsyncDatabase

::: dbkit.Database
