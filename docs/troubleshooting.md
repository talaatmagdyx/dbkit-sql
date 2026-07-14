# Troubleshooting

A symptom → likely cause → fix table for the failure modes teams hit most often, seeded from a
production-readiness review of this codebase. If you hit something not covered here, `dbkit
check`/`dbkit pools` (see `cli.md`) and the structured logs (§25.3) are the next place to look.

## "Why didn't my write retry?"

Retries for writes are off by default and gated by two independent conditions — if either is
missing, the write fails immediately instead of retrying:

1. `RetryConfig.retry_writes` must be `True` (default `False`).
2. The specific `Query` must have `idempotent=True` (or you passed an
   `idempotent_override=True` for that call).

This is intentional (§14) — dbkit never guesses whether a write is safe to repeat. Run
`dbkit query-list` to see which registered writes are marked idempotent, and whether dbkit's
best-effort heuristic thinks the SQL looks unguarded (a `WARNING` next to the query — see
`docs/api/query.md` and §3 below).

A `DatabaseCommitUnknownError` (or any error with `transaction_state_unknown=True`) is **never**
retried automatically, regardless of `retry_writes`/`idempotent` — the commit's outcome is
genuinely ambiguous, and retrying blind could duplicate a write that already landed. See the
next section.

## "I got a DatabaseCommitUnknownError — what do I do?"

This means the connection failed during `COMMIT` and dbkit cannot tell whether the transaction
actually committed on the server before the failure (§15). Both `db.transaction()` and
`db.execute()`/`db.insert_many()`/etc. give this same guarantee.

- **Do not blindly retry** the same write — if it already committed, retrying would duplicate
  it (unless the write itself is idempotent at the database level, e.g. `ON CONFLICT`).
- The safe pattern is to check whether the write already landed (e.g. query for the row by its
  natural/idempotency key) before deciding to retry or skip.
- For message-driven writes, `dbkit.integrations.ack_after_commit` already does the right thing:
  it never acks the broker message on a commit-unknown outcome, so the message redelivers and
  the transactional-inbox pattern (§28) dedupes safely on the next attempt.

## "My query looks idempotent but `dbkit query-list` warns about it anyway"

The heuristic (`_core/idempotency_lint.py`) only recognizes a few explicit textual patterns
(`ON CONFLICT`, `WHERE NOT EXISTS`, `MERGE`) on `INSERT` statements — it cannot see your
schema's unique constraints. If your `INSERT` is genuinely safe to run twice (e.g. a unique
constraint on a natural key that would raise on the second attempt rather than duplicate data),
the warning is a false positive and safe to ignore. If it's *not* actually safe, this is exactly
the case the warning exists to catch — see §3 of `PRODUCTION_READINESS_REVIEW.md` for the
duplicate-row failure mode this guards against.

## Pool exhaustion / requests hanging

- **Symptom:** requests intermittently hang or time out under load, `db_pool_wait_seconds` is
  high, or `dbkit pools` shows `checked_out` near `size + max_overflow`.
  **Cause:** the pool is undersized for your concurrency, or connections are held longer than
  expected (check `db_connection_hold_duration_seconds` — see `docs/observability.md`).
  **Fix:** raise `PoolConfig.size`/`max_overflow`, or find what's holding connections open
  (a missing `await`, a long-running transaction, a stream not being closed).

- **Symptom:** a specific concurrency tier (`ConcurrencyConfig.reads`/`writes`/`bulk_writes`)
  raises `DatabaseOverloadedError` under load.
  **Cause:** that tier's semaphore is saturated and no slot freed up within the operation's
  effective timeout — this is a deliberate bound (previously this queued forever with no
  signal; see the review's "Concurrency-limiter tiers" finding).
  **Fix:** raise the tier's limit, or reduce concurrent demand on it; the error is retryable, so
  a normal backoff/retry (respecting idempotency, as above) is appropriate.

## Connection-budget surprises

- **Symptom:** PostgreSQL starts rejecting connections (`too many clients already`) under a
  fleet-wide traffic spike, even though a single instance looks fine.
  **Cause:** total connections across every pod × shard × replica × pool size can exceed
  PostgreSQL's `max_connections` even when each individual pool looks small.
  **Fix:** run `dbkit connection-budget config.yaml --replicas N` (N = your pod/replica count)
  *before* a rollout, and set `ConnectionBudgetConfig.enforce_at_startup=True` so a
  misconfiguration fails fast at startup instead of under load.

## PgBouncer

- **Symptom:** prepared-statement errors or "prepared statement already exists" under
  PgBouncer.
  **Cause:** PgBouncer's *transaction* pooling mode can route the same logical connection to a
  different physical backend each transaction; driver-side prepared-statement autoprep
  (psycopg's `prepare_threshold`, asyncpg's `statement_cache_size`) then targets the wrong
  backend.
  **Fix:** set `PoolConfig.pgbouncer_compatible=True`, which disables that autoprep. Verify with
  `dbkit pools` or by checking `raw.driver_connection.prepare_threshold is None` after startup.

## asyncpg-specific

- **Symptom:** `COPY`/`tx.pipeline()` raise `DatabaseUnsupportedOperationError`, or a bare
  literal parameter (`sql("SELECT :n")` with no column context) fails with a asyncpg `DataError`
  about an unexpected type.
  **Cause:** both are genuine asyncpg limitations, not dbkit bugs: COPY/pipeline mode are
  psycopg-only raw-driver escape hatches, and asyncpg requires explicit typing for parameters
  with no column to infer a type from (unlike psycopg). See `README.md`'s compatibility note.
  **Fix:** use psycopg for COPY/pipeline mode; for untyped literal comparisons, add an explicit
  `CAST(:n AS integer)` (or the appropriate type) to the SQL text.
- **Symptom:** the sync `Database` facade doesn't work with an asyncpg DSN.
  **Cause:** asyncpg has no synchronous API at all — this is expected, not a bug (`greenlet_
  spawn has not been called` is the underlying SQLAlchemy error you'll see).
  **Fix:** use `AsyncDatabase` with asyncpg, or use psycopg for the sync frontend.

## Read-your-writes across threads

- **Symptom:** inside `db.consistency_scope(mode="read_your_writes")`, a read issued from a
  worker thread (`loop.run_in_executor`, `asyncio.to_thread`, a plain `threading.Thread`, or the
  sync `Database` facade driven from a thread pool) doesn't see a write made earlier in the same
  scope — it still hits a replica.
  **Cause:** the override is a `contextvars.ContextVar`. `asyncio.create_task()` copies the
  current context, so a task created *inside* the scope inherits it — but a new OS thread does
  not automatically inherit the calling thread's/task's context at all (this is standard
  `contextvars` behavior, not a dbkit limitation).
  **Fix:** capture `contextvars.copy_context()` in the async caller before dispatching to the
  thread, and run the thread's work through that captured context (e.g.
  `ctx.run(thread_fn, ...)` instead of calling `thread_fn` directly) — or simplest, just
  construct the read's `DatabaseTarget` with `role="write"`/`role="primary_only"` explicitly
  for that specific read instead of relying on the scope.

## Circuit breaker

- **Symptom:** `DatabaseCircuitOpenError` raised immediately, without even attempting a
  connection.
  **Cause:** the breaker for that `database`/`shard`/`role` tripped after enough
  infrastructure-category failures (connection/pool/availability/timeout — never
  integrity/programming errors) within `window_seconds`.
  **Fix:** check `db_circuit_breaker_state` (0=closed/1=half_open/2=open,
  `docs/observability.md`) and the underlying infra issue; the breaker will move to half-open
  and probe again after `open_seconds` on its own — no manual reset needed.
