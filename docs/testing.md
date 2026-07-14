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
- **primary failover to a genuinely different backend** (two throwaway containers behind an
  in-process TCP proxy; the proxy is repointed and the old container stopped) → transparent
  recovery onto the new backend, distinguished from a same-instance restart by a marker row
  seeded into each backend (requires Docker)

```bash
make chaos             # needs PostgreSQL; the restart test self-skips without Docker
```

## 5. Benchmarks and soak

A custom asyncio/monotonic harness (no external bench framework). Starts one container or
uses `--dsn` / `DBKIT_BENCH_DSN`.

```bash
make bench                                    # overhead, throughput, latency, batch, crud
python -m benchmarks --only crud --dsn "$DBKIT_TEST_DSN"
```

- **overhead** — A/B vs raw psycopg and raw SQLAlchemy Core (headline overhead %).
- **throughput** — ops/s for small reads and single-row inserts (async + sync).
- **latency** — open-loop paced P50/P95/P99.
- **batch** — `execute_many` vs per-row vs COPY.
- **crud** — INSERT, SELECT (point + range), UPDATE, UPSERT, DELETE: ops/s and P50/P99
  latency per operation, on both frontends (`--only crud`).

Results persist to `benchmarks/results/run_<ts>.json` and print a regression delta vs the
previous run. Metric keys encode direction (`*_ops_s` higher-better, `*_ms`/`*_pct` lower-better).

The **soak** applies sustained paced load with periodic fault injection and gates on
leak-free recovery (exit code = verdict):

```bash
make soak DURATION=120 KILL_EVERY=30          # or: python -m benchmarks.soak --dsn ...
```

Verdicts: made progress, recovered after every kill, and bounded RSS / FDs / asyncio tasks /
pool connections.

## 6. The `unasync` code generator

`src/dbkit/_async/` is the single hand-written source of truth; `src/dbkit/_sync/` is
mechanically generated from it by `tools/run_unasync.py`, a straight-line **token-substitution**
transform (not an AST rewrite) — line by line, apply the longest-match-first replacements in its
`TOKENS` dict, plus two comment markers:

- `# unasync: remove` at the end of a line — drops that single line in the generated output.
- `# unasync: remove-start` / `# unasync: remove-end` — drops everything between the markers
  (inclusive of the markers themselves).

The full token table (imports, SQLAlchemy async types, dbkit's own class names, concurrency
primitives, and control-flow keywords — `async def`→`def`, `await `→``, `async with`→`with`,
etc.) is in `tools/run_unasync.py`'s `TOKENS` dict; read it directly rather than duplicating it
here; it's the single source of truth and this doc would drift otherwise.

**What this can't handle:** anything that doesn't reduce to a token or a marked-out block —
e.g. an async-only control-flow shape with no sync equivalent, or a new asyncio primitive with
no entry in `TOKENS`. `_compat.py` exists precisely for this: it's hand-written **separately**
on both sides (`_async/_compat.py` / `_sync/_compat.py`, listed in `HANDWRITTEN` and skipped by
the generator entirely) for the handful of primitives that are genuinely different between the
two worlds (client-side timeouts, cancellation, semaphore-with-timeout — see
`semaphore_acquire()`). If you need new async-only behavior, either find a token-level rewrite
that already fits, or add a same-named function to both `_compat.py` files.

`tests/unit/test_unasync_translation.py` is a smoke test asserting the transform handles a
deliberately awkward fixture snippet (nested `async with`, `async for` inside a comprehension-
adjacent block, chained `await`) correctly — it exists to catch a *silent* mistranslation (code
that still parses and imports, just wrong) before it reaches `_sync/`, which `--check` alone
cannot do (it only proves regeneration is deterministic, not that the rules are complete).

## Local Docker note

The chaos server-restart test and the testcontainers path require a reachable Docker daemon.
If Docker is unavailable locally, set `DBKIT_TEST_DSN` / `DBKIT_BENCH_DSN` to an existing
PostgreSQL — everything except the container-restart test runs against it, and that one test
self-skips. CI provides PostgreSQL as a service and runs the full suite.
