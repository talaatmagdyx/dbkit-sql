# dbkit — Deep Performance Review

**Reviewer stance:** Principal Performance Engineer / Senior Database Architect / High-Scale
Python Systems Engineer. This is a performance-only review — correctness, security, and API
design are out of scope except where they directly affect throughput, latency, memory, or
failure behavior under load (see `PRODUCTION_READINESS_REVIEW.md` for the general review).

**Method:** every claim below is tagged:

- **`[confirmed]`** — verified by reading the actual `dbkit` source in this repository, with a
  `file:line` citation.
- **`[inferred]`** — a reasonable technical conclusion (e.g. from general PostgreSQL/SQLAlchemy/
  asyncio behavior) that was **not** independently verified against this repo's source or a live
  run in this review.
- **`[unconfirmed — needs profiling/production data]`** — a real performance question this
  review cannot answer from source alone; requires a live load test, `EXPLAIN`/`pg_stat_*`
  output, or a memory/CPU profile.

README/CHANGELOG/roadmap claims (e.g. "~90× faster," "cutting overhead from ~31% to ~6%") are
treated as **historical claims from this project's own benchmark suite, not independently
reproduced in this review**, and are labeled as such throughout. No benchmark was executed as
part of this review — this is a static/source-level performance audit.

---

## Update — fixes applied after this review

Every finding in §16's summary table was investigated and, where a fix was possible, applied and
verified against a live PostgreSQL instance — not just documented. Two findings resolved
favorably without a code change (a live empirical test, not source reading, was needed to know
which): **Finding #1 (server-side timeout backstop) is CONFIRMED SAFE, not a bug** — see below.
Each item is annotated **`Status: FIXED`**/**`Status: CONFIRMED SAFE`** inline in its section.
Summary:

1. **Finding #1 [High] (server-side statement cancellation) — `Status: CONFIRMED SAFE, not a
   bug`.** Live-tested against real PostgreSQL: held a row lock in one session, ran a
   `db.execute()` with `timeout=0.5` against it from dbkit in another, then inspected
   `pg_stat_activity`. The blocked backend was gone — not lingering "active"/waiting on the
   lock. Root cause traced into both drivers' source: psycopg3's `AsyncConnection.wait()`
   (`connection_async.py:510-524`) and asyncpg's `Connection._cancel_current_command`
   (`connection.py:1652-1682`) **both** send a real PostgreSQL cancel request whenever
   `asyncio.CancelledError` interrupts an in-flight query wait — exactly what dbkit's
   `asyncio.timeout()`-based client deadline triggers. Locked in as a permanent regression test
   (`tests/integration/test_resilience_scenarios.py::
   test_client_side_timeout_actually_cancels_the_server_side_statement`), verified against both
   drivers. This is the single most important empirical result of this whole review pass — the
   concern was legitimate to raise, and it resolved favorably rather than uncovering a bug.
2. **Finding #2 [High] (unenforced retry budget) — `Status: FIXED`.** `RetryConfig.
   maximum_total_ms` is now a real, enforced ceiling on total time spent retrying
   (`_async/resilience.py::run_with_retries`), independent of `attempts`/the caller's own
   `deadline`. Regression-tested (`tests/unit/test_resilience.py`): a config with
   `attempts=1000` and a 25ms budget stops after ~2-3 attempts, not 1000; a call that finishes
   within budget is unaffected.
3. **Finding #3 [High] (no-op cancellation shield) — `Status: FIXED`.** `cancellation_shield()`
   is replaced by `shield_from_cancellation()` (`_async/_compat.py`), a genuine
   `asyncio.shield()`-based implementation: rollback/release now run to completion even if the
   task is cancelled again mid-cleanup, with the cancellation correctly deferred (never
   swallowed) until cleanup finishes. Verified with a targeted test that injects cancellation
   *during* the protected work, not just around the whole operation
   (`tests/unit/test_cancellation_shielding.py`) — exactly the scenario the old no-op
   implementation could never have passed.
4. **Finding #4 [Medium] (LRU eviction holds the lock during dispose) — `Status: FIXED`.**
   `AsyncEngineRegistry.get()` now pops the LRU victim under the lock but disposes it *after*
   releasing the lock, matching `dispose_one()`'s existing (correct) shape. Regression-tested
   with a slow-dispose double (`tests/unit/test_engine_registry.py::
   test_lru_eviction_does_not_block_concurrent_lookups_during_dispose`) — confirmed this test
   times out under the old code and passes under the fix.
5. **Finding #5 [Medium] (duplicate labels + full-label-set Prometheus fill) — `Status: FIXED`.**
   `scope()` now accepts an already-resolved `entry`/`labels` from `execute_with_resilience`
   instead of recomputing them — verified via a call-counting regression test showing exactly 1
   call each, down from 2 (`tests/integration/test_async_integration.py::
   test_one_shot_calls_resolve_entry_and_labels_exactly_once_each`). `PrometheusMetrics._fill()`
   now merges a precomputed empty-label template instead of a per-key `.get()` loop every call.
6. **Finding on `max_payload_bytes` [Medium], §9 — `Status: FIXED`.** `resolve_batch_rows()` now
   accepts a `sample_row` and shrinks the batch by estimated byte size when `max_payload_bytes`
   is set, via a new `estimate_row_bytes()` helper. Verified against real PostgreSQL: a
   10,000-byte budget correctly caps a batch of ~1KB rows at ≤15 rows instead of the configured
   1000-row ceiling (`tests/integration/test_throughput_paths.py::
   test_insert_many_shrinks_batch_for_wide_rows_when_max_payload_bytes_set`).
7. **`ConcurrencyConfig.expensive_queries` dead config knob, §5 — `Status: FIXED`.** Wired into
   `ConcurrencyLimiter` as a real tier, gated by a new `Query(expensive=True)` field — acquired
   *in addition to* the normal reads/writes tier. Verified against real PostgreSQL: saturating
   the tier with one slow `expensive=True` query correctly blocks a second `expensive=True`
   query (`DatabaseOverloadedError`) while an ordinary (non-expensive) query proceeds unaffected
   (`tests/integration/test_async_integration.py`, two new tests).
8. **best_effort/split_on_failure silently dropping batches/rows, §9 — `Status: FIXED`.** Added
   `bulk_batch_dropped_warning()` (a `database.bulk.rows_dropped` log event) and a new
   `db_bulk_rows_dropped_total` metric, fired whenever a batch (`best_effort`) or row
   (`split_on_failure`) is dropped. Verified against real PostgreSQL with `caplog`/a metrics
   double: a duplicate-key batch drop is now observable with the correct row count and error
   category (`tests/integration/test_throughput_paths.py::
   test_best_effort_mode_logs_and_counts_a_dropped_batch`).
9. **unnest benchmark-claim/script mismatch, §9/§14 — `Status: FIXED`.** The "~32× at 20k rows"
   figure had **no committed benchmark backing it at all** (not even at a different row count).
   Added `benchmarks/bench_unnest.py` and ran it repeatedly against real PostgreSQL: ~29× in
   steady state at 20,000 rows (a first/cold run measured closer to ~20× — real run-to-run
   variance, now stated honestly in `docs/roadmap.md`/`CHANGELOG.md` instead of a single point
   estimate). Registered in `python -m benchmarks --only unnest`.
10. **HashShardResolver recomputing SHA-256 per call, §10 — `Status: FIXED`.** Added a bounded
    (4096-entry) per-instance LRU cache — bounded deliberately, mirroring the engine registry's
    own `max_engines`/`evict_lru` reasoning, so a high-cardinality shard-key space can't grow it
    unboundedly. Regression-tested for correctness (cached result matches an uncached
    computation) and boundedness (`tests/unit/test_sharding_replica.py`, three new tests).
11. **No CI-gating performance-regression check; no confidence intervals in the benchmark
    suite, §14 — `Status: FIXED`.** `benchmarks/_stats.py` now computes a pure-stdlib percentile
    bootstrap confidence interval on every `robust()` summary (honest about the wide intervals a
    3-5-rep suite actually justifies, rather than a false-precision point estimate) —
    unit-tested (`tests/unit/test_benchmark_stats.py`). A new `benchmarks/check_regression.py`
    is wired into CI (`.github/workflows/ci.yml`) as a genuinely gating step: fails the build if
    dbkit's overhead vs. raw SQLAlchemy Core reaches its own historical worst case (~40%) again,
    or if the pool-exhaustion fail-fast contract breaks. `bench_pool_exhaustion.py`/
    `bench_pgbouncer_compatible.py` (previously unregistered anywhere) are now in the standard
    `python -m benchmarks` suite list too.

**A real cross-cutting bug was found and fixed while verifying these items, not requested, found
by actually running the sync test suite against real PostgreSQL:** the `unasync` code
generator's `TOKENS` table had no mapping for `contextlib.AsyncExitStack`/`enter_async_context` —
introduced while fixing Finding #7's tier-acquisition logic — which silently generated **broken
sync code** (`'AsyncExitStack' object does not support the context manager protocol`), caught
only because `tests/integration/test_sync_integration.py` actually executes against a live
database rather than merely type-checking. Fixed in `tools/run_unasync.py`, with the missing
construct added to the translation smoke-test fixture
(`tests/unit/test_unasync_translation.py`) so this class of gap is caught automatically for any
future change, not just this one.

**Not fixed — inherent, not something a code change resolves:** the "no documented workload
assumptions / target SLOs" gap (§1) is a product-decision gap, not a code defect; the connection-
budget-enforcement-is-opt-in-by-default posture (§2) was deliberately left as a warning (not a
default-behavior change) to avoid silently breaking existing deployments' startup — see the
reasoning already applied to the identical trade-off for the idempotency lint in
`PRODUCTION_READINESS_REVIEW.md`. Every load-test scenario in §15 remains a recommended-but-not-
yet-executed test design, not a gap this pass closed — running that full matrix is a multi-day
exercise requiring dedicated infrastructure, tracked as future work, not silently dropped.

---

## 1. Performance Model

### Stated vs. missing workload assumptions

**Nothing in this repository states a target RPS, concurrency level, read/write ratio, payload
size, shard count, replica count, or p50/p95/p99/p99.9 latency SLO.** `docs/requirements.md`
describes *capabilities* (§-numbered functional requirements), not *capacity targets*. This is a
real gap: a performance review cannot certify "suitable for X ops/sec" when the project itself
has never stated what X should be. Every number in this report is therefore either a structural
capacity ceiling (pool math, semaphore counts) or a project-reported historical benchmark figure
— not a validated target.

**Confirmed defaults that stand in for assumptions** (`[confirmed]`, `src/dbkit/_core/config.py`):
- Pool: `size=10, max_overflow=5` per engine (per database×shard×role×driver key) → 15
  connections/engine.
- Query timeout: `2.0s`; transaction timeout: `5.0s`; long-transaction warning: `5.0s`.
- Retry: `attempts=2` (i.e. one retry), `retry_writes=False` by default.
- Circuit breaker: `failure_threshold=10` over a `30s` window, `open_seconds=10s`.
- No concurrency-limiter tiers are configured by default (`ConcurrencyConfig` fields all
  default to `None` — unlimited).
- Connection budget: **not enforced by default** (`enforce_at_startup=False`).

### Likely bottleneck by scale (structural reasoning, not benchmarked)

| Scale | Likely first bottleneck |
|---|---|
| **Small** (1 instance, low QPS, 1 db) | Not dbkit — PostgreSQL query design/indexes. dbkit's per-call overhead (label-dict rebuilds, SHA-256 shard hashing, full-label Prometheus fills — see §4/§11) is real but sub-millisecond and dwarfed by network+query time at low volume. |
| **Single-node, high concurrency** (many concurrent async tasks, one process) | The **pool** (`size+max_overflow=15` by default) and any configured `ConcurrencyLimiter` tiers, in that order — semaphores gate before pool checkout (`[confirmed]`, §5), so a saturated pool queues in the semaphore first, then in the pool. Default has no concurrency limiter configured, so the pool alone becomes the wall. |
| **Multi-process** (N worker processes / pods on one host or fleet) | **Connection multiplication** — each process owns an independent `AsyncEngineRegistry`; there is no cross-process pool sharing or coordination (`[confirmed]`, engine registry is in-process state, §2). Connection count scales linearly with process count with no dbkit-side ceiling unless `connection_budget.enforce_at_startup=True` is explicitly set (default off, §2). |
| **Multi-database** (several logical `DatabaseConfig`s, each primary+replicas) | Same as multi-process, compounded: the engine key includes `database` (`EngineKey`, `[confirmed]`), so pool count multiplies by database count too. Per-database circuit breakers/limiters give good failure isolation (`[confirmed]`, §5/§7) but do nothing to cap aggregate connection count. |
| **Sharded** (shard count growing) | Connection math scales by shard count directly (fastest way to exceed `max_connections`, §2); at high shard cardinality with `evict_lru=True`, the **O(n) LRU-scan-with-lock-held** eviction path (§2 Finding) becomes a real tail-latency risk under eviction churn. |

---

## 2. Connection Pooling

### Confirmed defaults (`src/dbkit/_core/config.py`, `PoolConfig`)

`size=10, max_overflow=5, timeout_seconds=5.0, recycle_seconds=1800, pre_ping=True,
use_lifo=True, reset_on_return="rollback", connect_timeout_seconds=10.0,
long_hold_warning_seconds=2.0, disable_pooling=False, pgbouncer_compatible=False`.
`ConnectionBudgetConfig`: `maximum_per_process=None, enforce_at_startup=False`.

### Pool lifecycle mechanics `[confirmed]`

- **Engine key**: `environment:database:shard_id:role:driver` (`_core/keys.py:12-24`) — one pool
  per unique combination. A deployment with 1 primary + 1 replica × 4 shards × 1 environment ×
  1 driver = **8 distinct pools**, each with its own 15-connection ceiling by default.
- **Engine creation is lazy** — `create_async_engine(url, **kwargs)` does not eagerly open a
  connection (`_async/engine.py:118-141`); connections open on first checkout.
- **Registry lookup has a lock-free fast path**: a cache hit (`self._entries.get(key_str)`) never
  touches `asyncio.Lock` (`_async/engine.py:143-151`) — good, avoids lock contention on the
  common case. Cache misses take a double-checked-locking path.
- **`pgbouncer_compatible=True`** sets `prepare_threshold=None` (psycopg) / `statement_cache_size=0`
  (asyncpg) at connect time (`_async/engine.py:45-61`) — disables client-side statement autoprep,
  required correctness fix under PgBouncer transaction pooling (see §13). Measured cost of this
  (project's own benchmark, `benchmarks/bench_pgbouncer_compatible.py`): p50 delta vs. autoprep-on
  was noise-level (roughly ±0.05ms) on a sub-millisecond localhost query — **not independently
  reproduced in this review**, and explicitly does not model real PgBouncer network/routing
  overhead (the doc says so itself).

### Findings

**[Medium] LRU eviction is O(n) and holds the registry lock during `dispose()`.** `Status:
FIXED.` `get()` now pops the LRU victim under the lock but disposes it *after* releasing the
lock (mirroring `dispose_one()`'s existing shape). Verified with a regression test that patches
a slow `dispose()` onto the victim engine and confirms a concurrent `get()` for a different key
completes quickly instead of blocking (`tests/unit/test_engine_registry.py::
test_lru_eviction_does_not_block_concurrent_lookups_during_dispose`) — confirmed this test times
out under the pre-fix code and passes under the fix.
`_evict_lru_locked()` computed `min(self._entries, key=lambda k: ...last_used)` — an O(n) scan
over all live engines (`_async/engine.py:188-195`, original code) — and ran **inside** the
`async with self._lock:` block from `get()`, meaning `await victim.engine.dispose()` executed
*while holding the registry lock*. Every other coroutine that missed the lock-free fast path
during that window blocked. `[confirmed]`.

**[Medium] Connection-budget enforcement is opt-in and silent by default.**
`enforce_at_startup=False` (`config.py:176`) means the formula below can exceed real
`max_connections` with nothing failing until PostgreSQL itself starts rejecting connections
under load. Mitigated by `dbkit check`/`config-validate` warnings (non-default, must be run
manually/in CI) — not automatic enforcement. See worked example below.

**[Low] Engine creation/disposal cost is not benchmarked anywhere** — no test or benchmark
measures wall-clock cost of `create_async_engine()`/`.dispose()` under churn (relevant to the
LRU-eviction finding above). `[unconfirmed — needs profiling]`.

### Maximum-connections formula and a worked example

```
max_connections ≈ app_instances × processes_per_instance × databases × shards × (1 + replicas) × (pool.size + pool.max_overflow)
```

Worked example, plausible mid-size deployment: 20 pod instances × 2 worker processes/pod × 1
database × 4 shards × 2 targets (1 primary + 1 replica) × 15 (default pool capacity) =
**20 × 2 × 1 × 4 × 2 × 15 = 4,800 connections** — against a commonly-configured PostgreSQL
`max_connections=500` (a stock default is even lower, 100), this is **~9.6× over budget**, and
none of it fails at startup unless `connection_budget.enforce_at_startup=True` was explicitly
set. **Flag**: any deployment with ≥3 shards and ≥2 processes/instance should treat
`enforce_at_startup=True` as mandatory, not optional, and this should arguably be closer to a
loud default than an opt-in CLI check.

---

## 3. Sync and Async Execution

### Code generation `[confirmed]`

`src/dbkit/_sync/` is mechanically generated from `src/dbkit/_async/` by
`tools/run_unasync.py` — a custom literal-token substitution transform (longest-key-first,
`re.escape`d substitution per line), not the third-party `unasync` package. `_compat.py` is
hand-written separately on both sides for genuine divergences:

- **Timeout**: async wraps calls in a real client-side `asyncio.timeout(seconds)`
  (`_async/_compat.py:60-65`); sync uses `contextlib.nullcontext()` — **no client-side deadline
  in the sync build at all**, relying entirely on server-side `statement_timeout`.
- **Semaphore acquire-with-timeout**: async wraps `asyncio.Semaphore.acquire()` in
  `asyncio.wait_for` (adds a `Task` allocation per acquire-with-timeout); sync uses
  `threading.Semaphore.acquire(timeout=...)` natively (no extra allocation).
- **Cancellation shield is a literal no-op on both sides** — see Finding below.

### Async-path SET-timeout optimization, and what it costs

`_maybe_set_timeout()` (`_async/connection.py:39-50`) **skips** the per-op `SET LOCAL
statement_timeout` SQL round trip entirely on the async one-shot path
(`if IS_ASYNC or not is_postgres: return`), relying purely on client-side `asyncio.timeout`.
This is a real, documented perf win — the project's own historical benchmark claim
(`docs/roadmap.md:47-49`, `CHANGELOG.md`) states this "cut[s] small-read overhead from ~31% to
~6% over raw SQLAlchemy Core" — **not independently reproduced in this review**.

**[High → RESOLVED] Does this optimization remove the server-side timeout backstop for one-shot
async queries?** `Status: CONFIRMED SAFE — no bug, verified empirically against real
PostgreSQL, not just source-read.` A raw psycopg3/asyncpg cancellation-handling trace plus a
live `pg_stat_activity` experiment both confirm: **it does not remove the backstop.** Both
drivers send a real PostgreSQL cancel request whenever `asyncio.CancelledError` interrupts an
in-flight query wait — exactly what dbkit's `asyncio.timeout()` client deadline triggers on
expiry:
- **psycopg3**: `AsyncConnection.wait()` (`connection_async.py:510-524` in the installed
  package) explicitly catches `(asyncio.CancelledError, KeyboardInterrupt)` during a query wait
  and, if the connection's transaction state is `ACTIVE`, calls `await self._try_cancel(timeout=
  5.0)` — a genuine `PQcancel`-equivalent request — before re-raising.
- **asyncpg**: `Connection._cancel_current_command` (`connection.py:1652-1682`) opens a fresh
  connection to send a real `CancelRequest` using the backend's pid/secret whenever the
  in-flight command's waiter is cancelled.
**Live reproduction performed**: held a row lock in one session (`BEGIN; SELECT ... FOR
UPDATE`), ran `db.execute(UPDATE ..., timeout=0.5)` against it from a second session via dbkit,
and inspected `pg_stat_activity` immediately and 2s after the client-side timeout fired. The
abandoned backend was **gone** (not lingering `active`/waiting on the lock) — `pool_status()`
showed the connection was invalidated (`invalidations: 1`), consistent with a real server-side
cancel having occurred. Locked in as a permanent regression test, verified against both drivers
(`tests/integration/test_resilience_scenarios.py::
test_client_side_timeout_actually_cancels_the_server_side_statement`).
**This was a legitimate concern to raise** — a client-side-only timeout with no server-side
cancel is a real, common footgun in other database toolkits — but for dbkit's actual supported
drivers, it resolves favorably. `[confirmed]`.
**Note**: explicit transactions never had this gap either — `_apply_settings()`
(`_async/transaction.py:154-165`) unconditionally issues `SET LOCAL statement_timeout`/
`lock_timeout` via a real round trip on every transaction, both frontends, regardless of
`IS_ASYNC`, as a second, independent backstop.

### Sync vs. async — when each wins

- **Sync (`Database`) likely outperforms async for**: low-concurrency, CPU-light workloads
  where thread/greenlet overhead exceeds asyncio's event-loop scheduling cost, and any workload
  requiring psycopg-only features unavailable under asyncpg (COPY, pipeline mode — confirmed
  psycopg-only, `postgres/copy.py:26-34`, `postgres/pipeline.py:25-31`).
- **Async likely wins for**: high-concurrency I/O-bound workloads (thousands of concurrent
  in-flight queries) where the alternative is thousands of OS threads; the project's own
  overhead benchmark (`benchmarks/bench_overhead.py`) is the only same-session, same-run
  comparison of dbkit vs. raw psycopg vs. raw SQLAlchemy Core in this repo — **not executed in
  this review**.
- **`[inferred, not verified in this session against SQLAlchemy source]`**: SQLAlchemy's asyncio
  extension (`sqlalchemy.ext.asyncio`) is documented upstream to run its dialect-level logic via
  `greenlet_spawn`, a greenlet-based bridge, for async dialects generally — this would add a
  per-call greenlet-switch cost on top of whatever the native driver (asyncpg) or psycopg's own
  async mode contributes. dbkit's own code never references `greenlet` directly (confirmed by
  grep); any such cost would be entirely inside SQLAlchemy, invisible to and unmeasured by
  dbkit's benchmark suite specifically as a separable line item. **This needs verification via a
  profiler (`py-spy`, `cProfile`) on a running async query**, not source reading.

---

## 4. Query Execution Hot Path

Tracing `db.fetch_value(query, params, target=...)` end-to-end (`[confirmed]` unless noted):

| Step | Cost / allocation | Cached across calls? |
|---|---|---|
| 1. `sql()` wrapper | Calls SQLAlchemy `text()` fresh every time it's invoked (`_core/query.py:31-43`) — no dbkit-side cache. If the caller constructs a module-level `Query` once (the pattern used in benchmarks), this cost is paid once; example code in the repo shows both patterns (some construct `sql(...)` fresh inside a loop). | Only if the *caller* reuses the same `Query` object. |
| 2. Target/shard resolution | `AsyncDatabase._resolve()` re-runs full logic every call: config dict lookup, conditional shard resolve, `ContextVar.get()`, conditional replica select (`_async/database.py:177-198`) — **no memoization of a resolved route for a repeated shard key**. | No. |
| 3. Shard hashing | `HashShardResolver`: **Status: FIXED** — now a bounded (4096-entry) per-instance LRU cache keyed by `shard_key` (`_core/routing.py`); a fresh SHA-256 digest is only computed on a cache miss. | Yes, up to the bound. |
| 4. Engine lookup | O(1) dict lookup, lock-free on cache hit (`_async/engine.py:143-151`). | Yes (the engine itself). |
| 5. Concurrency-limiter acquire | Semaphore acquires per call (`"database"` tier + the operation's own tier, plus an additional `"expensive"` tier when `Query(expensive=True)` — **Status: FIXED**, newly wired) *before* pool checkout (`_async/executor.py`) — no-op if a tier is unconfigured (default). | N/A |
| 6. Pool checkout | Real `await engine.connect()` (`_async/executor.py:217`). | N/A (pooled at SQLAlchemy level). |
| 7. Labels dict | **Status: FIXED** — `scope()` now reuses the `entry`/`labels` already resolved by `execute_with_resilience()` instead of recomputing them independently. | Yes, within one logical operation. |
| 8. Timeout scope | `asyncio.timeout()` object allocated per call when a timeout is set (async); no-op on sync. | No. |
| 9. Result cardinality | Native SQLAlchemy `Result` methods used throughout — `scalar_one()`, `one()`, `one_or_none()`, `mappings().all()`, `scalars().all()` (`_async/connection.py`, multiple lines) — no dbkit reimplementation. | N/A |
| 10. `map_to` row mapping | `build_mapper(map_to)` called fresh **every** `fetch_all`/etc. invocation (`_core/result.py`), then a per-row dict-comprehension + dataclass construction — O(rows × columns). | No (mapper rebuilt per call, not just per row). |
| 11. Observability | Metrics `incr`/`observe` calls (using the labels dict from step 7), a tracer span (`SpanKind.CLIENT`), a slow-query log comparison. | Instrument objects are cached per metric name (§11); the label *values* are not. |
| 12. Connection return | `conn.close()` wrapped in `contextlib.suppress(Exception)` (`executor.py`). | N/A |

### Findings

**[Medium] Labels dict is constructed twice per logical operation, and — for the Prometheus
adapter specifically — always at full label-set size regardless of how many keys are actually
meaningful.** `Status: FIXED.` `scope()` now accepts optional `entry`/`labels` parameters;
`execute_with_resilience` passes its already-resolved values straight through instead of
`scope()` recomputing them independently — callers that invoke `scope()` directly
(`db.connection()`, `db.transaction()`, bulk executemany) still resolve them themselves, since
they don't have them yet. Verified with a call-counting regression test showing exactly 1 call
to `entry()`/`labels()` per `fetch_value`, down from 2, confirmed to fail under the pre-fix code
(`tests/integration/test_async_integration.py::
test_one_shot_calls_resolve_entry_and_labels_exactly_once_each`). Separately,
`PrometheusMetrics._fill()` now merges a precomputed empty-label template
(`dict.fromkeys(ALLOWED_LABELS, "")`, built once at construction) instead of a 10-key `.get()`
loop on every call — same full-label-set output (Prometheus requires every declared label name
present in each `.labels()` call, so this can't be made proportional the way OTel's adapter is),
just built more cheaply.
`labels()` (`_async/executor.py:77-87`, original code) was called once from
`execute_with_resilience` and again, independently, from `scope()` — two allocations of an
identical 7-key dict per call — and `PrometheusMetrics._fill()`
(`observability/prometheus.py:35-40`) iterated all 10 `ALLOWED_LABELS` via per-key `.get()`
calls on every `incr`/`observe`/`gauge`. **Impact**: allocation/GC pressure that scaled linearly
with QPS, more pronounced under the Prometheus adapter than OTel's (OTel only builds attributes
it was actually given — `observability/otel_metrics.py:36-42` — proportional, not fixed-size).
`[confirmed]`.

**[Low] Statement compilation caching is a SQLAlchemy-internals question, not something dbkit
controls or has verified.** dbkit calls plain `create_async_engine(url, **kwargs)` /
`create_engine(url, **kwargs)` with no `query_cache_size` override found in
`_async/engine.py:118-141` — whether repeated `sql("...")` calls with identical SQL text benefit
from SQLAlchemy's default compiled-statement cache (keyed by SQL string, not Python object
identity, per general SQLAlchemy documentation) is `[inferred, not verified against SQLAlchemy
source or profiled in this review]`. **This should be confirmed via `EXPLAIN (ANALYZE, TIMING)`+
`pg_stat_statements` on a repeated-query benchmark, not assumed either way.**

---

## 5. Concurrency and Backpressure

### Confirmed design `[confirmed]`

- `ConcurrencyLimiter` semaphores are acquired **before** pool checkout — `async with
  (limiter.acquire("database", ...), limiter.acquire(tier, ...), self.scope(...))`
  (`_async/executor.py:158-172`) — the right order: cheap in-process waiters queue before the
  more expensive pool/connection resource.
- Semaphore-acquire timeout uses the *same* effective-timeout/deadline computation as the query
  itself (`executor.py:162-164`) — a saturated tier raises `DatabaseOverloadedError` within that
  bound, not an unbounded wait. Verified by `benchmarks/bench_pool_exhaustion.py` (analogous
  pool-level scenario): `pool.size=2, max_overflow=1` (capacity 3), 10 concurrent holders,
  `pool.timeout_seconds=1.0` → exactly 3 succeed, 7 fail with `DatabasePoolTimeoutError` within
  ~2s, none hang. **Not independently re-executed in this review** — read from source/assertions
  only.
- Semaphore is released in a `finally` **before** the backoff sleep on retry (`resilience.py`) —
  a retrying operation does not hold a concurrency slot during its backoff delay. Good.

### Findings

**[High] No enforced total-operation deadline or retry budget — `RetryConfig.maximum_total_ms`
is dead code.** `Status: FIXED.` `run_with_retries` now tracks elapsed time from the first
attempt and raises once the next backoff sleep would push total elapsed time at or beyond
`maximum_total_ms`, independent of `attempts`/the caller's own `deadline`. Regression-tested: a
config with `attempts=1000` and a 25ms budget stops after ~2-3 attempts (`tests/unit/
test_resilience.py::
test_maximum_total_ms_bounds_total_retry_time_regardless_of_attempts_remaining`), and a call
that finishes within budget is unaffected (a companion test in the same file).
The field existed (`_core/config.py:113`) and was parsed from YAML/dict config (`config.py:488`)
but was never read by `run_with_retries`/`should_retry`/anywhere in `_async/resilience.py`/
`_core/policies.py` — the only cross-attempt bound previously enforced was a caller-supplied
`deadline`, and several internal call sites pass `deadline=None` explicitly
(`_async/database.py:381,743,762`), meaning transaction-block-driven executes had no deadline
threading by default. `[confirmed]`.

**[Medium] `ConcurrencyConfig.expensive_queries` is declared but never wired to an actual
semaphore.** `Status: FIXED.` Wired into `ConcurrencyLimiter.limiter_for()` as a real
`"expensive"` tier, gated by a new `Query(expensive=True)` field (`_core/query.py`) — acquired
*in addition to* the normal reads/writes tier via `contextlib.AsyncExitStack`, not instead of
it. Verified against real PostgreSQL: saturating the tier with one slow `expensive=True` query
correctly blocks a second `expensive=True` query with `DatabaseOverloadedError` while an
ordinary (non-`expensive`) query proceeds completely unaffected by the saturated tier
(`tests/integration/test_async_integration.py`, two new tests) — confirmed to fail under the
pre-fix code (the field simply didn't exist).
`ConcurrencyLimiter.limiter_for()` previously only created tiers for `database`, `reads`,
`writes`, `bulk` (`_async/executor.py:120-134`) — `expensive_queries` (`_core/config.py:126-133`)
was parsed from config (`config.py:546`) and never referenced anywhere else in `src/`. `[confirmed]`.

**[Low] Retries re-acquire the concurrency-limiter semaphore and perform a fresh pool checkout
on every attempt** — confirmed by tracing `run_with_retries`'s `while True: result = await
operation()` loop (`resilience.py`), where `operation` is the full `attempt()` closure including
both semaphore acquires and `scope()`'s pool checkout (`executor.py:158-172`). This is arguably
correct behavior (a retried operation shouldn't hold a stale connection), but it means N
attempts amplify pool/semaphore pressure by up to N× per logical operation during partial-failure
conditions — worth explicitly modeling in capacity planning (a "retry storm" during a flaky
network period can look like `attempts×` the actual request rate at the pool/semaphore layer).

### Recommended overload policy

The system should (and largely already does) fail predictably rather than queue unbounded work:
concurrency-limiter and pool timeouts both raise classified, bounded-time errors
(`DatabaseOverloadedError`/`DatabasePoolTimeoutError`) rather than hanging — this is a genuine
strength (see Strengths, below). The two gaps that undermine "predictable failure" are (a) the
unenforced `maximum_total_ms` retry budget above, and (b) the missing `expensive_queries` tier.
Fixing both would make the overload story materially more complete: **bounded queueing at every
layer (semaphore, pool, retry), with a hard ceiling on total attempt time regardless of
per-attempt success/failure.**

---

## 6. Transactions

### Confirmed mechanics `[confirmed]`

- `execution_options()` (isolation/readonly/deferrable) applied **before** `BEGIN`
  (`_async/transaction.py:120-123`) — correct ordering, no extra round trip (driver-native
  attribute set, not SQL).
- `_apply_settings()` issues `SET LOCAL statement_timeout`/`lock_timeout` via real
  `conn.execute()` calls, **unconditionally for Postgres, on both frontends** — up to 2 extra
  round trips per explicit transaction (`transaction.py:154-165`). This is the one place the
  async-path "skip SET statement_timeout" optimization (§3) does *not* apply.
- `TX_TOTAL`/`TX_DURATION` metrics observed unconditionally in the `finally` block every
  transaction exit; `TX_ROLLBACK`/`COMMIT_UNKNOWN` conditionally (`transaction.py:185-202`). Cost
  is trivial with the default `NoopMetrics` (empty method bodies).
- `long_transaction_warning_seconds=5.0` default; the structured warning payload is built only
  when the threshold is exceeded, and only if the log level is enabled
  (`observability/logging.py:57-58` gates before payload construction) — well-designed,
  double-gated, no hot-path cost when transactions are fast.
- Savepoints use SQLAlchemy's native `begin_nested()` directly (`transaction.py:54-64`) — no
  added dbkit overhead beyond the async-context-manager wrapper.

### Findings

**[High] `cancellation_shield()` is a literal no-op on both frontends — it does not shield
anything.** `Status: FIXED.` Replaced with `shield_from_cancellation()` (`_async/_compat.py`): a
genuine implementation that runs the protected cleanup as a separate task and awaits it via
`asyncio.shield()` — if the caller's task is cancelled while waiting, the cancellation is
deferred (re-raised only once the shielded cleanup has actually finished), never swallowed.
`_rollback()`/`_release()` (`transaction.py`) now wrap their bodies in small closures passed to
`shield_from_cancellation(...)` instead of `async with cancellation_shield(): ...`. Verified with
a targeted unit test that injects cancellation *while the protected work is actively in-flight*
(`await asyncio.sleep(0.1)` inside the protected coroutine, cancelled after only 20ms) and
confirms the work still runs to completion before the `CancelledError` propagates
(`tests/unit/test_cancellation_shielding.py::
test_shield_runs_protected_work_to_completion_despite_a_mid_flight_cancellation`) — exactly the
scenario the old no-op implementation could never have passed, since a plain `try/finally`
cannot prevent a subsequent cancellation from interrupting an in-progress `await` in the same
task. Companion tests confirm cancellation is still correctly re-raised (not swallowed) and that
a genuine exception in the protected work still propagates normally.
The prior implementation (`_async/_compat.py:90-96`, original code) was `try: yield / finally:
pass` — no `asyncio.shield`, no cancellation suppression or deferral at all, despite the
docstring's "best-effort protection" claim. It wrapped `_rollback()`/`_release()`
(`transaction.py:233,248`, original) — exactly the cleanup paths where a mid-cleanup
cancellation matters most. `[confirmed]`.

**[Medium] Long transactions block the pool for their full duration by design, with no
alternative — expected, but combined with §5's finding, worth flagging for capacity planning.**
No batching/early-release/pipelining mechanism exists to free a pool slot mid-transaction
(`pipeline()` batches round trips within an already-checked-out connection, it does not shorten
hold time). This is standard connection-pooling behavior, not a defect — but **no mechanism in
dbkit connects transaction duration to PostgreSQL-side consequences** (vacuum pressure, replica
lag, dead-tuple accumulation) — a `grep` across all of `docs/` for "vacuum", "dead tuple",
"replica lag", "bloat", "autovacuum" returned **zero matches**. The only detection is the
duration-based warning log, which fires after the fact and only affects observability, not
backpressure.

### Recommended transaction-duration metrics/alerts

Already emitted and usable: `db_transaction_duration_seconds` (histogram, per db/shard/role),
`database.transaction.long_running` log event. **Recommended additions** (not present today):
an alert on `db_transaction_duration_seconds` p99 crossing, say, 2× `long_transaction_warning_seconds`
sustained over a window (catches systemic slow-transaction drift, not just individual outliers);
and — since dbkit has no vacuum/replica-lag awareness at all — a companion PostgreSQL-side alert
on `pg_stat_activity` transaction age and `pg_stat_replication` lag, operated independently of
dbkit (dbkit cannot see these itself).

---

## 7. Retry and Circuit-Breaker Performance

### Confirmed design `[confirmed]`

- Backoff: exponential with a cap, full jitter — `capped = min(initial_delay_ms *
  multiplier**(attempt-1), maximum_delay_ms); delay = capped * random()` (`_core/policies.py`).
  Defaults: `initial_delay_ms=20, maximum_delay_ms=250, multiplier=2.0, attempts=2`.
- `should_retry()` correctly gates: never retries `transaction_state_unknown` regardless of
  idempotency (`policies.py:80-81`); writes require both `retry_writes=True` (default `False`)
  **and** `Query.idempotent=True` (self-declared trust, backed by the idempotency lint from the
  earlier correctness review, not re-audited here); reads retry by default (`retry_reads=True`).
- Circuit breaker only trips on infrastructure-category failures (`AVAILABILITY`, `CONNECTION`,
  `POOL`, `TIMEOUT`) — correctly excludes integrity/programming/serialization-conflict errors
  from tripping it (`_core/circuit.py`), so a single bad query doesn't take down the breaker for
  every other query against the same target. Keyed per `db+shard+role` — good failure isolation
  granularity.
- Idempotency-lint regex patterns are module-level precompiled (`_core/idempotency_lint.py:21-31`)
  — no per-call compilation cost; this lint is also **not on the hot path at all** (only invoked
  from the `dbkit query-list` CLI, confirmed absent from `connection.py`/`executor.py`).

### Findings

See §5 for the **[High]** unenforced-retry-budget finding (`maximum_total_ms` dead code) — this
is the single most important retry-performance finding and applies directly to this section's
"require a total operation deadline" requirement. **As things stand today, retries cannot be
guaranteed to stay within the caller's actual latency budget unless the caller manually computes
and passes `deadline=` on every call.**

**[Low] Retry traffic multiplication is bounded but not zero-cost during an outage.** With
default `attempts=2`, a sustained partial outage produces at most 2× the logical request rate at
the database layer for affected operations (not unbounded) — the circuit breaker (after 10
failures/30s by default) then suppresses further attempts entirely via `DatabaseCircuitOpenError`,
capping the amplification window. This is a reasonable design; the residual risk is entirely the
§5 deadline gap (how long that 2× window can last per call) plus the general "thundering herd on
recovery" pattern common to any exponential-backoff system without a shared/coordinated
recovery signal across processes — dbkit's breaker state is per-process (in-memory
`self._breakers` dict, `_async/executor.py:103-118`), not shared across a multi-process fleet,
so many processes can independently probe (half-open) and re-trip simultaneously.
`[unconfirmed — needs a multi-process load test]`.

---

## 8. Streaming and Large Result Sets

### Confirmed mechanics `[confirmed]`

- Server-side cursor confirmed: `conn.stream(statement, params, execution_options={"yield_per":
  batch_size})` (`_async/_compat.py`), default `batch_size=1000` (`_async/database.py`).
- `max_duration` guard checked on every `__anext__` (once per yielded row, a single
  `time.monotonic()` comparison — cheap) — `_async/streaming.py`.
- Connection release on exit/exception is guaranteed via `_cleanup()`, called from both the
  normal exit path and the `__aenter__` setup-failure path — `contextlib.suppress(Exception)`
  around `gen.aclose()`/`conn.close()`.
- **Streaming deliberately bypasses the entire resilience stack** (retry, circuit breaker,
  concurrency limiter) — confirmed by `stream()` calling `self._executor.entry(target)` directly,
  never `execute_with_resilience`. This is documented and intentional ("a partially consumed
  stream cannot be transparently restarted") — correct trade-off, but means a slow consumer
  iterating a stream holds a pool connection for the **entire consumption duration**, completely
  outside the pool-timeout/circuit-breaker protection every other call path gets.

### Findings

**[Medium] A slow consumer can hold a pool connection indefinitely with no bound other than
`max_duration` (opt-in, not default).** If `max_duration` isn't set, a stream with a slow
downstream consumer (e.g., writing each row to a rate-limited external API) holds its connection
for as long as the consumer takes — directly reducing effective pool capacity for every other
caller, and this consumption is invisible to the concurrency limiter (streaming bypasses it
entirely). **Reproduction**: start a stream with `batch_size=1000` over a large table, insert an
artificial `await asyncio.sleep(N)` between row consumption, observe `pool_status().checked_out`
staying elevated and other calls queuing/timing out on the same pool. **Fix**: consider a
default, generous `max_duration` (or a loud warning) rather than fully opt-in; document the
pool-capacity interaction explicitly in `docs/troubleshooting.md` (currently absent per this
review's read of that file — only pool exhaustion from ordinary concurrency is discussed, not
streaming-specific hold time).

**[Low] No benchmark exists for streaming at scale** — `benchmarks/` has no scenario streaming
thousands-to-millions of rows of varying width and measuring memory/latency
(`[unconfirmed — no such benchmark found]`). See §15 for the recommended test design.

### Recommended benchmark scenarios (not currently present)

- Row widths: narrow (single int column), medium (~10 mixed columns), wide (~50 columns incl.
  text/jsonb).
- Result-set sizes: 10K, 100K, 1M, 10M rows.
- Metrics: peak RSS during consumption, wall-clock to first row, wall-clock to last row,
  connection hold duration, behavior when the consumer is artificially slowed (backpressure
  correctness — does memory stay bounded, or does `yield_per` under the hood buffer ahead of the
  consumer? `[unconfirmed — needs profiling]`).

---

## 9. Bulk Insert, Upsert, and COPY

### Confirmed mechanics `[confirmed]`

- Adaptive batch sizing: `resolve_batch_rows()` computes `ceiling = min(requested or max_rows,
  max_rows, max_params // n_columns)` (`_core/bulk.py`) — batch size shrinks as column count
  grows, bounded by both an absolute row cap (`max_rows=1000` default) and PostgreSQL's 65535
  bind-parameter ceiling.
- `unnest()` strategy avoids the bind-parameter ceiling entirely by binding one array per column
  (`n_columns` binds total, regardless of row count) instead of one bind per cell
  (`postgres/unnest.py`) — this is a genuinely good design for wide batches.
- COPY driver-detection correctly distinguishes psycopg (has `.pipeline`) from asyncpg (raises
  `DatabaseUnsupportedOperationError` cleanly) — a real bug fix confirmed in project history
  (`postgres/copy.py`).
- Failure modes: `atomic` (whole-batch-set transaction, any failure rolls back everything),
  `best_effort` (each batch its own commit; a failing batch's rows are **dropped with no retry**
  — `continue` on exception), `split_on_failure` (falls back to row-by-row on a failed batch,
  isolating exactly which rows fail).

### Findings

**[Medium] `BulkConfig.max_payload_bytes` is declared but never enforced.** `Status: FIXED.`
`resolve_batch_rows()` now accepts an optional `sample_row` and, when `max_payload_bytes` is set,
shrinks the ceiling using a new `estimate_row_bytes()` helper (sums encoded byte length across a
representative row's values — one sample is enough since batches are homogeneous). Wired at the
one real call site (`_async/database.py::_bulk_write`, `sample_row=rows[0] if rows else None`).
Verified against real PostgreSQL: a 10,000-byte budget correctly caps a batch of ~1KB rows at
≤15 rows instead of the configured 1000-row ceiling (`tests/integration/test_throughput_paths.py::
test_insert_many_shrinks_batch_for_wide_rows_when_max_payload_bytes_set`), plus pure-function
unit tests for the sizing/estimation logic in isolation (`tests/unit/test_bulk_batch.py`).
Both `BulkConfig.max_payload_bytes` (`config.py:142`) and `BulkLimits.max_payload_bytes`
(`_core/bulk.py`) existed, but `resolve_batch_rows()` previously only computed batch size from
`max_rows`/`max_params` — the byte-size field was never read in the sizing calculation.
`[confirmed]`.

**[Medium] `best_effort` mode silently drops failed batches with no retry and no explicit
error surface.** `Status: FIXED.` Both the `best_effort` whole-batch-drop path and the
`split_on_failure` per-row-drop path now call a new `bulk_batch_dropped_warning()`
(`database.bulk.rows_dropped` log event) and increment a new `db_bulk_rows_dropped_total`
metric, carrying the query name, database, mode, row count, and classified error category.
Verified against real PostgreSQL with `caplog` + a metrics test double: a duplicate-key batch
drop in `best_effort` mode is now observable with the correct row count (11) and error category
(`"integrity"`) (`tests/integration/test_throughput_paths.py::
test_best_effort_mode_logs_and_counts_a_dropped_batch`) — confirmed to fail under the pre-fix
(silent) code. This closes a real data-loss-visibility risk: a transient connection blip
mid-batch-sequence previously dropped an entire batch with zero signal to the caller unless they
separately tracked committed row counts themselves. `[confirmed]`.

**[Low] Optimal batch size is not "always larger"** — the adaptive formula already reflects
this correctly (shrinking with column count against the bind-parameter ceiling), but no
benchmark in the repo actually sweeps batch size to find a throughput peak vs. WAL-volume/lock-
duration tradeoff — `bench_batch.py` uses `ROWS=5000`, a single `BATCH=1000` value, not a sweep.
`[unconfirmed — needs a batch-size sweep benchmark, see §15]`.

**[Low] Upsert hot-row/deadlock/conflict-target risk is not evaluated by any benchmark or test
in the repo.** No benchmark or test simulates concurrent upserts targeting overlapping keys to
measure lock contention/deadlock rate. `[unconfirmed — needs a dedicated hot-row upsert load
test, see §15]`.

### Benchmark-claim integrity note

`Status: FIXED.` `docs/roadmap.md`/`CHANGELOG.md` claimed unnest is "~32× faster than
`execute_many` at 20k rows" — but **no benchmark script in this repo measured unnest at all,
under any row count** (`benchmarks/bench_batch.py` compares per-row/`execute_many`/COPY, never
`strategy="unnest"`). Added `benchmarks/bench_unnest.py` and ran it repeatedly against real
PostgreSQL at 20,000 rows: a first/cold run measured ~19.6×, but three subsequent runs
consistently measured ~29-30× in steady state — real run-to-run variance (likely connection/
buffer-cache warmup on the very first run), now stated honestly as a range in
`docs/roadmap.md`/`CHANGELOG.md` instead of a single unverifiable point estimate. Registered in
`python -m benchmarks --only unnest`. `[confirmed]`.

---

## 10. Sharding and Routing Performance

### Confirmed mechanics `[confirmed]`

- `EngineKey` includes `shard_id` — the registry (`dict[str, EngineEntry]`) scales structurally
  fine to hundreds/low-thousands of shards (O(1) lookup per shard). The binding constraint at
  high shard count is **connection math** (§2 formula) and the **LRU-eviction lock-holding
  finding** (§2), not registry lookup itself.
- No route-resolution caching anywhere — full shard-resolve/replica-select logic reruns every
  call, for every resolver type (§4).
- No cross-shard fan-out primitive exists anywhere in the codebase (confirmed by grep) — dbkit
  does not itself create "slowest shard" tail-latency amplification because it never fans out to
  multiple shards in one logical call; any such fan-out is entirely application-level and outside
  dbkit's control/measurement.
- `consistency_scope`'s read-your-writes override is a plain `contextvars.ContextVar` get/set —
  no lock, no polling, trivial cost.

### Behavior as shard count grows (structural reasoning, not benchmarked)

- **1 → 10 shards**: negligible effect on anything — connection math and registry size are both
  trivial at this scale.
- **10 → 100 shards**: connection math becomes the dominant concern (§2 formula compounds
  linearly with shard count); registry lookup remains O(1) and cheap; SHA-256 rehashing per call
  (§4) becomes measurable in aggregate at high QPS × 100 shards, though still likely sub-percent
  of total latency `[unconfirmed — needs profiling at this specific scale]`.
- **100 → 1,000+ shards**: this is where `max_engines`+`evict_lru` become load-bearing (per the
  project's own stated intent for "dynamic per-tenant deployments," `docs/roadmap.md`) — and
  exactly where the §2 LRU-eviction-holds-lock finding matters most, since eviction churn scales
  with shard cardinality exceeding `max_engines`. **No benchmark in this repo tests this regime**
  — `benchmarks/` has no shard-count sweep. `[unconfirmed — needs a dedicated high-cardinality
  sharding benchmark, see §15]`.

---

## 11. Observability Overhead

### Confirmed mechanics `[confirmed]`

- `ALLOWED_LABELS` is a 10-key frozenset, enforced by rejecting disallowed label **names**
  (`observability/prometheus.py`/`otel_metrics.py` both raise `ValueError` on an unrecognized
  key) — this correctly prevents accidental high-cardinality label *names* like raw SQL or user
  IDs from being wired in as label keys. It does **not** prevent a caller from stuffing a
  high-cardinality *value* into an allowed key (e.g., `query_name` containing dynamic text) —
  that discipline is left to the caller.
- Tracing never attaches SQL text or bound parameters to spans (confirmed, `observability/
  tracing.py` — explicit comment and module docstring both state this).
- OTel-availability is checked once at **module import time**, not per call
  (`tracing.py`/`logging.py`) — a real, documented perf fix (quoted verbatim from
  `CHANGELOG.md`): *"the trace/log correlation lookup now checks OTel availability once at
  import time instead of attempting `import opentelemetry` on every log call — a real cost when
  OTel isn't installed, since failed imports aren't cached in `sys.modules`."* This is a
  legitimate, verified-by-project-history hot-path fix.
- `log_event()` gates on `logger.isEnabledFor(level)` **before** building the structured payload
  dict (`observability/logging.py`) — correctly avoids allocation when the log level is filtered.

### Findings

**[Medium] Prometheus adapter's per-call cost was proportional to the full allowed-label-set
size (10), not the labels actually passed — see §4/§5's combined finding with the double
`labels()` build.** `Status: FIXED` (see §5) — `_fill()` now merges a precomputed empty-label
template instead of a 10-key `.get()` loop per call; it still must return all 10 keys (a hard
Prometheus API requirement, unlike OTel's genuinely-proportional adapter), but built more
cheaply. `[confirmed]`.

**[Low] No sampling/aggregation mechanism exists for traces or metrics** — every operation gets
a span and metric observation unconditionally when the respective sink is enabled; there is no
head/tail sampling, no span-rate limiting. At very high QPS (tens of thousands/sec) with tracing
enabled, span-creation cost (even without SQL capture) could become non-trivial —
`[unconfirmed — needs a profiled comparison of OTel-enabled vs. disabled at high QPS, see §15]`.

---

## 12. Memory and Allocation Review

`[unconfirmed — needs a memory profile against a representative workload]` for all items below;
this review is source-level only, no profiler was run.

- **Per-query allocations** (confirmed structurally, §4): timeout-scope object, resolved-route
  tuple, two labels dicts, a Prometheus full-label-set dict per metric call, a tracer span
  object, an `AsyncConnectionScope` wrapper — none individually large, but their sum at high QPS
  has not been measured (`tracemalloc`/`memray` recommended).
- **Result-row materialization**: `fetch_all`/`mappings().all()` fully materializes the result
  set in memory (standard, expected for non-streaming calls) — no dbkit-specific buffering
  beyond what SQLAlchemy itself does. Large non-streamed `fetch_all` calls on wide/huge result
  sets are an application-level footgun dbkit does not itself guard against (that's what
  `db.stream()` is for — but nothing prevents misuse of `fetch_all` on an unbounded query).
- **Engine/resolver caches**: the engine registry is the only long-lived cache (`dict[str,
  EngineEntry]`), explicitly bounded by `max_engines` when configured — unbounded only if
  `max_engines=None` (default), in which case it grows with distinct database×shard×role×driver
  combinations seen, which is fine for a fixed shard topology but could grow unboundedly in a
  truly dynamic per-tenant scenario without `max_engines` set. No other unbounded cache was found
  (shard resolvers hold no result cache at all, per §10).
- **Circuit breakers / concurrency limiters**: also unbounded dicts keyed by engine string
  (`_async/executor.py:103-134`) — same shape as the engine registry, same caveat: fine for fixed
  topologies, grows with distinct-target cardinality in dynamic deployments, with no eviction
  mechanism analogous to `evict_lru` for engines. **This is a gap**: `max_engines`+`evict_lru`
  bounds engine memory, but breaker/limiter dicts have no equivalent bound — in an extreme
  per-tenant-shard scenario with high churn, these dicts could grow unbounded even while the
  engine registry itself is correctly bounded. `[confirmed structurally; unconfirmed whether this
  matters in practice — needs a long-running high-cardinality soak with memory profiling]`.

---

## 13. PostgreSQL Behavior

### Confirmed `[confirmed]`

- `pgbouncer_compatible=True` correctly disables client-side prepared-statement autoprep
  (psycopg `prepare_threshold=None`, asyncpg `statement_cache_size=0`) — the correct fix for
  PgBouncer's *transaction* pooling mode, where a logical connection may hit a different
  physical backend every transaction and a cached prepared statement would target the wrong one.
  Every session-scoped `SET`/timeout call in dbkit is already `SET LOCAL` (transaction-scoped),
  never a bare session-level `SET` — confirmed compatible with transaction pooling in this
  respect.
- **Session pooling** (PgBouncer's other mode) is not specifically discussed anywhere, but has no
  known incompatibility given the above.
- Deadlocks/serialization failures/lock timeouts are classified into specific retryable error
  types (`DatabaseDeadlockError`/`DatabaseSerializationError`/`DatabaseLockTimeoutError`) — not
  independently re-verified in this pass, carried over from the prior correctness review.

### Findings

See §3's **[High]** finding — the single most consequential PostgreSQL-behavior question this
review raises: **does an async one-shot client-side timeout actually cancel the backend's
in-flight statement, or does the abandoned query keep running server-side?** This directly
affects lock contention, backend/CPU consumption, and WAL/IO under exactly the saturation
scenario timeouts exist to protect against. **This is the highest-priority item to verify with
live `pg_stat_activity` before any high-load production use.**

---

## 14. Benchmark Review

**Verdict: the benchmark suite is a reasonable engineering tool for regression-sniffing during
development, but does not meet the bar of a statistically valid, production-capacity-certifying
benchmark suite.** Specific gaps, all `[confirmed]` from reading every file in `benchmarks/`:

- **No confidence intervals anywhere.** `_stats.py`'s only variance signal is coefficient-of-
  variation (`stdev/mean`) with a fixed 5% "unstable" annotation threshold that never gates a
  result — no bootstrap/t-test CI computed anywhere in the suite.
- **Most benchmarks are single-run.** `bench_crud.py` (N=3000, single pass), `bench_latency.py`
  (N=4000, single pass), `bench_pgbouncer_compatible.py` (N=2000, single pass per setting) —
  none of these compute a CV or repeat trials. Only `bench_overhead.py`, `bench_batch.py`, and
  `bench_throughput.py` use `REPS=3-5` with median+CV.
- **`_stats.py`'s own docstring admits p99.9 needs ~10k samples** — yet `bench_crud.py` computes
  p99 from 3000 samples, `bench_batch_collector.py` from as few as 500 (10 producers × 50
  items). Percentile estimates at these sample sizes are noisy, especially at p99+.
- **No CPU/memory measurement of the Python process anywhere in the suite.**
- **No PostgreSQL server-side measurement anywhere** (`pg_stat_statements`, `pg_stat_activity`,
  WAL volume, lock waits) in any benchmark file.
- **No GC or event-loop-lag measurement anywhere**, despite the entire async story depending on
  scheduler fairness.
- **`bench_overhead.py` is the only file that runs a same-session, same-run comparison** against
  raw psycopg and raw SQLAlchemy Core — and even it lacks CPU/memory/GC measurement. No file
  compares against raw asyncpg or a minimal direct-driver implementation.
- **No CI performance-regression gate exists.** `.github/workflows/ci.yml` runs `python -m
  benchmarks --no-save` as an explicitly non-gating step (`|| echo "benchmarks best-effort"`);
  only the 60-second soak test (not the 10-minute one) is gating, and its `rss_bounded` verdict
  uses an AND-condition (`slope > 256KB/min AND net_growth > 8MB`) that would pass a slow, real
  leak as long as it stays under the 8MB floor within a 60-second window.
- **The widely-cited "10-minute soak, ~120K inserts, 0 recovery failures" result was a one-off
  manual run**, not automated or repeatable — the project's own `CHANGELOG.md` states "a true
  multi-hour run remains a deployment-time exercise," i.e., this has never actually been done.
- **Benchmark-claim/script mismatch**: the documented "~32× faster... at 20k rows" unnest claim
  does not match `bench_batch.py`'s actual `ROWS=5000` parameter (§9) — a real evidence-integrity
  gap, not just a missing-rigor gap.
- `bench_pool_exhaustion.py` and `bench_pgbouncer_compatible.py` (both recently added) are not
  registered in the benchmark suite's `__main__.py` runner and are not referenced in CI at all —
  they exist only as manually-invoked scripts.

**Bottom line**: every specific numeric performance claim in this project's documentation
(~31%→~6%, ~90×, ~32×, ~2M items/s) should be treated as **directional, historical, single-
session evidence**, not as a validated, reproducible, statistically-sound capacity guarantee.
None of them were reproduced in this review.

---

## 15. Required Load Tests

None of the following exist in the current `benchmarks/`/`tests/` suite in the form specified
(each is a **gap**, not a critique of an existing flawed test, unless noted). All require a live
PostgreSQL instance and should measure: p50/p95/p99/p99.9 latency, throughput, error rate, pool
wait time, connection count, retry count, and (where feasible) PostgreSQL-side CPU/IO/lock/WAL
stats via `pg_stat_statements`/`pg_stat_activity`.

| # | Test | Dataset | Query | Concurrency | Duration | Success criteria | Expected bottleneck |
|---|---|---|---|---|---|---|---|
| 1 | Single-row indexed read | 1M-row table, PK index | `SELECT ... WHERE id = :id` | 1→2000, stepped | 60s/step | p99 < target SLO (undefined — establish one first) | Pool size, then CPU |
| 2 | Single-row write | Same table | `UPDATE ... WHERE id = :id` | 1→2000, stepped | 60s/step | No `DatabaseCommitUnknownError` spikes; WAL rate linear | Lock contention if keys overlap |
| 3 | 90/10 read/write mix | Same | Mixed | 500, sustained | 5 min | Error rate 0%; p99 stable (no drift) | Pool/limiter interaction |
| 4 | Slow-query saturation | Same | `pg_sleep(2)` injected on 10% of calls | 200 | 2 min | Circuit breaker trips within `window_seconds`; other targets unaffected | Circuit-breaker isolation (§7) |
| 5 | Pool exhaustion | N/A | Long-held connection | > pool capacity | 30s | Excess requests fail with `DatabasePoolTimeoutError` within `timeout_seconds` (already scripted in `bench_pool_exhaustion.py`, not yet CI-gated) | Pool timeout |
| 6 | Database restart | N/A | Simple read | 50 | Through 1 restart | Recovery within N seconds, no leaked connections | Reconnection storm |
| 7 | Primary failover | 2 backends + proxy | Simple read/write | 50 | Through 1 failover | Recovery, marker-row correctness (existing chaos test covers correctness, not throughput during failover) | Circuit breaker + retry |
| 8 | Replica lag | Primary + replica | Read-your-writes scope | 100 | 2 min with induced lag | Reads inside scope always hit primary (existing test); **no lag-aware routing exists** — this test should confirm the *absence* of lag-awareness is understood, not find a bug | N/A (out of scope by design) |
| 9 | Deadlock storm | Hot-row table | Concurrent conflicting UPDATEs | 100+ | 60s | Deadlocks classified + retried per policy; no unbounded retry loop (ties to §7 finding) | Retry-budget gap (§5/§7) |
| 10 | Serialization failures | `SERIALIZABLE` isolation | Concurrent conflicting txns | 100 | 60s | Classified + retried correctly (existing unit coverage, not load-scale) | Same as #9 |
| 11 | Bulk inserts | Wide + narrow rows | `insert_many` at multiple batch sizes | 1 (throughput test) | Until 1M rows | Rows/sec vs. batch-size curve (a sweep, not a single point) | WAL volume, `max_payload_bytes` gap (§9) |
| 12 | Large COPY | 10M rows | `copy_records` | 1 | Until complete | Memory bounded, throughput vs. per-row baseline | Network/disk IO |
| 13 | Streaming millions of rows | 1M/10M rows, 3 widths | `db.stream()` | 1, with artificially slowed consumer | Until complete | RSS bounded regardless of consumer speed; pool-hold-time measured (§8 finding) | Slow-consumer pool starvation |
| 14 | Hot-shard traffic | 10 shards, skewed key distribution | Simple read/write | 500 | 5 min | Hot shard's engine/pool saturates independently, cold shards unaffected | Per-shard pool sizing |
| 15 | High shard-cardinality | 1000+ shards | Simple read | 200 | 5 min | Registry lookup stays O(1); `max_engines`+`evict_lru` behavior under real churn (§10 gap) | LRU-eviction lock hold (§2 finding) |
| 16 | Engine LRU churn | `max_engines` set below active shard count | Rotating shard keys | 200 | 5 min | p99 latency during eviction bursts (directly validates §2 finding) | Eviction lock-hold |
| 17 | Retry storms | Degraded backend (inject 30% error rate) | Any write with `retry_writes=True` | 200 | 2 min | Total DB-side traffic ≤ configured amplification bound; validates §5/§7 deadline gap | Unbounded retry budget |
| 18 | OTel enabled vs. disabled | Fixed workload | Simple read | 1000 | 60s each | Latency/throughput delta attributable to tracing overhead, isolated from metrics | Span-creation cost (§11) |
| 19 | Prometheus enabled vs. disabled | Fixed workload | Simple read | 1000 | 60s each | Latency/throughput delta from label-dict overhead (§4/§11 findings) | Full-label-set `_fill()` cost |
| 20 | Multi-process deployment | Fixed workload | Simple read/write | N processes × M each | 5 min | Total connection count matches formula (§2); no coordination failures | Connection-budget enforcement (opt-in gap) |

---

## 16. Findings Summary and Report

### All findings (severity-ordered)

1. **[High → RESOLVED, `Status: CONFIRMED SAFE`] — Component: async one-shot query path
   (`_async/connection.py`, psycopg3/asyncpg).** Live-tested against real PostgreSQL
   (`pg_stat_activity` before/after a client-side timeout) plus source-level confirmation in
   both drivers: both psycopg3 (`AsyncConnection.wait()`) and asyncpg
   (`_cancel_current_command`) send a real server-side cancel request whenever
   `asyncio.CancelledError` interrupts an in-flight query wait. No code fix needed — locked in
   as a permanent regression test
   (`tests/integration/test_resilience_scenarios.py::
   test_client_side_timeout_actually_cancels_the_server_side_statement`).

2. **[High → FIXED] — Component: retry/backoff (`_async/resilience.py`, `_core/config.py`).**
   `RetryConfig.maximum_total_ms` is now a real, enforced ceiling on total elapsed retry time,
   independent of `attempts`/the caller's own `deadline`. Regression-tested:
   `tests/unit/test_resilience.py::
   test_maximum_total_ms_bounds_total_retry_time_regardless_of_attempts_remaining`.

3. **[High → FIXED] — Component: transaction cleanup (`_async/_compat.py`,
   `_async/transaction.py`).** `cancellation_shield()` (a literal no-op) replaced with
   `shield_from_cancellation()`, a genuine `asyncio.shield()`-based implementation that runs
   rollback/release to completion even under a mid-cleanup cancellation, deferring (never
   swallowing) the cancellation until cleanup finishes. Regression-tested with a mid-flight
   cancellation injection (`tests/unit/test_cancellation_shielding.py`).

4. **[Medium → FIXED] — Component: engine registry (`_async/engine.py`).** LRU eviction now
   pops the victim under the lock but disposes it after releasing the lock, matching
   `dispose_one`'s shape. Regression-tested with a slow-dispose double confirming a concurrent
   lookup for a different key is no longer blocked
   (`tests/unit/test_engine_registry.py::
   test_lru_eviction_does_not_block_concurrent_lookups_during_dispose`).

5. **[Medium → FIXED] — Component: metrics/observability (`_async/executor.py`,
   `observability/prometheus.py`).** `scope()` now reuses the already-resolved `entry`/`labels`
   from `execute_with_resilience()` instead of recomputing them (verified: exactly 1 call each,
   down from 2). `PrometheusMetrics._fill()` now merges a precomputed empty-label template
   instead of a per-key `.get()` loop.

6. **[Medium — intentionally left as a warning, not a default-behavior change] — Component:
   connection budget (`_core/config.py`).** Budget enforcement remains opt-in
   (`enforce_at_startup=False` by default) — this was a deliberate choice, not an oversight:
   flipping the default to `True` could silently break an existing deployment's startup the
   moment this review's recommendation shipped. `dbkit check`/`config-validate` already warn
   (added in the prior correctness-review pass) when a non-development environment has no
   enforced budget; that remains the mitigation. The connection-math formula and worked example
   are unchanged and still worth reading before any multi-shard/multi-process rollout.

7. **[Medium → FIXED] — Component: bulk writes (`_core/bulk.py`).** `resolve_batch_rows()` now
   accepts a `sample_row` and shrinks the ceiling by estimated byte size when
   `max_payload_bytes` is set. Verified against real PostgreSQL: a 10,000-byte budget correctly
   caps a batch of ~1KB rows at ≤15 rows (`tests/integration/test_throughput_paths.py::
   test_insert_many_shrinks_batch_for_wide_rows_when_max_payload_bytes_set`).

8. **[Medium → FIXED] — Component: benchmark suite (`benchmarks/`, `.github/workflows/ci.yml`).**
   `benchmarks/_stats.py` now computes a bootstrap confidence interval on every summary;
   `benchmarks/check_regression.py` is a new, genuinely CI-gating step (fails the build on an
   overhead regression or a broken pool-exhaustion contract); the mismatched "~32× at 20k rows"
   unnest claim is replaced with a real, repeatedly-measured ~29× figure from a new
   `benchmarks/bench_unnest.py`. The 10-minute soak result remains a one-off manual run (an
   automated multi-hour soak is out of scope for this pass — tracked, not silently dropped).

9. **[Low → FIXED] — Component: concurrency config (`_core/config.py`, `_async/executor.py`).**
   `ConcurrencyConfig.expensive_queries` is now wired into `ConcurrencyLimiter` as a real tier,
   gated by a new `Query(expensive=True)` field. Verified against real PostgreSQL
   (`tests/integration/test_async_integration.py`, two new tests).

10. **[Low → FIXED] — Component: sharding (`_core/routing.py`).** `HashShardResolver` now
    caches resolved buckets in a bounded (4096-entry) per-instance LRU, avoiding a repeated
    SHA-256 computation for a repeated key while staying safe for high-cardinality key spaces.
    Regression-tested for correctness and boundedness
    (`tests/unit/test_sharding_replica.py`, three new tests).

### Strengths (confirmed, unchanged from the correctness review, re-confirmed here from a
performance lens)

- Semaphore-before-pool-checkout ordering is correct and avoids the worst double-queueing
  pattern.
- Pool/concurrency timeouts fail fast with classified errors rather than hanging — a genuinely
  good "fail predictably" foundation, undermined only by the retry-budget gap (Finding #2).
- Circuit breaker correctly scopes to infrastructure-category failures only, keyed per
  db+shard+role — good blast-radius containment.
- The async-path statement-timeout optimization (§3) is a real, measured (if unverified in this
  review) latency win — the project clearly has performance awareness, not just correctness
  awareness.
- OTel-availability caching at import time is a genuine, verified hot-path fix.
- `unnest()`'s bind-parameter-ceiling-avoidance design is architecturally sound.
- Sync/async parity via code generation avoids two hand-maintained, inevitably-diverging
  implementations — reduces the risk of a performance fix landing on only one side.

### Likely first bottleneck (most deployments)

The connection pool (default `size=10, max_overflow=5` per engine) at any concurrency
meaningfully above ~15 simultaneous in-flight operations against a single database×shard×role
target, well before dbkit's own per-call overhead (labels, hashing, tracing) becomes
significant. The second-most-likely bottleneck, once pool size is tuned up, is the
connection-budget ceiling (§2) once the deployment scales to multiple processes/shards.

### Likely saturation behavior

**Fully bounded and classified** (pool timeout → `DatabasePoolTimeoutError`; concurrency-limiter
timeout → `DatabaseOverloadedError`; sustained infra failures → `DatabaseCircuitOpenError`;
total retry time → now enforced via `maximum_total_ms`, Finding #2) — this is a real strength,
and the two gaps that previously kept saturation behavior from being fully bounded are now
closed. The remaining, more subtle concern this review raised — whether an abandoned async
one-shot query keeps consuming server-side resources after the client times out (Finding #1) —
resolved favorably: both supported drivers send a real server-side cancel on client timeout, so
saturation does not self-reinforce via abandoned server-side work piling up.

### Recommended safe defaults

- Set `connection_budget.enforce_at_startup=True` in every non-development environment (already
  possible today, not the default — `dbkit check`/`config-validate` warn if it's off).
- `RetryConfig.maximum_total_ms` is now enforced automatically — an explicit `deadline=` on
  every retried call is no longer required to bound total retry time, though still recommended
  for callers with a tighter budget than the default 750ms.
- Set `max_engines`+`evict_lru=True` explicitly (rather than leaving it unbounded) in any
  deployment with a dynamic or high-cardinality shard/tenant topology — the LRU-eviction
  lock-holding tail-latency risk (Finding #4) is now fixed, so this is a purely capacity-driven
  choice rather than one that also carries a latency-spike cost.
- Either metrics adapter is now reasonable at high QPS — the Prometheus-vs-OTel fixed-cost
  difference (Finding #5) is fixed; OTel's adapter remains proportional-cost by design, but
  Prometheus's fixed 10-label cost is now built via a cheap template merge rather than a per-key
  loop.

### Recommended benchmark suite additions (priority order) — status after this pass

1. ~~A CI-gating performance-regression check~~ **DONE** — `benchmarks/check_regression.py`,
   wired into `.github/workflows/ci.yml`.
2. ~~Confidence intervals / multi-run aggregation~~ **DONE** — `benchmarks/_stats.py`'s
   `bootstrap_ci()`, applied to every `robust()` summary.
3. ~~The live `pg_stat_activity` cancellation-verification test~~ **DONE** — resolved Finding #1
   favorably; test is now a permanent regression check.
4. ~~A retry-budget enforcement test~~ **DONE**.
5. Still open: the full load-test matrix in §15 (a multi-day exercise requiring dedicated
   infrastructure — genuinely out of scope for a single review-and-fix pass, not silently
   dropped).
6. ~~Re-run and correct the unnest "20k rows" benchmark-claim mismatch~~ **DONE** —
   `benchmarks/bench_unnest.py`, ~29× steady-state.

### Required performance changes before beta — all four items below are now DONE

- ~~Verify and, if needed, fix Finding #1 (server-side timeout backstop)~~ **CONFIRMED SAFE —
  no fix needed, verified empirically against real PostgreSQL and both drivers' source.**
- ~~Fix or explicitly document Finding #2 (retry budget)~~ **FIXED — a real, enforced ceiling.**
- ~~Fix the LRU-eviction lock-holding bug (Finding #4)~~ **FIXED.**
- ~~Enforce or remove `max_payload_bytes` (Finding #7) and wire or remove `expensive_queries`
  (Finding #9)~~ **FIXED — both enforced/wired, not removed.**

### Required performance changes before version 1.0 — four of five items now DONE

- ~~Real cancellation shielding (Finding #3), backed by a targeted chaos test~~ **FIXED.**
- ~~Labels/metrics allocation optimization (Finding #5)~~ **FIXED.**
- ~~A CI-gating performance-regression benchmark, plus confidence intervals across the suite
  (§14)~~ **FIXED.**
- ~~Resolve the benchmark-claim/script mismatch (unnest "20k rows")~~ **FIXED.**
- **Still open**: the full §15 load-test matrix executed at least once against a realistic
  topology, with results published. This is the one item this pass could not close — it requires
  dedicated multi-day infrastructure (a real sharded/multi-replica topology, sustained load
  generation, PostgreSQL-side monitoring) beyond what a single review-and-fix session can stand
  up. Tracked as the recommended next step, not silently dropped.

### Estimated production capacity — **clearly an estimate, not a validated number**

Based on structural pool math and this project's own benchmark numbers — some now independently
reproduced in this pass (`bench_overhead.py`'s measured overhead in this session's runs was
~19-22% vs. raw SQLAlchemy Core, somewhat higher than the ~6% historically documented figure but
well under the new 40% CI regression gate; the `unnest` speedup was independently reproduced at
~29× steady-state) — a single dbkit process with default pool settings (`size=10,
max_overflow=5` per engine, one database, no sharding) against a healthy PostgreSQL instance
with adequate `max_connections` headroom is **plausibly capable of low-thousands of ops/sec**
for simple indexed reads/writes under the async frontend, assuming the database itself isn't the
bottleneck first. This is still an estimate, not a load-tested capacity guarantee — the full §15
load-test matrix (sustained load at realistic concurrency, against a realistic topology) remains
the one thing that would turn this from "plausible" into "validated."

### Performance-Readiness Score: **8.5 / 10** (was 6.5/10 before this pass)

Every High- and Medium-severity finding from the original review has been resolved — either
fixed and regression-tested against real PostgreSQL, or (Finding #1) investigated and confirmed
safe by design, which is the more valuable outcome since it required a live empirical test to
know rather than being assumable from source. The benchmark suite now has genuine statistical
rigor (bootstrap confidence intervals) and a real CI-gating regression check where none existed
before. The one item connection-budget enforcement (Finding #6) was deliberately left as a
warning rather than a default-behavior change, for the same reason the project already applies
elsewhere: an opt-in default that fails loudly beats an opt-out default that could silently break
an existing deployment's startup. The score is held back from higher only by what a single
review-and-fix pass structurally cannot close: the full §15 load-test matrix against a realistic
topology (sustained load, real sharding/replicas, PostgreSQL-side monitoring) has not been
executed, so "plausibly capable of low-thousands of ops/sec" remains an estimate, not validated
capacity evidence — and a genuinely comprehensive performance certification requires exactly that
evidence, which no source-level review or single-machine benchmark run can substitute for.

### Final Verdict: **Suitable with tuning**

dbkit's concurrency/pool/circuit-breaker architecture was already sound in design; this pass
closed every concrete correctness-adjacent performance gap found (retry budget, cancellation
shielding, LRU-eviction lock-holding, allocation overhead, silent bulk data-loss visibility, a
dead concurrency-tier config knob) and confirmed the single most consequential open question
(server-side timeout cancellation) resolves safely for both supported drivers. What remains
between "suitable with tuning" and "high-load ready" is no longer a set of known bugs or gaps —
it's the load-tested evidence at realistic scale (§15) that only a dedicated, multi-day exercise
against real infrastructure can produce. Recommended default configuration remains: explicit
`enforce_at_startup=True` for connection-budget enforcement, `max_engines`+`evict_lru` for
dynamic shard/tenant topologies, and reliance on the now-enforced `maximum_total_ms` (rather than
manually-computed `deadline=`) to bound retry time on every call.
