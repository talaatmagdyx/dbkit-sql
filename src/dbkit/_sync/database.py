# This file is GENERATED from ../_async/database.py by tools/run_unasync.py.
# Do not edit by hand. Run `make unasync` after changing the async source.

"""The ``Database`` facade — the primary public entrypoint (§8.1).

Orchestrates target resolution, engine lookup, connection acquisition (measuring pool wait),
execution, resilience (retries/circuit breaker/concurrency limits), streaming, bulk writes,
COPY, metrics, and graceful startup/shutdown.
"""

from __future__ import annotations

import contextlib
import logging
import time
from collections.abc import Iterator, Awaitable, Callable, Mapping, Sequence
from typing import Any

from sqlalchemy import Table, insert
from sqlalchemy import Connection, Engine

from .._core import bulk as bulk_mod
from .._core.circuit import CircuitBreaker
from .._core.config import DbkitConfig
from .._core.errors import (
    DatabaseError,
    DatabaseRoutingError,
    DatabaseUnsupportedOperationError,
    classify,
)
from .._core.policies import effective_timeout
from .._core.query import Query, coerce_statement
from .._core.result import ExecutionResult
from .._core.routing import DatabaseTarget, ResolvedRoute, SingleShardResolver
from .._pool import PoolSnapshot
from ..observability import logging as obslog
from ..observability import metrics as m
from ..observability.metrics import MetricsSink, NoopMetrics
from ._compat import API_LABEL, copy_from_records
from .connection import ConnectionScope, run_execute
from .engine import EngineRegistry, EngineEntry
from .health import HealthReport, TargetHealth, ping
from .resilience import ConcurrencyLimiter, run_with_retries
from .streaming import ResultStream
from .transaction import _TransactionManager


class Database:
    """Async, SQL-first database facade over SQLAlchemy Core (§8.1)."""

    def __init__(
        self,
        config: DbkitConfig,
        *,
        metrics: MetricsSink | None = None,
    ) -> None:
        self._config = config
        if metrics is None:
            metrics = (
                _make_default_metrics(config)
                if config.defaults.observability.metrics
                else NoopMetrics()
            )
        self._metrics = metrics
        self._registry = EngineRegistry(config, metrics=self._metrics)
        self._shards = SingleShardResolver()
        self._breakers: dict[str, CircuitBreaker] = {}
        self._limiters: dict[str, ConcurrencyLimiter] = {}
        self._started = False

    # -- construction ------------------------------------------------------------- #

    @classmethod
    def from_config(
        cls, config: DbkitConfig | Mapping[str, Any], *, metrics: MetricsSink | None = None
    ) -> Database:
        cfg = config if isinstance(config, DbkitConfig) else DbkitConfig.from_dict(config)
        return cls(cfg, metrics=metrics)

    @property
    def config(self) -> DbkitConfig:
        return self._config

    @property
    def engine_count(self) -> int:
        return self._registry.count

    # -- lifecycle ---------------------------------------------------------------- #

    def start(self, *, warm: bool = False) -> None:
        """Create required engines and (optionally) warm connections (§27.1)."""
        if self._started:
            return
        self._config.validate()
        for name, db in self._config.databases.items():
            if db.primary.required:
                entry = self._registry.get(
                    ResolvedRoute(database=name, shard_id="default", role="primary")
                )
                if warm:
                    ping(entry.engine, timeout=self._config.defaults.query_timeout_seconds)
        self._started = True
        obslog.log_event(logging.INFO, "database.started", engines=self._registry.count)

    def require_ready(self) -> None:
        """Raise unless every required target is reachable (§27.1)."""
        report = self.health()
        if not report.ready:
            failed = [t.key for t in report.targets if not t.healthy]
            raise DatabaseError(f"required databases not ready: {failed}")

    def close(self, grace_period: float = 10.0) -> None:
        """Dispose all engines (§27.2). ``grace_period`` reserved for in-flight draining."""
        self._registry.dispose_all()
        self._started = False
        obslog.log_event(logging.INFO, "database.closed")

    def __enter__(self) -> Database:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- routing ------------------------------------------------------------------ #

    def _resolve(self, target: DatabaseTarget) -> ResolvedRoute:
        db = self._config.databases.get(target.database)
        if db is None:
            raise DatabaseRoutingError(f"unknown database {target.database!r}")
        shard_id = (
            self._shards.resolve(target.database, target.shard_key)
            if target.shard_key is not None
            else "default"
        )
        # Phase 1: reads fall back to the primary (replica routing arrives in Phase 4).
        return ResolvedRoute(database=target.database, shard_id=shard_id, role="primary")

    def _entry(self, target: DatabaseTarget) -> tuple[EngineEntry, ResolvedRoute]:
        route = self._resolve(target)
        entry = self._registry.get(route)
        return entry, route

    def _labels(self, entry: EngineEntry, query_name: str, operation: str) -> dict[str, str]:
        return {
            "environment": entry.key.environment,
            "database": entry.key.database,
            "shard": entry.key.shard_id,
            "role": entry.key.role,
            "query_name": query_name,
            "operation": operation,
            "api": API_LABEL,
        }

    @staticmethod
    def _is_postgres(entry: EngineEntry) -> bool:
        return entry.target.dialect == "postgresql"

    @staticmethod
    def _query_meta(query: object) -> tuple[str, str, Query | None]:
        if isinstance(query, Query):
            return query.name, query.operation, query
        return "adhoc", "read", None

    # -- resilience --------------------------------------------------------------- #

    def _breaker_for(self, entry: EngineEntry) -> CircuitBreaker | None:
        """One circuit breaker per db+shard+role, created lazily if enabled (§16)."""
        cb = self._config.defaults.circuit_breaker
        if not cb.enabled:
            return None
        key = str(entry.key)
        breaker = self._breakers.get(key)
        if breaker is None:
            breaker = CircuitBreaker(
                failure_threshold=cb.failure_threshold,
                window_seconds=cb.window_seconds,
                open_seconds=cb.open_seconds,
                half_open_max_calls=cb.half_open_max_calls,
            )
            self._breakers[key] = breaker
        return breaker

    def _limiter_for(self, database: str) -> ConcurrencyLimiter:
        """One concurrency limiter per named database, from its concurrency config (§17)."""
        limiter = self._limiters.get(database)
        if limiter is None:
            cc = self._config.databases[database].concurrency
            limiter = ConcurrencyLimiter(
                {
                    "database": cc.database,
                    "reads": cc.reads,
                    "writes": cc.writes,
                    "bulk": cc.bulk_writes,
                }
            )
            self._limiters[database] = limiter
        return limiter

    def _execute_with_resilience(
        self,
        target: DatabaseTarget,
        query: object,
        op: Callable[[ConnectionScope], Awaitable[Any]],
        *,
        commit: bool,
        timeout: float | None,
        deadline: float | None,
    ) -> Any:
        """Run ``op`` under concurrency limiting, circuit breaking, and retries (§14, §16, §17).

        Concurrency is acquired *inside* each attempt (before pool checkout) so queueing
        happens in cheap waiters and a retry backoff does not hold a slot.
        """
        entry, _route = self._entry(target)
        query_name, operation, q = self._query_meta(query)
        labels = self._labels(entry, query_name, operation)
        breaker = self._breaker_for(entry)
        limiter = self._limiter_for(entry.key.database)
        tier = "writes" if (q and q.is_write) else "reads"

        def attempt() -> Any:
            with (
                limiter.acquire("database"),
                limiter.acquire(tier),
                self._scope(
                    target, query, call_timeout=timeout, deadline=deadline, commit=commit
                ) as scope,
            ):
                return op(scope)

        return run_with_retries(
            attempt,
            query=q,
            retry=self._config.defaults.retry,
            breaker=breaker,
            metrics=self._metrics,
            labels=labels,
            deadline=deadline,
        )

    # -- acquisition -------------------------------------------------------------- #

    @contextlib.contextmanager
    def _scope(
        self,
        target: DatabaseTarget,
        query: object,
        *,
        call_timeout: float | None,
        deadline: float | None,
        commit: bool,
    ) -> Iterator[ConnectionScope]:
        """Acquire a short-lived connection scope, measure pool wait, emit metrics (§11.1)."""
        entry, _route = self._entry(target)
        query_name, operation, q = self._query_meta(query)
        labels = self._labels(entry, query_name, operation)
        timeout = effective_timeout(
            call_timeout,
            q,
            self._config.defaults.query_timeout_seconds,
            deadline,
            time.monotonic(),
        )

        wait_start = time.monotonic()
        conn = self._acquire(entry.engine, labels, query_name)
        pool_wait = time.monotonic() - wait_start
        self._metrics.observe(m.POOL_WAIT_SECONDS, pool_wait, labels=labels)

        scope = ConnectionScope(
            conn,
            is_postgres=self._is_postgres(entry),
            default_timeout=timeout,
            database=entry.key.database,
            shard_id=entry.key.shard_id,
            role=entry.key.role,
        )
        op_start = time.monotonic()
        try:
            yield scope
            if commit:
                conn.commit()
        except BaseException as exc:
            with contextlib.suppress(Exception):
                conn.rollback()
            if isinstance(exc, DatabaseError):
                self._metrics.incr(
                    m.OP_ERRORS, labels={**labels, "error_category": exc.category.value}
                )
            raise
        finally:
            duration = time.monotonic() - op_start
            with contextlib.suppress(Exception):
                conn.close()
            self._metrics.incr(m.OP_TOTAL, labels=labels)
            self._metrics.observe(m.OP_DURATION, duration, labels=labels)
            if duration * 1000 >= self._config.defaults.observability.slow_query_ms:
                obslog.slow_query_warning(
                    query_name=query_name,
                    duration_ms=duration * 1000,
                    threshold_ms=self._config.defaults.observability.slow_query_ms,
                    database=entry.key.database,
                    pool_wait_ms=pool_wait * 1000,
                )

    def _acquire(
        self, engine: Engine, labels: dict[str, str], query_name: str
    ) -> Connection:
        try:
            conn = engine.connect()
        except Exception as exc:
            err = classify(exc, query_name=query_name, database_name=labels.get("database"))
            self._metrics.incr(m.OP_ERRORS, labels={**labels, "error_category": err.category.value})
            raise err from exc
        # Best-effort context tag for leak diagnostics (§10.5).
        with contextlib.suppress(Exception):
            conn.info["dbkit_context"] = query_name
        return conn

    # -- read API ----------------------------------------------------------------- #

    def fetch_all(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: DatabaseTarget,
        map_to: Any = None,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> list[Any]:
        rows: list[Any] = self._execute_with_resilience(
            target,
            query,
            lambda scope: scope.fetch_all(query, params, map_to=map_to),
            commit=False,
            timeout=timeout,
            deadline=deadline,
        )
        return rows

    def fetch_one(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: DatabaseTarget,
        map_to: Any = None,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> Any:
        return self._execute_with_resilience(
            target,
            query,
            lambda scope: scope.fetch_one(query, params, map_to=map_to),
            commit=False,
            timeout=timeout,
            deadline=deadline,
        )

    def fetch_optional(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: DatabaseTarget,
        map_to: Any = None,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> Any | None:
        return self._execute_with_resilience(
            target,
            query,
            lambda scope: scope.fetch_optional(query, params, map_to=map_to),
            commit=False,
            timeout=timeout,
            deadline=deadline,
        )

    def fetch_value(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: DatabaseTarget,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> Any:
        return self._execute_with_resilience(
            target,
            query,
            lambda scope: scope.fetch_value(query, params),
            commit=False,
            timeout=timeout,
            deadline=deadline,
        )

    def fetch_values(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: DatabaseTarget,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> list[Any]:
        values: list[Any] = self._execute_with_resilience(
            target,
            query,
            lambda scope: scope.fetch_values(query, params),
            commit=False,
            timeout=timeout,
            deadline=deadline,
        )
        return values

    # -- write API ---------------------------------------------------------------- #

    def execute(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: DatabaseTarget,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> ExecutionResult:
        query_name, _, _ = self._query_meta(query)
        start = time.monotonic()
        row_count = self._execute_with_resilience(
            target,
            query,
            lambda scope: scope.execute(query, params),
            commit=True,
            timeout=timeout,
            deadline=deadline,
        )
        return ExecutionResult(
            row_count=row_count,
            query_name=query_name,
            database_name=target.database,
            duration_ms=(time.monotonic() - start) * 1000,
        )

    def execute_many(
        self,
        query: object,
        params_seq: Sequence[Mapping[str, Any]],
        *,
        target: DatabaseTarget,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> ExecutionResult:
        """Run one statement against many parameter sets (driver executemany)."""
        query_name, _op, _q = self._query_meta(query)
        statement = coerce_statement(query)
        start = time.monotonic()
        with self._scope(
            target, query, call_timeout=timeout, deadline=deadline, commit=True
        ) as scope:
            try:
                cursor = run_execute(
                    scope.raw,
                    statement,
                    list(params_seq),
                    timeout=scope._default_timeout,
                    is_postgres=scope._is_postgres,
                )
                row_count = cursor.rowcount
            except Exception as exc:
                raise classify(exc, query_name=query_name, database_name=target.database) from exc
        return ExecutionResult(
            row_count=row_count,
            query_name=query_name,
            database_name=target.database,
            duration_ms=(time.monotonic() - start) * 1000,
        )

    # -- explicit scopes ---------------------------------------------------------- #

    @contextlib.contextmanager
    def connection(
        self, *, target: DatabaseTarget, timeout: float | None = None
    ) -> Iterator[ConnectionScope]:
        """A held connection with commit-on-success / rollback-on-error semantics (§11.2)."""
        with self._scope(
            target, "adhoc", call_timeout=timeout, deadline=None, commit=True
        ) as scope:
            yield scope

    @contextlib.contextmanager
    def transaction(
        self,
        *,
        target: DatabaseTarget,
        isolation: str | None = None,
        read_only: bool = False,
        timeout: float | None = None,
        lock_timeout: float | None = None,
    ) -> Iterator[Any]:
        """An explicit transaction with isolation/timeout options (§11.3).

        Usage::

            with db.transaction(target=write_target) as tx:
                tx.execute(INSERT, params)
        """
        route = self._resolve(target)
        entry = self._registry.get(route)
        manager = _TransactionManager(
            entry.engine,
            is_postgres=self._is_postgres(entry),
            default_timeout=self._config.defaults.transaction_timeout_seconds,
            database=entry.key.database,
            shard_id=entry.key.shard_id,
            role=entry.key.role,
            isolation=isolation,
            read_only=read_only,
            timeout=timeout,
            lock_timeout=lock_timeout,
            query_name="transaction",
        )
        with manager as scope:
            yield scope

    # -- health & introspection --------------------------------------------------- #

    def health(self) -> HealthReport:
        targets: list[TargetHealth] = []
        ready = True
        for name, db in self._config.databases.items():
            if not db.primary.required:
                continue
            try:
                entry = self._registry.get(
                    ResolvedRoute(database=name, shard_id="default", role="primary")
                )
                ping(entry.engine, timeout=self._config.defaults.query_timeout_seconds)
                targets.append(TargetHealth(key=f"{name}.primary", healthy=True))
            except Exception as exc:
                ready = False
                targets.append(TargetHealth(key=f"{name}.primary", healthy=False, error=str(exc)))
        return HealthReport(live=True, ready=ready, targets=targets)

    def pool_status(self) -> list[PoolSnapshot]:
        return self._registry.snapshots()

    # -- streaming ---------------------------------------------------------------- #

    def stream(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: DatabaseTarget,
        batch_size: int = 1000,
        map_to: Any = None,
        max_duration: float | None = None,
    ) -> ResultStream:
        """Stream a large result set with a server-side cursor, bounded memory (§20).

        Usage::

            with db.stream(EXPORT, params, target=t, batch_size=1000) as rows:
                for row in rows:
                    ...

        The stream owns its connection until the context exits, so it bypasses auto-retry.
        """
        entry, _route = self._entry(target)
        statement = coerce_statement(query)
        query_name, operation, _q = self._query_meta(query)
        labels = self._labels(entry, query_name, operation)
        return ResultStream(
            entry.engine,
            statement,
            params,
            batch_size=batch_size,
            map_to=map_to,
            database=entry.key.database,
            role=entry.key.role,
            query_name=query_name,
            metrics=self._metrics,
            labels=labels,
            max_duration=max_duration,
        )

    # -- bulk writes -------------------------------------------------------------- #

    def insert_many(
        self,
        table: Table,
        rows: Sequence[Mapping[str, Any]],
        *,
        target: DatabaseTarget,
        mode: bulk_mod.FailureMode = "atomic",
        batch_size: int | None = None,
        timeout: float | None = None,
    ) -> ExecutionResult:
        """Insert many rows in adaptively-sized batches (§19). ``rows`` are param dicts."""
        return self._bulk_write(
            table,
            insert(table),
            rows,
            target=target,
            mode=mode,
            batch_size=batch_size,
            timeout=timeout,
            query_name=f"{table.name}.insert_many",
        )

    def upsert_many(
        self,
        table: Table,
        rows: Sequence[Mapping[str, Any]],
        *,
        target: DatabaseTarget,
        conflict_index_elements: Sequence[str],
        update_columns: Sequence[str] | None = None,
        mode: bulk_mod.FailureMode = "atomic",
        batch_size: int | None = None,
        timeout: float | None = None,
    ) -> ExecutionResult:
        """Upsert many rows via PostgreSQL ``ON CONFLICT`` (§19). ``update_columns=None`` means
        ``DO NOTHING``; otherwise those columns are updated from the proposed row."""
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        base = pg_insert(table)
        if update_columns is None:
            stmt = base.on_conflict_do_nothing(index_elements=list(conflict_index_elements))
        else:
            stmt = base.on_conflict_do_update(
                index_elements=list(conflict_index_elements),
                set_={c: base.excluded[c] for c in update_columns},
            )
        return self._bulk_write(
            table,
            stmt,
            rows,
            target=target,
            mode=mode,
            batch_size=batch_size,
            timeout=timeout,
            query_name=f"{table.name}.upsert_many",
        )

    def _bulk_write(
        self,
        table: Table,
        statement: Any,
        rows: Sequence[Mapping[str, Any]],
        *,
        target: DatabaseTarget,
        mode: bulk_mod.FailureMode,
        batch_size: int | None,
        timeout: float | None,
        query_name: str,
    ) -> ExecutionResult:
        start = time.monotonic()
        if not rows:
            return ExecutionResult(
                row_count=0, query_name=query_name, database_name=target.database, duration_ms=0.0
            )
        entry, _route = self._entry(target)
        labels = self._labels(entry, query_name, "write")
        limiter = self._limiter_for(entry.key.database)
        bulk_cfg = self._config.defaults.bulk
        n_cols = len(bulk_mod.column_names(list(rows)))
        batch_rows = bulk_mod.resolve_batch_rows(
            n_cols,
            batch_size or bulk_cfg.default_batch_rows,
            bulk_mod.BulkLimits(max_rows=bulk_cfg.max_batch_rows),
        )
        batches = list(bulk_mod.iter_batches(list(rows), batch_rows))
        self._metrics.observe(m.BULK_BATCH_SIZE, batch_rows, labels=labels)
        eff_timeout = timeout or self._config.defaults.transaction_timeout_seconds

        written = 0
        with limiter.acquire("database"), limiter.acquire("bulk"):
            if mode == "atomic":
                with self.transaction(target=target, timeout=eff_timeout) as tx:
                    for batch in batches:
                        try:
                            cursor = run_execute(
                                tx.raw,
                                statement,
                                list(batch),
                                timeout=eff_timeout,
                                is_postgres=self._is_postgres(entry),
                            )
                        except Exception as exc:
                            raise classify(
                                exc, query_name=query_name, database_name=target.database
                            ) from exc
                        written += (
                            cursor.rowcount
                            if cursor.rowcount and cursor.rowcount > 0
                            else len(batch)
                        )
            else:
                written = self._bulk_best_effort(
                    entry, statement, batches, mode=mode, timeout=eff_timeout, query_name=query_name
                )

        self._metrics.incr(m.BULK_ROWS, written, labels=labels)
        return ExecutionResult(
            row_count=written,
            query_name=query_name,
            database_name=target.database,
            duration_ms=(time.monotonic() - start) * 1000,
        )

    def _bulk_best_effort(
        self,
        entry: EngineEntry,
        statement: Any,
        batches: list[Any],
        *,
        mode: bulk_mod.FailureMode,
        timeout: float,
        query_name: str,
    ) -> int:
        """best_effort: each batch commits independently; split_on_failure additionally retries a
        failed batch row-by-row to isolate the bad rows (§19.3)."""
        target = DatabaseTarget(database=entry.key.database, role="write")
        written = 0
        for batch in batches:
            try:
                with self._scope(
                    target, query_name, call_timeout=timeout, deadline=None, commit=True
                ) as scope:
                    cursor = run_execute(
                        scope.raw,
                        statement,
                        list(batch),
                        timeout=timeout,
                        is_postgres=self._is_postgres(entry),
                    )
                    written += (
                        cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else len(batch)
                    )
            except Exception:
                if mode != "split_on_failure":
                    continue
                for row in batch:
                    try:
                        with self._scope(
                            target, query_name, call_timeout=timeout, deadline=None, commit=True
                        ) as scope:
                            run_execute(
                                scope.raw,
                                statement,
                                [row],
                                timeout=timeout,
                                is_postgres=self._is_postgres(entry),
                            )
                            written += 1
                    except Exception:
                        continue
        return written

    # -- COPY (PostgreSQL fast bulk ingest) --------------------------------------- #

    def copy_records(
        self,
        table: str,
        columns: Sequence[str],
        records: Any,
        *,
        target: DatabaseTarget,
        timeout: float | None = None,
    ) -> ExecutionResult:
        """Bulk-ingest ``records`` into ``table`` via PostgreSQL COPY (§19.2).

        ``records`` is an iterable (async or sync) of row sequences matching ``columns``.
        Fastest path for large ingests; PostgreSQL + psycopg only.
        """
        entry, _route = self._entry(target)
        if not self._is_postgres(entry):
            raise DatabaseUnsupportedOperationError("COPY is only supported on PostgreSQL")
        labels = self._labels(entry, f"{table}.copy", "write")
        limiter = self._limiter_for(entry.key.database)
        eff_timeout = timeout or self._config.defaults.transaction_timeout_seconds
        start = time.monotonic()
        written = 0
        # COPY runs on the raw driver connection, so it must sit inside an explicit
        # transaction (which begins the SQLAlchemy transaction) for the commit to persist it.
        with (
            limiter.acquire("database"),
            limiter.acquire("bulk"),
            self.transaction(target=target, timeout=eff_timeout) as tx,
        ):
            try:
                written = copy_from_records(tx.raw, table, list(columns), records)
            except Exception as exc:
                raise classify(
                    exc, query_name=f"{table}.copy", database_name=target.database
                ) from exc
        self._metrics.incr(m.BULK_ROWS, written, labels=labels)
        return ExecutionResult(
            row_count=written,
            query_name=f"{table}.copy",
            database_name=target.database,
            duration_ms=(time.monotonic() - start) * 1000,
        )


def _make_default_metrics(config: DbkitConfig) -> MetricsSink:
    return NoopMetrics()
