# This file is GENERATED from ../_async/executor.py by tools/run_unasync.py.
# Do not edit by hand. Run `make unasync` after changing the async source.

"""Resilience-orchestration collaborator for :class:`~dbkit._sync.database.Database`
(§11.1, §14, §16, §17).

Extracted from the facade so it stays a thin dispatcher over fetch/execute/bulk/stream/
transaction methods, each delegating the actual "acquire a connection and run one statement,
with concurrency limits + circuit breaker + retries" mechanics to one focused collaborator —
mirroring how bulk writes, streaming, and transactions already live in their own modules. This
module owns exactly what the facade's own ``# -- resilience --`` / ``# -- acquisition --``
section comments already called out as one boundary; nothing here changes behavior, only where
the code lives.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Iterator, Awaitable, Callable
from typing import Any

from sqlalchemy import Connection, Engine

from .._core.circuit import CircuitBreaker
from .._core.config import DbkitConfig
from .._core.errors import (
    DatabaseCommitUnknownError,
    DatabaseError,
    classify,
    is_connection_error,
)
from .._core.policies import effective_timeout
from .._core.query import Query
from .._core.routing import DatabaseTarget, ResolvedRoute
from ..observability import logging as obslog
from ..observability import metrics as m
from ..observability.metrics import MetricsSink
from ..observability.tracing import Tracer
from ._compat import API_LABEL
from .connection import ConnectionScope
from .engine import EngineRegistry, EngineEntry
from .resilience import ConcurrencyLimiter, run_with_retries


class ResilientExecutor:
    """Resolves a target to an engine, acquires a connection, and runs one statement under
    the configured concurrency limits, circuit breaker, and retry policy.

    ``resolve`` is injected (rather than this class doing its own routing) because routing
    (shard/replica selection, the read-your-writes override) is a facade concern, not a
    resilience one — this collaborator only needs "given a target, which route/engine."
    """

    def __init__(
        self,
        config: DbkitConfig,
        *,
        registry: EngineRegistry,
        resolve: Callable[[DatabaseTarget], ResolvedRoute],
        metrics: MetricsSink,
        tracer: Tracer,
    ) -> None:
        self._config = config
        self._registry = registry
        self._resolve = resolve
        self._metrics = metrics
        self._tracer = tracer
        self._breakers: dict[str, CircuitBreaker] = {}
        self._limiters: dict[str, ConcurrencyLimiter] = {}

    # -- routing lookup ------------------------------------------------------------- #

    def entry(self, target: DatabaseTarget) -> tuple[EngineEntry, ResolvedRoute]:
        """The engine entry and resolved route for ``target``, creating the engine if needed."""
        route = self._resolve(target)
        entry = self._registry.get(route)
        return entry, route

    def labels(self, entry: EngineEntry, query_name: str, operation: str) -> dict[str, str]:
        """The metrics/tracing label set for one operation against ``entry``."""
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
    def is_postgres(entry: EngineEntry) -> bool:
        """Whether ``entry``'s target dialect is PostgreSQL."""
        return entry.target.dialect == "postgresql"

    @staticmethod
    def query_meta(query: object) -> tuple[str, str, Query | None]:
        """``(query_name, operation, Query | None)`` for a caller-supplied query object."""
        if isinstance(query, Query):
            return query.name, query.operation, query
        return "adhoc", "read", None

    # -- resilience ------------------------------------------------------------------ #

    def breaker_for(self, entry: EngineEntry) -> CircuitBreaker | None:
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

    def limiter_for(self, database: str) -> ConcurrencyLimiter:
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

    def execute_with_resilience(
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
        entry, _route = self.entry(target)
        query_name, operation, q = self.query_meta(query)
        labels = self.labels(entry, query_name, operation)
        breaker = self.breaker_for(entry)
        limiter = self.limiter_for(entry.key.database)
        tier = "writes" if (q and q.is_write) else "reads"

        def attempt() -> Any:
            # Bound the concurrency-limiter wait by the same effective timeout the query
            # itself would get, so a saturated tier fails with a classified error instead of
            # queueing invisibly forever (§17).
            limiter_timeout = effective_timeout(
                timeout, q, self._config.defaults.query_timeout_seconds, deadline, time.monotonic()
            )
            with (
                limiter.acquire("database", timeout=limiter_timeout),
                limiter.acquire(tier, timeout=limiter_timeout),
                self.scope(
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

    # -- acquisition ------------------------------------------------------------------ #

    @contextlib.contextmanager
    def scope(
        self,
        target: DatabaseTarget,
        query: object,
        *,
        call_timeout: float | None,
        deadline: float | None,
        commit: bool,
    ) -> Iterator[ConnectionScope]:
        """Acquire a short-lived connection scope, measure pool wait, emit metrics (§11.1)."""
        entry, _route = self.entry(target)
        query_name, operation, q = self.query_meta(query)
        labels = self.labels(entry, query_name, operation)
        timeout = effective_timeout(
            call_timeout,
            q,
            self._config.defaults.query_timeout_seconds,
            deadline,
            time.monotonic(),
        )

        with self._tracer.span(
            f"dbkit.{operation}",
            operation_type=operation,
            query_name=query_name,
            database=entry.key.database,
            shard=entry.key.shard_id,
            role=entry.key.role,
        ) as span:
            wait_start = time.monotonic()
            conn = self.acquire(entry.engine, labels, query_name)
            pool_wait = time.monotonic() - wait_start
            self._metrics.observe(m.POOL_WAIT_SECONDS, pool_wait, labels=labels)
            span.set_attribute("db.pool.wait_ms", pool_wait * 1000)

            scope = ConnectionScope(
                conn,
                is_postgres=self.is_postgres(entry),
                default_timeout=timeout,
                database=entry.key.database,
                shard_id=entry.key.shard_id,
                role=entry.key.role,
            )
            op_start = time.monotonic()
            try:
                yield scope
            except BaseException as exc:
                with contextlib.suppress(Exception):
                    conn.rollback()
                if isinstance(exc, DatabaseError):
                    self._metrics.incr(
                        m.OP_ERRORS, labels={**labels, "error_category": exc.category.value}
                    )
                raise
            else:
                if commit:
                    try:
                        conn.commit()
                    except BaseException as commit_exc:
                        # Mirror _TransactionManager._commit(): a failure during COMMIT
                        # may mean the write committed anyway (§15) — never silently swallow
                        # that ambiguity, and never leave the exception unclassified either
                        # (both would previously happen only on this auto-commit write path).
                        if is_connection_error(commit_exc):
                            err: DatabaseError = DatabaseCommitUnknownError(
                                "connection failed during COMMIT; transaction outcome is unknown",
                                original=commit_exc,
                                database_name=entry.key.database,
                                shard_id=entry.key.shard_id,
                                role=entry.key.role,
                                query_name=query_name,
                            )
                        else:
                            with contextlib.suppress(Exception):
                                conn.rollback()
                            err = classify(
                                commit_exc,
                                query_name=query_name,
                                database_name=entry.key.database,
                                role=entry.key.role,
                            )
                        self._metrics.incr(
                            m.OP_ERRORS, labels={**labels, "error_category": err.category.value}
                        )
                        raise err from commit_exc
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

    def acquire(
        self, engine: Engine, labels: dict[str, str], query_name: str
    ) -> Connection:
        """Check out a connection, classifying any failure; tags it for leak diagnostics."""
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
