"""The ``AsyncDatabase`` facade — the primary public entrypoint (§8.1).

Orchestrates target resolution, engine lookup, connection acquisition (measuring pool wait),
execution, metrics, and graceful startup/shutdown. Bulk and streaming methods are declared
here and raise :class:`DatabaseUnsupportedOperationError` until Phase 3, so the API surface is
stable from day one.
"""

from __future__ import annotations

import contextlib
import logging
import time
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

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
from ._compat import API_LABEL
from .connection import AsyncConnectionScope, run_execute
from .engine import AsyncEngineRegistry, EngineEntry
from .health import HealthReport, TargetHealth, ping
from .transaction import _AsyncTransactionManager


class AsyncDatabase:
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
        self._registry = AsyncEngineRegistry(config, metrics=self._metrics)
        self._shards = SingleShardResolver()
        self._started = False

    # -- construction ------------------------------------------------------------- #

    @classmethod
    def from_config(
        cls, config: DbkitConfig | Mapping[str, Any], *, metrics: MetricsSink | None = None
    ) -> AsyncDatabase:
        cfg = config if isinstance(config, DbkitConfig) else DbkitConfig.from_dict(config)
        return cls(cfg, metrics=metrics)

    @property
    def config(self) -> DbkitConfig:
        return self._config

    @property
    def engine_count(self) -> int:
        return self._registry.count

    # -- lifecycle ---------------------------------------------------------------- #

    async def start(self, *, warm: bool = False) -> None:
        """Create required engines and (optionally) warm connections (§27.1)."""
        if self._started:
            return
        self._config.validate()
        for name, db in self._config.databases.items():
            if db.primary.required:
                entry = await self._registry.get(
                    ResolvedRoute(database=name, shard_id="default", role="primary")
                )
                if warm:
                    await ping(entry.engine, timeout=self._config.defaults.query_timeout_seconds)
        self._started = True
        obslog.log_event(logging.INFO, "database.started", engines=self._registry.count)

    async def require_ready(self) -> None:
        """Raise unless every required target is reachable (§27.1)."""
        report = await self.health()
        if not report.ready:
            failed = [t.key for t in report.targets if not t.healthy]
            raise DatabaseError(f"required databases not ready: {failed}")

    async def close(self, grace_period: float = 10.0) -> None:
        """Dispose all engines (§27.2). ``grace_period`` reserved for in-flight draining."""
        await self._registry.dispose_all()
        self._started = False
        obslog.log_event(logging.INFO, "database.closed")

    async def __aenter__(self) -> AsyncDatabase:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

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

    async def _entry(self, target: DatabaseTarget) -> tuple[EngineEntry, ResolvedRoute]:
        route = self._resolve(target)
        entry = await self._registry.get(route)
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

    # -- acquisition -------------------------------------------------------------- #

    @contextlib.asynccontextmanager
    async def _scope(
        self,
        target: DatabaseTarget,
        query: object,
        *,
        call_timeout: float | None,
        deadline: float | None,
        commit: bool,
    ) -> AsyncIterator[AsyncConnectionScope]:
        """Acquire a short-lived connection scope, measure pool wait, emit metrics (§11.1)."""
        entry, _route = await self._entry(target)
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
        conn = await self._acquire(entry.engine, labels, query_name)
        pool_wait = time.monotonic() - wait_start
        self._metrics.observe(m.POOL_WAIT_SECONDS, pool_wait, labels=labels)

        scope = AsyncConnectionScope(
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
                await conn.commit()
        except BaseException as exc:
            with contextlib.suppress(Exception):
                await conn.rollback()
            if isinstance(exc, DatabaseError):
                self._metrics.incr(
                    m.OP_ERRORS, labels={**labels, "error_category": exc.category.value}
                )
            raise
        finally:
            duration = time.monotonic() - op_start
            with contextlib.suppress(Exception):
                await conn.close()
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

    async def _acquire(
        self, engine: AsyncEngine, labels: dict[str, str], query_name: str
    ) -> AsyncConnection:
        try:
            conn = await engine.connect()
        except Exception as exc:
            err = classify(exc, query_name=query_name, database_name=labels.get("database"))
            self._metrics.incr(m.OP_ERRORS, labels={**labels, "error_category": err.category.value})
            raise err from exc
        # Best-effort context tag for leak diagnostics (§10.5).
        with contextlib.suppress(Exception):
            conn.info["dbkit_context"] = query_name
        return conn

    # -- read API ----------------------------------------------------------------- #

    async def fetch_all(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: DatabaseTarget,
        map_to: Any = None,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> list[Any]:
        async with self._scope(
            target, query, call_timeout=timeout, deadline=deadline, commit=False
        ) as scope:
            return await scope.fetch_all(query, params, map_to=map_to)

    async def fetch_one(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: DatabaseTarget,
        map_to: Any = None,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> Any:
        async with self._scope(
            target, query, call_timeout=timeout, deadline=deadline, commit=False
        ) as scope:
            return await scope.fetch_one(query, params, map_to=map_to)

    async def fetch_optional(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: DatabaseTarget,
        map_to: Any = None,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> Any | None:
        async with self._scope(
            target, query, call_timeout=timeout, deadline=deadline, commit=False
        ) as scope:
            return await scope.fetch_optional(query, params, map_to=map_to)

    async def fetch_value(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: DatabaseTarget,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> Any:
        async with self._scope(
            target, query, call_timeout=timeout, deadline=deadline, commit=False
        ) as scope:
            return await scope.fetch_value(query, params)

    async def fetch_values(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: DatabaseTarget,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> list[Any]:
        async with self._scope(
            target, query, call_timeout=timeout, deadline=deadline, commit=False
        ) as scope:
            return await scope.fetch_values(query, params)

    # -- write API ---------------------------------------------------------------- #

    async def execute(
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
        async with self._scope(
            target, query, call_timeout=timeout, deadline=deadline, commit=True
        ) as scope:
            row_count = await scope.execute(query, params)
        return ExecutionResult(
            row_count=row_count,
            query_name=query_name,
            database_name=target.database,
            duration_ms=(time.monotonic() - start) * 1000,
        )

    async def execute_many(
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
        async with self._scope(
            target, query, call_timeout=timeout, deadline=deadline, commit=True
        ) as scope:
            try:
                cursor = await run_execute(
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

    @contextlib.asynccontextmanager
    async def connection(
        self, *, target: DatabaseTarget, timeout: float | None = None
    ) -> AsyncIterator[AsyncConnectionScope]:
        """A held connection with commit-on-success / rollback-on-error semantics (§11.2)."""
        async with self._scope(
            target, "adhoc", call_timeout=timeout, deadline=None, commit=True
        ) as scope:
            yield scope

    @contextlib.asynccontextmanager
    async def transaction(
        self,
        *,
        target: DatabaseTarget,
        isolation: str | None = None,
        read_only: bool = False,
        timeout: float | None = None,
        lock_timeout: float | None = None,
    ) -> AsyncIterator[Any]:
        """An explicit transaction with isolation/timeout options (§11.3).

        Usage::

            async with db.transaction(target=write_target) as tx:
                await tx.execute(INSERT, params)
        """
        route = self._resolve(target)
        entry = await self._registry.get(route)
        manager = _AsyncTransactionManager(
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
        async with manager as scope:
            yield scope

    # -- health & introspection --------------------------------------------------- #

    async def health(self) -> HealthReport:
        targets: list[TargetHealth] = []
        ready = True
        for name, db in self._config.databases.items():
            if not db.primary.required:
                continue
            try:
                entry = await self._registry.get(
                    ResolvedRoute(database=name, shard_id="default", role="primary")
                )
                await ping(entry.engine, timeout=self._config.defaults.query_timeout_seconds)
                targets.append(TargetHealth(key=f"{name}.primary", healthy=True))
            except Exception as exc:
                ready = False
                targets.append(TargetHealth(key=f"{name}.primary", healthy=False, error=str(exc)))
        return HealthReport(live=True, ready=ready, targets=targets)

    def pool_status(self) -> list[PoolSnapshot]:
        return self._registry.snapshots()

    # -- Phase 3 stubs ------------------------------------------------------------ #

    def stream(self, *args: object, **kwargs: object) -> Any:
        raise DatabaseUnsupportedOperationError("stream() arrives in Phase 3")

    async def insert_many(self, *args: object, **kwargs: object) -> Any:
        raise DatabaseUnsupportedOperationError("insert_many() arrives in Phase 3")

    async def upsert_many(self, *args: object, **kwargs: object) -> Any:
        raise DatabaseUnsupportedOperationError("upsert_many() arrives in Phase 3")

    async def copy_records(self, *args: object, **kwargs: object) -> Any:
        raise DatabaseUnsupportedOperationError("copy_records() arrives in Phase 3")


def _make_default_metrics(config: DbkitConfig) -> MetricsSink:
    return NoopMetrics()
