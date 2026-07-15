"""The ``AsyncDatabase`` facade — the primary public entrypoint (§8.1).

Orchestrates target resolution, engine lookup, connection acquisition (measuring pool wait),
execution, resilience (retries/circuit breaker/concurrency limits), streaming, bulk writes,
COPY, metrics, and graceful startup/shutdown.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import dataclasses
import logging
import time
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any, Literal

from sqlalchemy import Table, insert
from sqlalchemy.dialects import postgresql

from .._core import bulk as bulk_mod
from .._core.config import DatabaseConfig, DbkitConfig
from .._core.errors import (
    DatabaseError,
    DatabaseRoutingError,
    DatabaseUnsupportedOperationError,
    classify,
)
from .._core.query import coerce_statement
from .._core.result import ExecutionResult
from .._core.routing import (
    DatabaseTarget,
    ReplicaSelector,
    ResolvedRoute,
    RoundRobinReplicaSelector,
    ShardResolver,
    SingleShardResolver,
)
from .._pool import PoolSnapshot
from ..observability import logging as obslog
from ..observability import metrics as m
from ..observability.metrics import MetricsSink, NoopMetrics, default_metrics_sink
from ..observability.tracing import Tracer, make_tracer
from ..postgres import unnest as unnest_mod
from ._compat import copy_from_records
from .connection import AsyncConnectionScope, run_execute
from .engine import AsyncEngineRegistry, EngineEntry
from .executor import ResilientExecutor
from .health import HealthReport, TargetHealth, ping
from .streaming import AsyncResultStream
from .transaction import _AsyncTransactionManager

#: Task-local (safe across concurrent operations) consistency-scope override for read routing.
_consistency_mode: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "dbkit_consistency_mode", default=None
)


class AsyncDatabase:
    """Async, SQL-first database facade over SQLAlchemy Core (§8.1)."""

    def __init__(
        self,
        config: DbkitConfig,
        *,
        metrics: MetricsSink | None = None,
        tracer: Tracer | None = None,
        shard_resolver: ShardResolver | None = None,
        replica_selector: ReplicaSelector | None = None,
    ) -> None:
        """Build a facade from an already-validated :class:`DbkitConfig`.

        Prefer :meth:`from_config` unless you need to pass a config object you built/validated
        yourself. Does not connect to any database — call :meth:`start` before use.
        """
        self._config = config
        if metrics is None:
            metrics = (
                _make_default_metrics(config)
                if config.defaults.observability.metrics
                else NoopMetrics()
            )
        self._metrics = metrics
        self._tracer = tracer or make_tracer(config.defaults.observability.tracing)
        self._registry = AsyncEngineRegistry(
            config,
            metrics=self._metrics,
            max_engines=config.max_engines,
            evict_lru=config.evict_lru_engines,
        )
        self._shards = shard_resolver or SingleShardResolver()
        self._replicas = replica_selector or RoundRobinReplicaSelector(
            {name: [r.name for r in db.replicas] for name, db in config.databases.items()}
        )
        self._executor = ResilientExecutor(
            config,
            registry=self._registry,
            resolve=self._resolve,
            metrics=self._metrics,
            tracer=self._tracer,
        )
        self._started = False
        # Serializes dynamic register/unregister so concurrent registrations of the
        # same name cannot interleave purge/swap steps.
        self._registration_lock = asyncio.Lock()
        # Dynamic-database LRU (§22.4): statically configured names are never evicted.
        self._static_databases = frozenset(config.databases)
        self._dynamic_lru: dict[str, None] = {}

    # -- construction ------------------------------------------------------------- #

    @classmethod
    def from_config(
        cls,
        config: DbkitConfig | Mapping[str, Any],
        *,
        metrics: MetricsSink | None = None,
        tracer: Tracer | None = None,
        shard_resolver: ShardResolver | None = None,
        replica_selector: ReplicaSelector | None = None,
    ) -> AsyncDatabase:
        """Build a facade from a :class:`DbkitConfig` or a plain dict (validated via
        :meth:`DbkitConfig.from_dict`). The usual entry point."""
        cfg = config if isinstance(config, DbkitConfig) else DbkitConfig.from_dict(config)
        return cls(
            cfg,
            metrics=metrics,
            tracer=tracer,
            shard_resolver=shard_resolver,
            replica_selector=replica_selector,
        )

    @property
    def config(self) -> DbkitConfig:
        """The validated configuration this facade was built from."""
        return self._config

    @property
    def engine_count(self) -> int:
        """Number of live SQLAlchemy engines currently held (one per target)."""
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
        """``async with AsyncDatabase(...) as db:`` — calls :meth:`start`."""
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Calls :meth:`close`."""
        await self.close()

    # -- dynamic registration (§22.4) ---------------------------------------------- #

    async def register_database(
        self,
        name: str,
        config: DatabaseConfig | Mapping[str, Any],
        *,
        connect: bool | None = None,
    ) -> bool:
        """Register (or replace) a named database at runtime.

        For topologies discovered after :meth:`start` — e.g. shard DSNs resolved per
        tenant from a service registry — so callers do not need to maintain their own
        engine registries. ``config`` is a :class:`DatabaseConfig` or the same dict
        shape as one entry under the top-level ``databases`` mapping.

        The database map is swapped copy-on-write, so concurrent readers always see a
        complete mapping. Replacing an existing name first disposes its engines and
        resets its limiter/breakers, so the new settings fully apply. Returns ``True``
        when an existing entry was replaced.

        ``connect`` forces (or suppresses) eager engine creation; the default follows
        ``primary.required`` when the facade is already started. Engine creation is
        lazy in SQLAlchemy, so this never blocks on the network unless the pool is
        warmed explicitly.
        """
        db = (
            config
            if isinstance(config, DatabaseConfig)
            else DatabaseConfig.from_dict(config, name=name)
        )
        db.validate()
        db.enforce_connection_budget(self._config.defaults, database_name=name)
        async with self._registration_lock:
            replaced = name in self._config.databases
            prospective = dict(self._config.databases)
            prospective[name] = db
            # No random pool growth at the fleet level either: admitting this database
            # must not push the process past its configured connection budget (§10.3).
            dataclasses.replace(self._config, databases=prospective).enforce_connection_budget()
            if replaced:
                await self._purge_database_state(name)
            databases = dict(self._config.databases)
            databases[name] = db
            # DbkitConfig is a frozen dataclass; swap the mapping atomically.
            object.__setattr__(self._config, "databases", databases)
            self._set_selector_replicas(name, db)
            self._touch_dynamic(name)
            await self._evict_lru_databases_locked()
            if connect is None:
                connect = self._started and db.primary.required
            if connect:
                await self._registry.get(
                    ResolvedRoute(database=name, shard_id="default", role="primary")
                )
        obslog.log_event(logging.INFO, "database.registered", database=name, replaced=replaced)
        return replaced

    async def ensure_database(
        self,
        name: str,
        config: DatabaseConfig | Mapping[str, Any],
        *,
        connect: bool | None = None,
    ) -> bool:
        """Idempotent :meth:`register_database`: act only when ``name`` is missing or its
        config changed. Returns ``True`` when a (re)registration actually happened.

        The unchanged path is lock-free and cheap (build + compare one frozen config,
        a few µs), so callers routing by runtime topology can call this in front of
        every query::

            await db.ensure_database(shard_id, {"primary": {"url": dsn}})
            rows = await db.fetch_all(query, params,
                                      target=DatabaseTarget(database=shard_id, role="read"))

        A changed config (rotated host, credentials, pool settings) re-registers in
        place: old engines are disposed and the next query connects with the new
        settings — no restart needed, including password-only rotations.
        """
        db = (
            config
            if isinstance(config, DatabaseConfig)
            else DatabaseConfig.from_dict(config, name=name)
        )
        if self._config.databases.get(name) == db:
            self._touch_dynamic(name)
            return False
        await self.register_database(name, db, connect=connect)
        return True

    async def unregister_database(self, name: str) -> bool:
        """Remove a dynamically (or statically) registered database and dispose its
        engines, limiter, and breakers. Returns ``False`` if ``name`` is unknown.

        In-flight operations against the database finish on their checked-out
        connections (idle-only disposal); new calls raise ``DatabaseRoutingError``.
        """
        async with self._registration_lock:
            if name not in self._config.databases:
                return False
            await self._purge_database_state(name)
            databases = dict(self._config.databases)
            del databases[name]
            object.__setattr__(self._config, "databases", databases)
            self._clear_selector_replicas(name)
            self._dynamic_lru.pop(name, None)
        obslog.log_event(logging.INFO, "database.unregistered", database=name)
        return True

    @contextlib.asynccontextmanager
    async def database_scope(
        self,
        name: str,
        config: DatabaseConfig | Mapping[str, Any],
        *,
        connect: bool | None = None,
    ) -> AsyncIterator[DatabaseTarget]:
        """Register ``name`` for the duration of the block, then unregister it.

        Yields a primary-routed :class:`DatabaseTarget` for convenience::

            async with db.database_scope("tenant-42", {"primary": {"url": dsn}}) as target:
                rows = await db.fetch_all(query, params, target=target)

        **Scope this to a unit of work, never to a request.** Engines/pools are
        created on first use inside the block and disposed on exit — wrapping every
        HTTP request in a scope recreates connections each time and defeats pooling
        entirely. Intended uses: tests, migrations, one-off tenant batch jobs, or an
        ad-hoc query against a shard the process does not normally serve. Long-lived
        services should call :meth:`register_database` once per discovered shard and
        keep it registered.
        """
        await self.register_database(name, config, connect=connect)
        try:
            yield DatabaseTarget(database=name)
        finally:
            await self.unregister_database(name)

    def _touch_dynamic(self, name: str) -> None:
        """Mark a dynamically registered database as recently used (LRU order)."""
        if name in self._static_databases:
            return
        self._dynamic_lru.pop(name, None)
        self._dynamic_lru[name] = None  # dicts preserve insertion order

    async def _evict_lru_databases_locked(self) -> None:
        """Unregister least-recently-used dynamic databases beyond ``max_databases``.

        Called under ``_registration_lock``. Eviction fully purges the victim:
        engines disposed, limiter and breakers reset, selector entry dropped.
        """
        limit = self._config.max_databases
        if limit is None:
            return
        while len(self._dynamic_lru) > limit:
            victim = next(iter(self._dynamic_lru))
            self._dynamic_lru.pop(victim, None)
            await self._purge_database_state(victim)
            databases = dict(self._config.databases)
            databases.pop(victim, None)
            object.__setattr__(self._config, "databases", databases)
            self._clear_selector_replicas(victim)
            obslog.log_event(logging.INFO, "database.evicted", database=victim)

    async def _purge_database_state(self, name: str) -> None:
        """Dispose engines + reset resilience state for one database."""
        await self._registry.dispose_database(name)
        self._executor.forget_database(name)

    def _set_selector_replicas(self, name: str, db: DatabaseConfig) -> None:
        """Update the replica selector for a (re)registered database, if it supports it.

        The built-in selectors implement ``set_replicas``; custom selectors that do not
        are left untouched (they own their routing state).
        """
        set_replicas = getattr(self._replicas, "set_replicas", None)
        if set_replicas is None:
            return
        if isinstance(self._replicas, RoundRobinReplicaSelector):
            set_replicas(name, [r.name for r in db.replicas])
        else:
            set_replicas(name, [(r.name, r.weight) for r in db.replicas])

    def _clear_selector_replicas(self, name: str) -> None:
        set_replicas = getattr(self._replicas, "set_replicas", None)
        if set_replicas is not None:
            set_replicas(name, [])

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
        # A consistency_scope(read_your_writes) override forces reads to the primary so they
        # observe writes made earlier in the same scope (§23).
        if target.wants_replica and db.replicas and _consistency_mode.get() is None:
            replica_name = self._replicas.select(target.database, shard_id)
            if replica_name is not None:
                return ResolvedRoute(
                    database=target.database,
                    shard_id=shard_id,
                    role="replica",
                    replica_name=replica_name,
                )
            # Explicit fallback: selector found no replica for this database -> primary (§23).
        return ResolvedRoute(database=target.database, shard_id=shard_id, role="primary")

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
        """Run a one-shot read, acquiring and releasing its own connection (§11.1).

        Returns every row, mapped to ``map_to``, with no cardinality constraint. Retries and
        the circuit breaker apply per the resolved ``target``'s policy.
        """
        rows: list[Any] = await self._executor.execute_with_resilience(
            target,
            query,
            lambda scope: scope.fetch_all(query, params, map_to=map_to),
            commit=False,
            timeout=timeout,
            deadline=deadline,
        )
        return rows

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
        """Run a one-shot read expecting exactly one row.

        Raises :class:`~dbkit.errors.DatabaseResultError` if zero or more than one row comes
        back (via SQLAlchemy's own ``Result.one()``).
        """
        return await self._executor.execute_with_resilience(
            target,
            query,
            lambda scope: scope.fetch_one(query, params, map_to=map_to),
            commit=False,
            timeout=timeout,
            deadline=deadline,
        )

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
        """Run a one-shot read expecting zero or one row; ``None`` if empty."""
        return await self._executor.execute_with_resilience(
            target,
            query,
            lambda scope: scope.fetch_optional(query, params, map_to=map_to),
            commit=False,
            timeout=timeout,
            deadline=deadline,
        )

    async def fetch_value(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: DatabaseTarget,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> Any:
        """Run a one-shot read expecting exactly one row and return its first column."""
        return await self._executor.execute_with_resilience(
            target,
            query,
            lambda scope: scope.fetch_value(query, params),
            commit=False,
            timeout=timeout,
            deadline=deadline,
        )

    async def fetch_values(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: DatabaseTarget,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> list[Any]:
        """Run a one-shot read and return the first column of every row."""
        values: list[Any] = await self._executor.execute_with_resilience(
            target,
            query,
            lambda scope: scope.fetch_values(query, params),
            commit=False,
            timeout=timeout,
            deadline=deadline,
        )
        return values

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
        """Run a one-shot write/DDL statement, auto-committed on success (§11.1)."""
        query_name, _, _ = self._executor.query_meta(query)
        start = time.monotonic()
        row_count = await self._executor.execute_with_resilience(
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
        query_name, _op, _q = self._executor.query_meta(query)
        statement = coerce_statement(query)
        start = time.monotonic()
        async with self._executor.scope(
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
        async with self._executor.scope(
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
        deferrable: bool = False,
        timeout: float | None = None,
        lock_timeout: float | None = None,
    ) -> AsyncIterator[Any]:
        """An explicit transaction with isolation/timeout options (§11.3).

        ``deferrable`` (PostgreSQL only) is only meaningful together with
        ``isolation="serializable"`` and ``read_only=True``: it lets a read-only serializable
        transaction wait for a safe snapshot instead of risking a serialization failure.

        Usage::

            async with db.transaction(target=write_target) as tx:
                await tx.execute(INSERT, params)
        """
        route = self._resolve(target)
        entry = await self._registry.get(route)
        labels = self._executor.labels(entry, "transaction", "transaction")
        with self._tracer.span(
            "dbkit.transaction",
            operation_type="transaction",
            query_name="transaction",
            database=entry.key.database,
            shard=entry.key.shard_id,
            role=entry.key.role,
        ) as span:
            manager = _AsyncTransactionManager(
                entry.engine,
                is_postgres=self._executor.is_postgres(entry),
                default_timeout=self._config.defaults.transaction_timeout_seconds,
                database=entry.key.database,
                shard_id=entry.key.shard_id,
                role=entry.key.role,
                isolation=isolation,
                read_only=read_only,
                deferrable=deferrable,
                timeout=timeout,
                lock_timeout=lock_timeout,
                query_name="transaction",
                metrics=self._metrics,
                labels=labels,
                long_transaction_warning_seconds=(
                    self._config.defaults.long_transaction_warning_seconds
                ),
                span=span,
            )
            async with manager as scope:
                yield scope

    @contextlib.asynccontextmanager
    async def consistency_scope(
        self, *, mode: Literal["read_your_writes"] = "read_your_writes"
    ) -> AsyncIterator[None]:
        """Force reads within this scope to the primary so they observe writes made earlier
        in the same scope — read-your-writes over replica routing (§23)::

            async with db.consistency_scope(mode="read_your_writes"):
                await db.execute(write_query, target=write_target)
                row = await db.fetch_one(read_query, target=read_target)  # sees the write

        The override is a ``contextvars.ContextVar``, so it never leaks across concurrent
        operations in other tasks — but it also does not cross a thread boundary (a plain
        ``threading.Thread``, ``loop.run_in_executor``, or the sync ``Database`` facade driven
        from a worker thread does not inherit it). See "Read-your-writes across threads" in
        ``docs/troubleshooting.md`` if you need the override to apply there too.
        """
        token = _consistency_mode.set(mode)
        try:
            yield
        finally:
            _consistency_mode.reset(token)

    # -- health & introspection --------------------------------------------------- #

    async def health(self) -> HealthReport:
        """Readiness check: ``SELECT 1`` against every required target (§26)."""
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
        """A point-in-time snapshot of every currently live engine's connection pool."""
        return self._registry.snapshots()

    async def drain_engine(self, key: str) -> bool:
        """Force-dispose one engine's idle pooled connections by its snapshot ``key`` (as printed
        by :meth:`pool_status`, e.g. ``"prod:app:default:primary:psycopg"``), so the *next* call
        routed to it rebuilds a fresh engine with fresh connections. Returns ``False`` if no live
        engine currently has that key.

        Useful right before a planned failover/topology change: rather than waiting for
        connections to naturally recycle (``PoolConfig.recycle_seconds``), force them closed so
        subsequent traffic reaches the new backend immediately. Only idle pooled connections are
        closed — one already checked out by an in-flight call keeps working until released,
        exactly like LRU eviction (`AsyncEngineRegistry(evict_lru=True)`).

        This must be called from *within the running application process* — there is no
        ``dbkit`` CLI command for this, deliberately: each CLI invocation is a fresh, separate
        process with its own empty engine registry, so it has no way to reach into an
        already-running application's live engines. Wire this to a signal handler or an admin
        HTTP route in your own application instead (see ``docs/troubleshooting.md``).
        """
        return await self._registry.dispose_one(key)

    # -- streaming ---------------------------------------------------------------- #

    async def stream(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: DatabaseTarget,
        batch_size: int = 1000,
        map_to: Any = None,
        max_duration: float | None = None,
    ) -> AsyncResultStream:
        """Stream a large result set with a server-side cursor, bounded memory (§20).

        Usage::

            async with await db.stream(EXPORT, params, target=t, batch_size=1000) as rows:
                async for row in rows:
                    ...

        The stream owns its connection until the context exits, so it bypasses auto-retry.
        """
        entry, _route = await self._executor.entry(target)
        statement = coerce_statement(query)
        query_name, operation, _q = self._executor.query_meta(query)
        labels = self._executor.labels(entry, query_name, operation)
        return AsyncResultStream(
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
            tracer=self._tracer,
            shard_id=entry.key.shard_id,
        )

    # -- bulk writes -------------------------------------------------------------- #

    async def insert_many(
        self,
        table: Table,
        rows: Sequence[Mapping[str, Any]],
        *,
        target: DatabaseTarget,
        mode: bulk_mod.FailureMode = "atomic",
        strategy: bulk_mod.InsertStrategy = "execute_many",
        batch_size: int | None = None,
        timeout: float | None = None,
    ) -> ExecutionResult:
        """Insert many rows in adaptively-sized batches (§19). ``rows`` are param dicts.

        ``strategy="unnest"`` (PostgreSQL only) binds one array parameter per column instead
        of one parameter per column per row, so batch size isn't limited by the bind-parameter
        ceiling — a mid-tier option between ``execute_many`` and COPY.
        """
        return await self._bulk_write(
            table,
            insert(table),
            rows,
            target=target,
            mode=mode,
            strategy=strategy,
            batch_size=batch_size,
            timeout=timeout,
            query_name=f"{table.name}.insert_many",
        )

    async def upsert_many(
        self,
        table: Table,
        rows: Sequence[Mapping[str, Any]],
        *,
        target: DatabaseTarget,
        conflict_index_elements: Sequence[str],
        update_columns: Sequence[str] | None = None,
        mode: bulk_mod.FailureMode = "atomic",
        strategy: bulk_mod.InsertStrategy = "execute_many",
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
        return await self._bulk_write(
            table,
            stmt,
            rows,
            target=target,
            mode=mode,
            strategy=strategy,
            batch_size=batch_size,
            timeout=timeout,
            query_name=f"{table.name}.upsert_many",
            conflict_index_elements=conflict_index_elements,
            update_columns=update_columns,
        )

    async def _bulk_write(
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
        strategy: bulk_mod.InsertStrategy = "execute_many",
        conflict_index_elements: Sequence[str] | None = None,
        update_columns: Sequence[str] | None = None,
    ) -> ExecutionResult:
        start = time.monotonic()
        if not rows:
            return ExecutionResult(
                row_count=0, query_name=query_name, database_name=target.database, duration_ms=0.0
            )
        entry, _route = await self._executor.entry(target)
        labels = self._executor.labels(entry, query_name, "write")
        limiter = self._executor.limiter_for(entry.key.database)
        bulk_cfg = self._config.defaults.bulk
        columns = bulk_mod.column_names(list(rows))

        if strategy == "unnest":
            if not self._executor.is_postgres(entry):
                raise DatabaseUnsupportedOperationError(
                    "strategy='unnest' requires PostgreSQL; this target uses a different dialect"
                )
            column_types = {
                c: table.c[c].type.compile(dialect=postgresql.dialect())  # type: ignore[no-untyped-call]
                for c in columns
            }
            unnest_stmt = unnest_mod.unnest_insert_sql(
                table.name,
                columns,
                column_types,
                conflict_index_elements=conflict_index_elements,
                update_columns=update_columns,
            )
            plan = _BulkPlan(unnest_stmt, columns=columns)
        else:
            plan = _BulkPlan(statement, columns=None)

        batch_rows = bulk_mod.resolve_batch_rows(
            len(columns),
            batch_size or bulk_cfg.default_batch_rows,
            bulk_mod.BulkLimits(
                max_rows=bulk_cfg.max_batch_rows, max_payload_bytes=bulk_cfg.max_payload_bytes
            ),
            sample_row=rows[0] if rows else None,
        )
        batches = list(bulk_mod.iter_batches(list(rows), batch_rows))
        self._metrics.observe(m.BULK_BATCH_SIZE, batch_rows, labels=labels)
        eff_timeout = timeout or self._config.defaults.transaction_timeout_seconds

        written = 0
        with self._tracer.span(
            "dbkit.bulk_write",
            operation_type="write",
            query_name=query_name,
            database=entry.key.database,
            shard=entry.key.shard_id,
            role=entry.key.role,
        ) as span:
            async with limiter.acquire("database"), limiter.acquire("bulk"):
                if mode == "atomic":
                    async with self.transaction(target=target, timeout=eff_timeout) as tx:
                        for batch in batches:
                            stmt, params = plan.for_batch(batch)
                            try:
                                cursor = await run_execute(
                                    tx.raw,
                                    stmt,
                                    params,
                                    timeout=eff_timeout,
                                    is_postgres=self._executor.is_postgres(entry),
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
                    written = await self._bulk_best_effort(
                        entry,
                        plan,
                        batches,
                        mode=mode,
                        timeout=eff_timeout,
                        query_name=query_name,
                    )
            span.set_attribute("db.rows_affected", written)

        self._metrics.incr(m.BULK_ROWS, written, labels=labels)
        return ExecutionResult(
            row_count=written,
            query_name=query_name,
            database_name=target.database,
            duration_ms=(time.monotonic() - start) * 1000,
        )

    async def _bulk_best_effort(
        self,
        entry: EngineEntry,
        plan: _BulkPlan,
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
                stmt, params = plan.for_batch(batch)
                async with self._executor.scope(
                    target, query_name, call_timeout=timeout, deadline=None, commit=True
                ) as scope:
                    cursor = await run_execute(
                        scope.raw,
                        stmt,
                        params,
                        timeout=timeout,
                        is_postgres=self._executor.is_postgres(entry),
                    )
                    written += (
                        cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else len(batch)
                    )
            except Exception as exc:
                if mode != "split_on_failure":
                    # best_effort: this whole batch is dropped, never retried -- the only
                    # signal a caller gets is this log line + metric (performance review §9).
                    category = classify(exc, query_name=query_name).category.value
                    obslog.bulk_batch_dropped_warning(
                        query_name=query_name,
                        database=entry.key.database,
                        mode=mode,
                        rows_dropped=len(batch),
                        error_category=category,
                    )
                    self._metrics.incr(
                        m.BULK_ROWS_DROPPED,
                        len(batch),
                        labels={"database": entry.key.database, "query_name": query_name},
                    )
                    continue
                for row in batch:
                    try:
                        row_stmt, row_params = plan.for_batch([row])
                        async with self._executor.scope(
                            target, query_name, call_timeout=timeout, deadline=None, commit=True
                        ) as scope:
                            await run_execute(
                                scope.raw,
                                row_stmt,
                                row_params,
                                timeout=timeout,
                                is_postgres=self._executor.is_postgres(entry),
                            )
                            written += 1
                    except Exception as row_exc:
                        # split_on_failure: this one row is dropped after its batch already
                        # failed -- same silent-loss risk, narrowed to a single row.
                        category = classify(row_exc, query_name=query_name).category.value
                        obslog.bulk_batch_dropped_warning(
                            query_name=query_name,
                            database=entry.key.database,
                            mode=mode,
                            rows_dropped=1,
                            error_category=category,
                        )
                        self._metrics.incr(
                            m.BULK_ROWS_DROPPED,
                            1,
                            labels={"database": entry.key.database, "query_name": query_name},
                        )
                        continue
        return written

    # -- COPY (PostgreSQL fast bulk ingest) --------------------------------------- #

    async def copy_records(
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
        entry, _route = await self._executor.entry(target)
        if not self._executor.is_postgres(entry):
            raise DatabaseUnsupportedOperationError("COPY is only supported on PostgreSQL")
        labels = self._executor.labels(entry, f"{table}.copy", "write")
        limiter = self._executor.limiter_for(entry.key.database)
        eff_timeout = timeout or self._config.defaults.transaction_timeout_seconds
        start = time.monotonic()
        written = 0
        # COPY runs on the raw driver connection, so it must sit inside an explicit
        # transaction (which begins the SQLAlchemy transaction) for the commit to persist it.
        with self._tracer.span(
            "dbkit.copy",
            operation_type="write",
            query_name=f"{table}.copy",
            database=entry.key.database,
            shard=entry.key.shard_id,
            role=entry.key.role,
        ) as span:
            async with (
                limiter.acquire("database"),
                limiter.acquire("bulk"),
                self.transaction(target=target, timeout=eff_timeout) as tx,
            ):
                try:
                    written = await copy_from_records(tx.raw, table, list(columns), records)
                except Exception as exc:
                    raise classify(
                        exc, query_name=f"{table}.copy", database_name=target.database
                    ) from exc
            span.set_attribute("db.rows_affected", written)
        self._metrics.incr(m.BULK_ROWS, written, labels=labels)
        return ExecutionResult(
            row_count=written,
            query_name=f"{table}.copy",
            database_name=target.database,
            duration_ms=(time.monotonic() - start) * 1000,
        )


class _BulkPlan:
    """How to turn a batch of row-dicts into ``(statement, params)`` for one execution —
    abstracts over ``execute_many`` (list-of-dicts params) vs ``unnest`` (one array bind per
    column) so :meth:`AsyncDatabase._bulk_write` / `_bulk_best_effort` stay strategy-agnostic.
    """

    def __init__(self, statement: Any, *, columns: Sequence[str] | None) -> None:
        """``columns`` non-``None`` selects the ``unnest`` strategy; ``None`` selects
        ``execute_many``."""
        self._statement = statement
        self._columns = columns

    def for_batch(self, batch: Sequence[Mapping[str, Any]]) -> tuple[Any, Any]:
        """The ``(statement, params)`` pair to execute for this batch."""
        if self._columns is not None:
            return self._statement, unnest_mod.columnar_params(batch, self._columns)
        return self._statement, list(batch)


def _make_default_metrics(config: DbkitConfig) -> MetricsSink:
    """Honor ``observability.metrics: true`` (the default): Prometheus when available,
    no-op otherwise. Shared process-wide singleton — see :func:`default_metrics_sink`."""
    return default_metrics_sink()
