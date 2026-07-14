"""Async engine registry (§9).

One :class:`~sqlalchemy.ext.asyncio.AsyncEngine` per unique
``environment:database:shard:role:driver`` key, created lazily and disposed on shutdown.
SQLAlchemy owns pooling and dialects; this module just keys, builds, instruments, and caps
engines.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import NullPool
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .._core.config import DbkitConfig, PoolConfig, TargetConfig
from .._core.errors import DatabaseConfigurationError, DatabaseRoutingError
from .._core.keys import EngineKey
from .._core.routing import ResolvedRoute
from .._pool import PoolInstrumentation, PoolSnapshot
from ..observability import metrics as m
from ..observability.metrics import MetricsSink, NoopMetrics
from ._compat import sync_engine_of


@dataclass
class EngineEntry:
    """One cached engine plus the bookkeeping :class:`AsyncEngineRegistry` needs for it."""

    key: EngineKey
    engine: AsyncEngine
    target: TargetConfig
    instrumentation: PoolInstrumentation
    labels: dict[str, str]
    last_used: float = field(default_factory=time.monotonic)

    def snapshot(self) -> PoolSnapshot:
        """This engine's current connection-pool snapshot."""
        return self.instrumentation.snapshot(sync_engine_of(self.engine).pool)


def _connect_args(target: TargetConfig, pool: PoolConfig) -> dict[str, Any]:
    driver = target.driver
    if driver in ("psycopg", "psycopg2"):
        args: dict[str, Any] = {"connect_timeout": int(max(pool.connect_timeout_seconds, 1))}
        if pool.pgbouncer_compatible:
            # Disable server-side prepared-statement autoprep — required under PgBouncer
            # transaction pooling, where a connection may hit a different backend each
            # transaction (§ pgbouncer_compatible docstring on PoolConfig).
            args["prepare_threshold"] = None
        return args
    if driver == "asyncpg":
        # asyncpg names its connection establishment timeout "timeout".
        args = {"timeout": pool.connect_timeout_seconds}
        if pool.pgbouncer_compatible:
            args["statement_cache_size"] = 0
        return args
    return {}


class AsyncEngineRegistry:
    """Creates and caches engines; enforces a maximum engine count (§9, §22.4).

    When ``max_engines`` is set and ``evict_lru`` is True, reaching the cap evicts (disposes)
    the least-recently-used engine instead of failing — the pattern for dynamic per-tenant
    databases where the number of distinct tenants ever seen may be unbounded, but only a
    bounded number should have live connections at once. The default (``evict_lru=False``)
    is a hard cap: exceeding it is a configuration error, not a silent eviction.
    """

    def __init__(
        self,
        config: DbkitConfig,
        *,
        metrics: MetricsSink | None = None,
        max_engines: int | None = None,
        evict_lru: bool = False,
    ) -> None:
        """A registry backed by ``config``, empty until :meth:`get` creates engines lazily."""
        self._config = config
        self._metrics = metrics or NoopMetrics()
        self._max_engines = max_engines
        self._evict_lru = evict_lru
        self._entries: dict[str, EngineEntry] = {}
        self._lock = asyncio.Lock()

    @property
    def count(self) -> int:
        """Number of currently live engines."""
        return len(self._entries)

    def _target_for(self, route: ResolvedRoute) -> TargetConfig:
        db = self._config.databases.get(route.database)
        if db is None:
            raise DatabaseRoutingError(f"unknown database {route.database!r}")
        if route.role == "replica":
            for r in db.replicas:
                if r.name == route.replica_name:
                    return r
            raise DatabaseRoutingError(
                f"unknown replica {route.replica_name!r} for database {route.database!r}"
            )
        return db.primary

    def _key_for(self, route: ResolvedRoute, target: TargetConfig) -> EngineKey:
        role = "primary" if route.role == "primary" else f"replica:{route.replica_name}"
        return EngineKey(
            environment=self._config.environment,
            database=route.database,
            shard_id=route.shard_id,
            role=role,
            driver=target.driver,
        )

    def _build_engine(self, target: TargetConfig) -> AsyncEngine:
        pool = target.resolved_pool(self._config.defaults)
        kwargs: dict[str, Any] = {
            "connect_args": _connect_args(target, pool),
            "pool_pre_ping": pool.pre_ping,
        }
        if pool.disable_pooling:
            kwargs["poolclass"] = NullPool
        else:
            reset = None if pool.reset_on_return in (None, "none") else pool.reset_on_return
            kwargs.update(
                pool_size=pool.size,
                max_overflow=pool.max_overflow,
                pool_timeout=pool.timeout_seconds,
                pool_recycle=pool.recycle_seconds,
                pool_use_lifo=pool.use_lifo,
                pool_reset_on_return=reset,
            )
        try:
            return create_async_engine(target.url, **kwargs)
        except Exception as exc:
            raise DatabaseConfigurationError(
                f"failed to create engine for {target.name!r}: {exc}"
            ) from exc

    async def get(self, route: ResolvedRoute) -> EngineEntry:
        """The cached engine for ``route``, creating (and, if needed, evicting) one lazily."""
        target = self._target_for(route)
        key = self._key_for(route, target)
        key_str = str(key)
        entry = self._entries.get(key_str)
        if entry is not None:
            entry.last_used = time.monotonic()
            return entry
        victim: EngineEntry | None = None
        async with self._lock:
            entry = self._entries.get(key_str)  # double-checked
            if entry is None:
                if self._max_engines is not None and len(self._entries) >= self._max_engines:
                    if not self._evict_lru:
                        raise DatabaseConfigurationError(
                            f"engine limit reached ({self._max_engines}); cannot create {key_str!r}"
                        )
                    # Pop the victim under the lock, but dispose it *after* releasing the lock
                    # (below) — disposing while holding the lock would block every concurrent
                    # get() that only misses the fast path on I/O it has nothing to do with
                    # (performance review §2 Finding #4).
                    victim = self._pop_lru_locked()
                labels = {
                    "environment": key.environment,
                    "database": key.database,
                    "shard": key.shard_id,
                    "role": key.role,
                }
                engine = self._build_engine(target)
                pool = target.resolved_pool(self._config.defaults)
                instrumentation = PoolInstrumentation(
                    key=key_str,
                    labels=labels,
                    long_hold_warning_seconds=pool.long_hold_warning_seconds,
                    metrics=self._metrics,
                )
                instrumentation.attach(sync_engine_of(engine))
                entry = EngineEntry(
                    key=key,
                    engine=engine,
                    target=target,
                    instrumentation=instrumentation,
                    labels=labels,
                )
                self._entries[key_str] = entry
            entry.last_used = time.monotonic()
        if victim is not None:
            await victim.engine.dispose()
            self._metrics.incr(m.CONN_CLOSED, labels=victim.labels)
        return entry

    def _pop_lru_locked(self) -> EngineEntry | None:
        """Pop (but don't dispose) the least-recently-used engine. Caller must hold
        ``self._lock``; the caller is responsible for disposing the popped engine after
        releasing the lock."""
        if not self._entries:
            return None
        oldest_key = min(self._entries, key=lambda k: self._entries[k].last_used)
        return self._entries.pop(oldest_key)

    def snapshots(self) -> list[PoolSnapshot]:
        """A pool snapshot for every currently live engine."""
        return [entry.snapshot() for entry in self._entries.values()]

    async def dispose_all(self) -> None:
        """Dispose every engine and clear the registry (called by ``AsyncDatabase.close``)."""
        async with self._lock:
            entries = list(self._entries.values())
            self._entries.clear()
        for entry in entries:
            await entry.engine.dispose()

    async def dispose_one(self, key: str) -> bool:
        """Dispose one engine by its snapshot key (e.g. ``"env:database:shard:role:driver"``, as
        printed by :meth:`snapshots`/``dbkit pools``), forcing subsequent calls to rebuild it
        with fresh connections. Returns ``False`` if no live engine has that key.

        Only closes idle pooled connections, exactly like the LRU-eviction path
        (:meth:`_pop_lru_locked`) — a connection already checked out by another in-flight
        coroutine keeps working until the caller releases it (verified by the same regression
        test covering LRU eviction under concurrent use).
        """
        async with self._lock:
            entry = self._entries.pop(key, None)
        if entry is None:
            return False
        await entry.engine.dispose()
        self._metrics.incr(m.CONN_CLOSED, labels=entry.labels)
        return True
