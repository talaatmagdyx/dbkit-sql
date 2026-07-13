"""Connection-pool instrumentation, protection, and leak detection (§10.4-10.6).

SQLAlchemy fires pool events (``connect``/``checkout``/``checkin``/``invalidate``/``close``)
as *synchronous* callbacks on the sync engine — even for an ``AsyncEngine`` (they run on
``engine.sync_engine``). So this instrumentation is identical for both frontends and lives in
one shared, non-generated module.

It does not manage connections itself — SQLAlchemy's pool does that. It observes the pool,
feeds ``db_pool_*`` metrics, powers :meth:`AsyncDatabase.pool_status`, and detects leaks /
long-held connections.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any

from sqlalchemy import event

from .observability import logging as obslog
from .observability import metrics as m
from .observability.metrics import MetricsSink, NoopMetrics


@dataclass
class PoolSnapshot:
    """A point-in-time view of a single engine's pool, for ``pool_status`` / health (§10.4)."""

    key: str
    size: int
    checked_out: int
    checked_in: int
    overflow: int
    max_overflow: int
    created: int
    closed: int
    invalidations: int
    longest_current_hold_seconds: float

    @property
    def total_capacity(self) -> int:
        return self.size + max(self.max_overflow, 0)

    @property
    def utilization(self) -> float:
        cap = self.total_capacity
        return (self.checked_out / cap) if cap else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "size": self.size,
            "checked_out": self.checked_out,
            "checked_in": self.checked_in,
            "overflow": self.overflow,
            "created": self.created,
            "closed": self.closed,
            "invalidations": self.invalidations,
            "utilization": round(self.utilization, 3),
            "longest_current_hold_seconds": round(self.longest_current_hold_seconds, 3),
        }


@dataclass
class _Checkout:
    since: float
    context: str | None
    owner: str | None


class PoolInstrumentation:
    """Attaches pool-event listeners to one engine and tracks its live pool state."""

    def __init__(
        self,
        *,
        key: str,
        labels: dict[str, str],
        long_hold_warning_seconds: float,
        metrics: MetricsSink | None = None,
    ) -> None:
        self.key = key
        self._labels = labels
        self._long_hold = long_hold_warning_seconds
        self._metrics = metrics or NoopMetrics()
        self._lock = threading.Lock()
        self._created = 0
        self._closed = 0
        self._invalidations = 0
        # record-id -> checkout metadata (only currently-held connections)
        self._checked_out: dict[int, _Checkout] = {}

    # -- event handlers (run synchronously inside SQLAlchemy) --------------------- #

    def _on_connect(self, _dbapi_conn: Any, _record: Any) -> None:
        with self._lock:
            self._created += 1
        self._metrics.incr(m.CONN_CREATED, labels=self._labels)

    def _on_checkout(self, _dbapi_conn: Any, record: Any, _proxy: Any) -> None:
        info = record.info
        info["dbkit_checkout_at"] = time.monotonic()
        with self._lock:
            self._checked_out[id(record)] = _Checkout(
                since=info["dbkit_checkout_at"],
                context=info.get("dbkit_context"),
                owner=info.get("dbkit_owner"),
            )

    def _on_checkin(self, _dbapi_conn: Any, record: Any) -> None:
        now = time.monotonic()
        started = record.info.pop("dbkit_checkout_at", None)
        record.info.pop("dbkit_context", None)
        record.info.pop("dbkit_owner", None)
        with self._lock:
            self._checked_out.pop(id(record), None)
        if started is not None:
            hold = now - started
            self._metrics.observe(m.CONN_HOLD_DURATION, hold, labels=self._labels)
            if hold >= self._long_hold:
                obslog.log_event(
                    logging.WARNING,
                    "database.pool.long_hold",
                    database=self._labels.get("database"),
                    role=self._labels.get("role"),
                    duration_ms=round(hold * 1000, 3),
                    pool=self.key,
                )

    def _on_invalidate(self, _dbapi_conn: Any, _record: Any, _exc: Any) -> None:
        with self._lock:
            self._invalidations += 1
        self._metrics.incr(m.POOL_INVALIDATIONS, labels=self._labels)

    def _on_close(self, _dbapi_conn: Any, _record: Any) -> None:
        with self._lock:
            self._closed += 1
        self._metrics.incr(m.CONN_CLOSED, labels=self._labels)

    def attach(self, target: Any) -> None:
        """Register listeners on ``target`` (a sync ``Engine`` / ``Pool``)."""
        event.listen(target, "connect", self._on_connect)
        event.listen(target, "checkout", self._on_checkout)
        event.listen(target, "checkin", self._on_checkin)
        event.listen(target, "invalidate", self._on_invalidate)
        event.listen(target, "close", self._on_close)

    # -- introspection ------------------------------------------------------------ #

    def longest_current_hold(self) -> float:
        now = time.monotonic()
        with self._lock:
            if not self._checked_out:
                return 0.0
            oldest = min(c.since for c in self._checked_out.values())
        return now - oldest

    def snapshot(self, pool: Any) -> PoolSnapshot:
        """Build a :class:`PoolSnapshot` from a live SQLAlchemy pool object."""

        # QueuePool exposes size()/checkedout()/checkedin()/overflow(); other pools may not.
        def _call(name: str, default: int = 0) -> int:
            fn = getattr(pool, name, None)
            try:
                return int(fn()) if callable(fn) else default
            except Exception:
                return default

        size = _call("size")
        checked_out = _call("checkedout")
        checked_in = _call("checkedin")
        # QueuePool.overflow() is negative while base slots remain free; report only the
        # connections actually opened beyond the base pool size.
        overflow = max(_call("overflow", 0), 0)
        max_overflow = getattr(pool, "_max_overflow", 0) or 0
        with self._lock:
            created, closed, invalidations = self._created, self._closed, self._invalidations
        snap = PoolSnapshot(
            key=self.key,
            size=size,
            checked_out=checked_out,
            checked_in=checked_in,
            overflow=overflow,
            max_overflow=int(max_overflow),
            created=created,
            closed=closed,
            invalidations=invalidations,
            longest_current_hold_seconds=self.longest_current_hold(),
        )
        # Emit gauges opportunistically whenever status is sampled.
        self._metrics.gauge(m.POOL_SIZE, snap.size, labels=self._labels)
        self._metrics.gauge(m.POOL_CHECKED_OUT, snap.checked_out, labels=self._labels)
        self._metrics.gauge(m.POOL_OVERFLOW, snap.overflow, labels=self._labels)
        return snap
