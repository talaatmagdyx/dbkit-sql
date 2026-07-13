"""Metrics protocol and a no-op default; a Prometheus adapter lives behind the extra (§25.1).

The core depends only on the :class:`MetricsSink` protocol, so applications can plug in any
backend. Label sets are restricted to the low-cardinality allowlist in §25.1 — never raw SQL,
user/tenant/message/request ids, or exception strings.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, runtime_checkable

Labels = Mapping[str, str]


@runtime_checkable
class MetricsSink(Protocol):
    """Minimal metrics surface dbkit emits to."""

    def incr(self, name: str, value: float = 1.0, labels: Labels | None = None) -> None: ...

    def observe(self, name: str, value: float, labels: Labels | None = None) -> None: ...

    def gauge(self, name: str, value: float, labels: Labels | None = None) -> None: ...


class NoopMetrics:
    """Default sink — does nothing, allocates nothing meaningful on the hot path."""

    def incr(self, name: str, value: float = 1.0, labels: Labels | None = None) -> None:
        pass

    def observe(self, name: str, value: float, labels: Labels | None = None) -> None:
        pass

    def gauge(self, name: str, value: float, labels: Labels | None = None) -> None:
        pass


# Metric names (§25.1). Kept as constants so labels/names stay consistent across the codebase.
OP_TOTAL = "db_operation_total"
OP_DURATION = "db_operation_duration_seconds"
OP_ERRORS = "db_operation_errors_total"
OP_RETRIES = "db_operation_retries_total"
OP_TIMEOUTS = "db_operation_timeouts_total"
OP_CANCELLED = "db_operation_cancelled_total"

POOL_SIZE = "db_pool_size"
POOL_CHECKED_OUT = "db_pool_checked_out"
POOL_OVERFLOW = "db_pool_overflow"
POOL_WAIT_SECONDS = "db_pool_wait_seconds"
POOL_TIMEOUT_TOTAL = "db_pool_timeout_total"
POOL_INVALIDATIONS = "db_pool_invalidations_total"
CONN_CREATED = "db_connections_created_total"
CONN_CLOSED = "db_connections_closed_total"
CONN_CHECKOUT_DURATION = "db_connection_checkout_duration_seconds"
CONN_HOLD_DURATION = "db_connection_hold_duration_seconds"

TX_TOTAL = "db_transaction_total"
TX_DURATION = "db_transaction_duration_seconds"
TX_ROLLBACK = "db_transaction_rollback_total"
COMMIT_UNKNOWN = "db_commit_unknown_total"

#: Labels dbkit is allowed to attach (§25.1). Adapters should reject anything outside this set.
ALLOWED_LABELS = frozenset(
    {
        "service",
        "environment",
        "database",
        "shard",
        "role",
        "query_name",
        "operation",
        "error_category",
        "retry_attempt",
        "api",  # "sync" | "async" — distinguishes the two frontends
    }
)


def try_prometheus_sink(namespace: str = "dbkit") -> MetricsSink:
    """Construct a Prometheus-backed sink, or raise if ``prometheus_client`` is missing."""
    from .prometheus import PrometheusMetrics

    return PrometheusMetrics(namespace=namespace)
