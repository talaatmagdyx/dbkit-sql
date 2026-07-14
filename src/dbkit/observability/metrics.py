"""Metrics protocol and a no-op default; a Prometheus adapter lives behind the extra (§25.1).

The core depends only on the :class:`MetricsSink` protocol, so applications can plug in any
backend. Label sets are restricted to the low-cardinality allowlist in §25.1 — never raw SQL,
user/tenant/message/request ids, or exception strings.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

Labels = Mapping[str, str]


@runtime_checkable
class MetricsSink(Protocol):
    """Minimal metrics surface dbkit emits to."""

    def incr(self, name: str, value: float = 1.0, labels: Labels | None = None) -> None:
        """Increment a counter metric by ``value``."""
        ...

    def observe(self, name: str, value: float, labels: Labels | None = None) -> None:
        """Record one observation of a histogram/summary metric."""
        ...

    def gauge(self, name: str, value: float, labels: Labels | None = None) -> None:
        """Set a gauge metric to its current ``value``."""
        ...


class NoopMetrics:
    """Default sink — does nothing, allocates nothing meaningful on the hot path."""

    def incr(self, name: str, value: float = 1.0, labels: Labels | None = None) -> None:
        """Does nothing."""

    def observe(self, name: str, value: float, labels: Labels | None = None) -> None:
        """Does nothing."""

    def gauge(self, name: str, value: float, labels: Labels | None = None) -> None:
        """Does nothing."""


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

#: Current circuit breaker state per db+shard+role: 0=closed, 1=half_open, 2=open (§16).
CIRCUIT_STATE = "db_circuit_breaker_state"

STREAM_ROWS = "db_stream_rows_total"
STREAM_BYTES = "db_stream_bytes_total"
BULK_ROWS = "db_bulk_rows_total"
BULK_BATCH_SIZE = "db_bulk_batch_size"
#: Rows silently dropped by a best_effort/split_on_failure bulk write (never retried) — the
#: only metric signal for otherwise-silent data loss under those failure modes (§19.3).
BULK_ROWS_DROPPED = "db_bulk_rows_dropped_total"

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


def try_otel_metrics_sink(meter_name: str = "dbkit", meter_provider: Any = None) -> MetricsSink:
    """Construct an OpenTelemetry Metrics-backed sink, or raise if ``opentelemetry-api`` is
    missing. An alternative to :func:`try_prometheus_sink` — pick one per deployment."""
    from .otel_metrics import OTelMetrics

    return OTelMetrics(meter_name=meter_name, meter_provider=meter_provider)
