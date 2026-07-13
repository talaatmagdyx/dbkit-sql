"""Observability: structured logging, a metrics protocol, and a Prometheus adapter (§25)."""

from __future__ import annotations

from .logging import log_event, logger, slow_query_warning
from .metrics import MetricsSink, NoopMetrics, try_prometheus_sink

__all__ = [
    "MetricsSink",
    "NoopMetrics",
    "log_event",
    "logger",
    "slow_query_warning",
    "try_prometheus_sink",
]
