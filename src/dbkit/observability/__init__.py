"""Observability: structured logging, metrics, and OpenTelemetry tracing (§25)."""

from __future__ import annotations

from .logging import log_event, logger, slow_query_warning
from .metrics import MetricsSink, NoopMetrics, try_otel_metrics_sink, try_prometheus_sink
from .tracing import SpanHandle, Tracer, make_tracer

__all__ = [
    "MetricsSink",
    "NoopMetrics",
    "SpanHandle",
    "Tracer",
    "log_event",
    "logger",
    "make_tracer",
    "slow_query_warning",
    "try_otel_metrics_sink",
    "try_prometheus_sink",
]
