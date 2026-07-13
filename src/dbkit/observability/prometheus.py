"""Prometheus adapter for :class:`MetricsSink` (behind the ``[prometheus]`` extra)."""

from __future__ import annotations

from typing import Any

from .metrics import ALLOWED_LABELS, Labels


class PrometheusMetrics:
    """Lazily create Counter/Histogram/Gauge collectors keyed by metric name.

    Every metric is registered with the full :data:`ALLOWED_LABELS` label set; unspecified
    labels are filled with an empty string so cardinality stays predictable.
    """

    def __init__(self, namespace: str = "dbkit", registry: Any = None) -> None:
        try:
            from prometheus_client import REGISTRY, Counter, Gauge, Histogram
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "PrometheusMetrics requires prometheus-client — install dbkit[prometheus]"
            ) from exc
        self._Counter = Counter
        self._Gauge = Gauge
        self._Histogram = Histogram
        self._registry = registry if registry is not None else REGISTRY
        self._namespace = namespace
        self._labelnames = sorted(ALLOWED_LABELS)
        self._counters: dict[str, Any] = {}
        self._gauges: dict[str, Any] = {}
        self._histograms: dict[str, Any] = {}

    def _fill(self, labels: Labels | None) -> dict[str, str]:
        provided = labels or {}
        bad = set(provided) - ALLOWED_LABELS
        if bad:
            raise ValueError(f"disallowed metric labels: {sorted(bad)}")
        return {name: str(provided.get(name, "")) for name in self._labelnames}

    def _counter(self, name: str) -> Any:
        if name not in self._counters:
            self._counters[name] = self._Counter(
                name, name, self._labelnames, registry=self._registry
            )
        return self._counters[name]

    def _gauge_metric(self, name: str) -> Any:
        if name not in self._gauges:
            self._gauges[name] = self._Gauge(name, name, self._labelnames, registry=self._registry)
        return self._gauges[name]

    def _histogram(self, name: str) -> Any:
        if name not in self._histograms:
            self._histograms[name] = self._Histogram(
                name, name, self._labelnames, registry=self._registry
            )
        return self._histograms[name]

    def incr(self, name: str, value: float = 1.0, labels: Labels | None = None) -> None:
        self._counter(name).labels(**self._fill(labels)).inc(value)

    def observe(self, name: str, value: float, labels: Labels | None = None) -> None:
        self._histogram(name).labels(**self._fill(labels)).observe(value)

    def gauge(self, name: str, value: float, labels: Labels | None = None) -> None:
        self._gauge_metric(name).labels(**self._fill(labels)).set(value)
