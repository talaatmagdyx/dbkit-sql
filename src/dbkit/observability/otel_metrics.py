"""OpenTelemetry Metrics adapter for :class:`MetricsSink` (behind the ``otel`` extra).

An alternative to the Prometheus adapter: routes the same counters/histograms/gauges through
``opentelemetry.metrics`` instead, so they flow to whatever exporter the application's
``MeterProvider`` is configured with (OTLP, Prometheus-via-OTel, console, etc.).
"""

from __future__ import annotations

from typing import Any

from .metrics import ALLOWED_LABELS, Labels


class OTelMetrics:
    """Lazily create one OTel instrument per metric name via ``opentelemetry.metrics``.

    Unlike the Prometheus adapter, OTel attributes don't need a fixed pre-declared label set —
    each measurement carries only the labels it was given (filtered against
    :data:`ALLOWED_LABELS`).
    """

    def __init__(self, *, meter_provider: Any = None, meter_name: str = "dbkit") -> None:
        try:
            from opentelemetry import metrics
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "OTelMetrics requires opentelemetry-api — install dbkit[otel]"
            ) from exc
        self._meter = metrics.get_meter(meter_name, meter_provider=meter_provider)
        self._counters: dict[str, Any] = {}
        self._histograms: dict[str, Any] = {}
        self._gauges: dict[str, Any] = {}

    @staticmethod
    def _attributes(labels: Labels | None) -> dict[str, str]:
        provided = labels or {}
        bad = set(provided) - ALLOWED_LABELS
        if bad:
            raise ValueError(f"disallowed metric labels: {sorted(bad)}")
        return {k: str(v) for k, v in provided.items()}

    def _counter(self, name: str) -> Any:
        if name not in self._counters:
            self._counters[name] = self._meter.create_counter(name)
        return self._counters[name]

    def _histogram(self, name: str) -> Any:
        if name not in self._histograms:
            self._histograms[name] = self._meter.create_histogram(name)
        return self._histograms[name]

    def _gauge(self, name: str) -> Any:
        if name not in self._gauges:
            self._gauges[name] = self._meter.create_gauge(name)
        return self._gauges[name]

    def incr(self, name: str, value: float = 1.0, labels: Labels | None = None) -> None:
        self._counter(name).add(value, attributes=self._attributes(labels))

    def observe(self, name: str, value: float, labels: Labels | None = None) -> None:
        self._histogram(name).record(value, attributes=self._attributes(labels))

    def gauge(self, name: str, value: float, labels: Labels | None = None) -> None:
        self._gauge(name).set(value, attributes=self._attributes(labels))
