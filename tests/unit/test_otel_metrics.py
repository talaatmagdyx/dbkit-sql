from __future__ import annotations

import pytest

from dbkit.observability.metrics import try_otel_metrics_sink


@pytest.fixture
def reader():
    pytest.importorskip("opentelemetry.sdk.metrics")
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    return InMemoryMetricReader()


@pytest.fixture
def provider(reader):
    from opentelemetry.sdk.metrics import MeterProvider

    return MeterProvider(metric_readers=[reader])


def _points(reader, metric_name: str) -> list:
    for rm in reader.get_metrics_data().resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == metric_name:
                    return list(metric.data.data_points)
    return []


def test_incr_records_a_counter(reader, provider) -> None:
    sink = try_otel_metrics_sink(meter_provider=provider)
    sink.incr("db_transaction_total", 2.0, {"database": "app"})

    (point,) = _points(reader, "db_transaction_total")
    assert point.value == 2.0
    assert dict(point.attributes) == {"database": "app"}


def test_observe_records_a_histogram(reader, provider) -> None:
    sink = try_otel_metrics_sink(meter_provider=provider)
    sink.observe("db_transaction_duration_seconds", 0.75, {"database": "app"})

    (point,) = _points(reader, "db_transaction_duration_seconds")
    assert point.sum == pytest.approx(0.75)


def test_gauge_records_the_latest_value(reader, provider) -> None:
    sink = try_otel_metrics_sink(meter_provider=provider)
    sink.gauge("db_pool_size", 3.0, {"database": "app"})
    sink.gauge("db_pool_size", 5.0, {"database": "app"})

    (point,) = _points(reader, "db_pool_size")
    assert point.value == 5.0


def test_disallowed_labels_are_rejected(provider) -> None:
    sink = try_otel_metrics_sink(meter_provider=provider)
    with pytest.raises(ValueError, match="disallowed metric labels"):
        sink.incr("db_transaction_total", 1.0, {"user_id": "123"})


def test_missing_otel_api_raises_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import sys

    import opentelemetry

    # ``from opentelemetry import metrics`` skips reimporting if the parent package already
    # has a ``metrics`` attribute (from an earlier import elsewhere in the session), so both
    # the attribute and the sys.modules entry must be cleared to force a fresh (failing) import
    # — simulates opentelemetry-api not being installed without uninstalling it.
    monkeypatch.delattr(opentelemetry, "metrics", raising=False)
    monkeypatch.setitem(sys.modules, "opentelemetry.metrics", None)
    with pytest.raises(RuntimeError, match="opentelemetry-api"):
        try_otel_metrics_sink()
