"""Default metrics sink: config `observability.metrics: true` now actually wires
Prometheus (singleton — global-registry collectors must be shared per process)."""

from __future__ import annotations

import pytest

from dbkit import AsyncDatabase
from dbkit.observability import metrics as metrics_mod
from dbkit.observability.metrics import NoopMetrics, default_metrics_sink


@pytest.fixture(autouse=True)
def reset_singleton(monkeypatch):
    monkeypatch.setattr(metrics_mod, "_DEFAULT_SINK", None)


def test_default_sink_is_prometheus_when_available() -> None:
    pytest.importorskip("prometheus_client")
    sink = default_metrics_sink()
    assert type(sink).__name__ == "PrometheusMetrics"


def test_default_sink_is_singleton() -> None:
    assert default_metrics_sink() is default_metrics_sink()


def test_two_facades_share_one_sink() -> None:
    pytest.importorskip("prometheus_client")
    cfg = {"databases": {"app": {"primary": {"url": "postgresql+psycopg://h/app"}}}}
    a = AsyncDatabase.from_config(cfg)
    b = AsyncDatabase.from_config(cfg)  # would raise on duplicate collectors if not shared
    assert a._metrics is b._metrics


def test_metrics_false_stays_noop() -> None:
    cfg = {
        "databases": {"app": {"primary": {"url": "postgresql+psycopg://h/app"}}},
        "defaults": {"observability": {"metrics": False}},
    }
    db = AsyncDatabase.from_config(cfg)
    assert isinstance(db._metrics, NoopMetrics)
