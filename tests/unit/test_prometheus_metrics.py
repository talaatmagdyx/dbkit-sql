from __future__ import annotations

import pytest

from dbkit.observability.metrics import ALLOWED_LABELS, try_prometheus_sink


@pytest.fixture
def registry():
    pytest.importorskip("prometheus_client")
    from prometheus_client import CollectorRegistry

    return CollectorRegistry()


def _sample(registry, metric_name: str) -> dict | None:
    for family in registry.collect():
        for sample in family.samples:
            if sample.name == metric_name:
                return dict(sample.labels)
    return None


def test_incr_records_a_counter(registry) -> None:
    from dbkit.observability.prometheus import PrometheusMetrics

    sink = PrometheusMetrics(namespace="test_incr", registry=registry)
    sink.incr("db_transaction_total", 2.0, {"database": "app"})

    labels = _sample(registry, "db_transaction_total")
    assert labels is not None
    assert labels["database"] == "app"


def test_fill_always_returns_every_allowed_label_even_when_none_are_provided() -> None:
    """Prometheus requires every declared label name in each ``.labels()`` call — the empty
    ones must default to ``""``, not be omitted (this is a hard API requirement, unlike the
    duplicate-labels-dict-build finding this file's other tests validate)."""
    from dbkit.observability.prometheus import PrometheusMetrics

    sink = PrometheusMetrics(namespace="test_fill_empty")
    filled = sink._fill(None)
    assert set(filled) == ALLOWED_LABELS
    assert all(v == "" for v in filled.values())


def test_fill_defaults_absent_labels_and_keeps_provided_ones() -> None:
    from dbkit.observability.prometheus import PrometheusMetrics

    sink = PrometheusMetrics(namespace="test_fill_partial")
    filled = sink._fill({"database": "app", "role": "primary"})
    assert filled["database"] == "app"
    assert filled["role"] == "primary"
    # every other allowed label defaults to the empty string, not omitted
    for name in ALLOWED_LABELS - {"database", "role"}:
        assert filled[name] == ""


def test_fill_does_not_mutate_the_shared_empty_template() -> None:
    """The empty-labels template is built once and reused as the base for every call — a
    caller-visible mutation of one result must not leak into the next call."""
    from dbkit.observability.prometheus import PrometheusMetrics

    sink = PrometheusMetrics(namespace="test_fill_no_leak")
    first = sink._fill({"database": "app"})
    first["role"] = "mutated"
    second = sink._fill(None)
    assert second["role"] == ""


def test_disallowed_labels_are_rejected() -> None:
    from dbkit.observability.prometheus import PrometheusMetrics

    sink = PrometheusMetrics(namespace="test_disallowed")
    with pytest.raises(ValueError, match="disallowed metric labels"):
        sink.incr("db_transaction_total", 1.0, {"user_id": "123"})


def test_missing_prometheus_client_raises_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import sys

    monkeypatch.setitem(sys.modules, "prometheus_client", None)
    with pytest.raises(RuntimeError, match="prometheus-client"):
        try_prometheus_sink()
