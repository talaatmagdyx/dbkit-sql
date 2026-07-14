from __future__ import annotations

import pytest

from dbkit.observability.tracing import make_tracer


def test_disabled_tracer_is_a_full_noop() -> None:
    tracer = make_tracer(enabled=False)
    assert tracer.enabled is False
    with tracer.span("dbkit.read", query_name="q") as span:
        span.set_attribute("db.rows_affected", 1)  # must not raise


def test_disabled_tracer_still_propagates_exceptions() -> None:
    tracer = make_tracer(enabled=False)
    with pytest.raises(ValueError), tracer.span("dbkit.read"):
        raise ValueError("boom")


@pytest.fixture(scope="module")
def span_exporter():
    """OTel's global TracerProvider can be set only once per process, so this fixture installs
    one shared provider/exporter for every test in this module and each test clears the
    exporter instead of installing a fresh provider."""
    pytest.importorskip("opentelemetry.sdk.trace")
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    # Only takes effect the first time a provider is set for this process; if some other test
    # already installed one, the real global provider (not necessarily this one) stays active.
    trace.set_tracer_provider(provider)
    yield exporter


@pytest.fixture(autouse=True)
def _clear_spans(span_exporter):
    span_exporter.clear()
    yield


def test_enabled_tracer_records_spans_with_sdk(span_exporter) -> None:
    tracer = make_tracer(enabled=True)
    assert tracer.enabled is True
    with tracer.span(
        "dbkit.read",
        operation_type="read",
        query_name="users.get",
        database="app",
        shard="default",
        role="primary",
    ) as span:
        span.set_attribute("db.pool.wait_ms", 1.5)

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1
    s = spans[0]
    assert s.name == "dbkit.read"
    assert s.attributes["db.query.name"] == "users.get"
    assert s.attributes["db.namespace"] == "app"
    assert s.attributes["db.target.role"] == "primary"
    assert s.attributes["db.pool.wait_ms"] == 1.5
    assert s.status.status_code.name == "UNSET"


def test_enabled_tracer_records_exception_status(span_exporter) -> None:
    tracer = make_tracer(enabled=True)
    with pytest.raises(RuntimeError), tracer.span("dbkit.write", query_name="q"):
        raise RuntimeError("db exploded")

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].status.status_code.name == "ERROR"
    assert len(spans[0].events) == 1
    assert spans[0].events[0].name == "exception"


def test_span_kind_is_client(span_exporter) -> None:
    """Database client spans must use SpanKind.CLIENT per the OTel semantic conventions."""
    from opentelemetry.trace import SpanKind

    tracer = make_tracer(enabled=True)
    with tracer.span("dbkit.read", query_name="q"):
        pass

    (recorded,) = span_exporter.get_finished_spans()
    assert recorded.kind == SpanKind.CLIENT


def test_tracer_accepts_explicit_tracer_provider() -> None:
    """A caller-supplied tracer_provider is used instead of the (possibly unset) global one."""
    pytest.importorskip("opentelemetry.sdk.trace")
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    tracer = make_tracer(enabled=True, tracer_provider=provider)
    with tracer.span("dbkit.read", query_name="q"):
        pass

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "dbkit.read"


def test_no_span_attributes_ever_contain_sql_text(span_exporter) -> None:
    """Statement text/params must never reach a span (§25.2, §29) — only logical metadata."""
    tracer = make_tracer(enabled=True)
    with tracer.span(
        "dbkit.write", query_name="users.insert", database="app", role="primary"
    ) as span:
        span.set_attribute("db.rows_affected", 3)

    (recorded,) = span_exporter.get_finished_spans()
    allowed_keys = {
        "db.system",
        "db.operation.type",
        "db.query.name",
        "db.namespace",
        "db.shard.id",
        "db.target.role",
        "db.pool.wait_ms",
        "db.rows_affected",
    }
    assert set(recorded.attributes.keys()) <= allowed_keys
