from __future__ import annotations

import logging

import pytest

from dbkit.observability.logging import long_transaction_warning, slow_query_warning


def test_long_transaction_warning_payload(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="dbkit"):
        long_transaction_warning(
            duration_ms=1234.5,
            threshold_ms=500.0,
            database="app",
            role="primary",
            outcome="commit",
        )
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.levelno == logging.WARNING
    payload = record.dbkit  # type: ignore[attr-defined]
    assert payload["event"] == "database.transaction.long_running"
    assert payload["database"] == "app"
    assert payload["role"] == "primary"
    assert payload["duration_ms"] == 1234.5
    assert payload["threshold_ms"] == 500.0
    assert payload["outcome"] == "commit"


def test_long_transaction_warning_below_threshold_is_still_emitted_when_called() -> None:
    """The helper itself always logs when called — the *caller* decides whether duration
    crosses the threshold before calling it (mirrors slow_query_warning)."""
    # No assertion on suppression here; this documents that gating happens at the call site
    # (in _AsyncTransactionManager.__aexit__), not inside the logging helper itself.
    long_transaction_warning(duration_ms=1.0, threshold_ms=5000.0, database="app")


def test_slow_query_warning_payload(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="dbkit"):
        slow_query_warning(
            query_name="users.get",
            duration_ms=750.0,
            threshold_ms=500.0,
            database="app",
            pool_wait_ms=12.0,
            rows=1,
        )
    payload = caplog.records[0].dbkit  # type: ignore[attr-defined]
    assert payload["event"] == "database.query.slow"
    assert payload["query_name"] == "users.get"
    assert payload["rows"] == 1


def test_log_event_has_no_trace_context_without_a_span(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="dbkit"):
        slow_query_warning(query_name="q", duration_ms=1.0, threshold_ms=1.0)
    payload = caplog.records[0].dbkit  # type: ignore[attr-defined]
    assert "trace_id" not in payload
    assert "span_id" not in payload


def test_log_event_carries_trace_context_inside_a_span(caplog: pytest.LogCaptureFixture) -> None:
    """Trace/log correlation (§25.2/§25.3): logs emitted while a span is recording carry its
    trace_id/span_id so a log line can be joined back to the trace that produced it."""
    pytest.importorskip("opentelemetry.sdk.trace")
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider

    provider = TracerProvider()
    tracer = provider.get_tracer("test")

    with (
        caplog.at_level(logging.WARNING, logger="dbkit"),
        tracer.start_as_current_span("s") as span,
    ):
        span_ctx = span.get_span_context()
        slow_query_warning(query_name="q", duration_ms=1.0, threshold_ms=1.0)

    payload = caplog.records[0].dbkit  # type: ignore[attr-defined]
    assert payload["trace_id"] == format(span_ctx.trace_id, "032x")
    assert payload["span_id"] == format(span_ctx.span_id, "016x")
    assert trace.get_current_span().get_span_context().is_valid is False  # span ended
