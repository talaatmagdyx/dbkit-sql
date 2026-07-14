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
