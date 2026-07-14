"""Structured logging helpers (§25.3).

dbkit logs through the stdlib ``logging`` module under the ``dbkit`` logger, passing a
structured payload in ``extra={"dbkit": {...}}`` so a JSON formatter can render it. Bound
parameters are never logged unless explicitly enabled, and sensitive params are always
redacted.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

logger = logging.getLogger("dbkit")

try:
    from opentelemetry import trace as _otel_trace

    _OTEL_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised by the no-otel test lane
    _OTEL_AVAILABLE = False


def _trace_context() -> dict[str, str]:
    """``trace_id``/``span_id`` of the current OTel span, for trace/log correlation (§25.2/§25.3).

    Empty if OTel isn't installed or no span is currently recording. The availability check is
    done once at import time (not per call) so this stays cheap on the hot logging path even
    when OTel isn't installed — a per-call ``import opentelemetry`` would otherwise re-walk
    ``sys.path`` on every log event, since failed imports aren't cached in ``sys.modules``.
    """
    if not _OTEL_AVAILABLE:
        return {}
    ctx = _otel_trace.get_current_span().get_span_context()
    if not ctx.is_valid:
        return {}
    return {"trace_id": format(ctx.trace_id, "032x"), "span_id": format(ctx.span_id, "016x")}


def log_event(
    level: int,
    event: str,
    *,
    query_name: str | None = None,
    database: str | None = None,
    shard: str | None = None,
    role: str | None = None,
    duration_ms: float | None = None,
    pool_wait_ms: float | None = None,
    rows: int | None = None,
    retry_attempt: int | None = None,
    error_code: str | None = None,
    **extra: Any,
) -> None:
    """Emit one structured lifecycle event (§25.3)."""
    if not logger.isEnabledFor(level):
        return
    payload: dict[str, Any] = {"event": event}
    for key, value in (
        ("query_name", query_name),
        ("database", database),
        ("shard", shard),
        ("role", role),
        ("duration_ms", duration_ms),
        ("pool_wait_ms", pool_wait_ms),
        ("rows", rows),
        ("retry_attempt", retry_attempt),
        ("error_code", error_code),
    ):
        if value is not None:
            payload[key] = value
    payload.update(_trace_context())
    payload.update(extra)
    logger.log(level, event, extra={"dbkit": payload})


def slow_query_warning(
    *,
    query_name: str,
    duration_ms: float,
    threshold_ms: float,
    database: str | None = None,
    pool_wait_ms: float | None = None,
    rows: int | None = None,
) -> None:
    """Emit a ``database.query.slow`` warning (caller decides whether the threshold was hit)."""
    log_event(
        logging.WARNING,
        "database.query.slow",
        query_name=query_name,
        database=database,
        duration_ms=round(duration_ms, 3),
        pool_wait_ms=None if pool_wait_ms is None else round(pool_wait_ms, 3),
        rows=rows,
        threshold_ms=threshold_ms,
    )


def long_transaction_warning(
    *,
    duration_ms: float,
    threshold_ms: float,
    database: str | None = None,
    role: str | None = None,
    outcome: str | None = None,
) -> None:
    """Warn when an explicit transaction was held open longer than the configured threshold —
    the transaction analog of the pool's long-connection-hold warning (§10.5, §16)."""
    log_event(
        logging.WARNING,
        "database.transaction.long_running",
        database=database,
        role=role,
        duration_ms=round(duration_ms, 3),
        threshold_ms=threshold_ms,
        outcome=outcome,
    )


def redacted_params_for_log(
    params: Mapping[str, Any] | None,
    *,
    log_parameters: bool,
    sensitive: set[str] | None,
) -> dict[str, Any] | None:
    """Return params suitable for logging, or ``None`` when parameter logging is disabled."""
    if not log_parameters or not params:
        return None
    from .._core.errors.redaction import redact_params

    return redact_params(params, sensitive=sensitive)
