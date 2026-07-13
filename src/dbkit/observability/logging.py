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
