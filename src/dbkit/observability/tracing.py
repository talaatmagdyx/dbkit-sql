"""OpenTelemetry tracing (§25.2), with a graceful no-op fallback.

If the ``opentelemetry-api`` package is not installed, or config disables tracing, every
call becomes a cheap no-op — the facade never needs to check whether tracing is available.
SQL statement text is never recorded on spans (redaction, §25.2, §29): only the logical query
name, target metadata, and outcome are attached.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from typing import Any

try:
    from opentelemetry import trace

    _OTEL_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised by the no-otel test lane
    _OTEL_AVAILABLE = False


class Tracer:
    """Wraps an OpenTelemetry tracer; every method is a no-op if OTel isn't installed/enabled."""

    def __init__(self, *, enabled: bool, service_name: str = "dbkit") -> None:
        self._enabled = enabled and _OTEL_AVAILABLE
        self._tracer = trace.get_tracer(service_name) if self._enabled else None

    @property
    def enabled(self) -> bool:
        return self._enabled

    @contextlib.contextmanager
    def span(
        self,
        name: str,
        *,
        db_system: str = "postgresql",
        operation_type: str | None = None,
        query_name: str | None = None,
        database: str | None = None,
        shard: str | None = None,
        role: str | None = None,
    ) -> Iterator[SpanHandle]:
        """A database operation span (§25.2). Attributes never include SQL text or params."""
        if not self._enabled or self._tracer is None:
            yield SpanHandle(None)
            return
        attributes: dict[str, Any] = {"db.system": db_system}
        if operation_type:
            attributes["db.operation.type"] = operation_type
        if query_name:
            attributes["db.query.name"] = query_name
        if database:
            attributes["db.namespace"] = database
        if shard:
            attributes["db.shard.id"] = shard
        if role:
            attributes["db.target.role"] = role
        # start_as_current_span records exceptions and sets ERROR status automatically when
        # the exception propagates out of this block (record_exception/set_status_on_exception
        # default to True) — no manual handling needed here.
        with self._tracer.start_as_current_span(name, attributes=attributes) as span:
            yield SpanHandle(span)


class SpanHandle:
    """Thin wrapper so callers can set attributes without checking ``None``/OTel presence."""

    def __init__(self, span: Any) -> None:
        self._span = span

    def set_attribute(self, key: str, value: Any) -> None:
        if self._span is not None:
            self._span.set_attribute(key, value)


def make_tracer(enabled: bool, service_name: str = "dbkit") -> Tracer:
    return Tracer(enabled=enabled, service_name=service_name)
