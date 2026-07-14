"""OpenTelemetry tracing (§25.2), with a graceful no-op fallback.

If the ``opentelemetry-api`` package is not installed, or config disables tracing, every
call becomes a cheap no-op — the facade never needs to check whether tracing is available.
SQL statement text is never recorded on spans (redaction, §25.2, §29): only the logical query
name, target metadata, and outcome are attached.
"""

from __future__ import annotations

import contextlib
import importlib.metadata
from collections.abc import Iterator, Mapping
from typing import Any

try:
    from opentelemetry import trace

    _OTEL_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised by the no-otel test lane
    _OTEL_AVAILABLE = False

try:
    _LIBRARY_VERSION = importlib.metadata.version("dbkit")
except importlib.metadata.PackageNotFoundError:  # pragma: no cover - unbuilt source checkout
    _LIBRARY_VERSION = "0.0.0"


class Tracer:
    """Wraps an OpenTelemetry tracer; every method is a no-op if OTel isn't installed/enabled.

    ``tracer_provider``/``schema_url``/``attributes`` are the same parameters
    ``opentelemetry.trace.get_tracer`` itself accepts — pass a ``tracer_provider`` to bind to a
    specific (non-global) provider, e.g. per-tenant tracing or test isolation.
    """

    def __init__(
        self,
        *,
        enabled: bool,
        tracer_provider: Any = None,
        schema_url: str | None = None,
        attributes: Mapping[str, Any] | None = None,
    ) -> None:
        """A no-op tracer if ``enabled`` is False or ``opentelemetry-api`` isn't installed."""
        self._enabled = enabled and _OTEL_AVAILABLE
        self._tracer = (
            trace.get_tracer(
                "dbkit",
                _LIBRARY_VERSION,
                tracer_provider=tracer_provider,
                schema_url=schema_url,
                attributes=attributes,
            )
            if self._enabled
            else None
        )

    @property
    def enabled(self) -> bool:
        """Whether this tracer actually records spans (False = full no-op)."""
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
        # default to True) — no manual handling needed here. kind=CLIENT per the OTel semantic
        # conventions for database client spans (the default is INTERNAL).
        with self._tracer.start_as_current_span(
            name, kind=trace.SpanKind.CLIENT, attributes=attributes
        ) as span:
            yield SpanHandle(span)


class SpanHandle:
    """Thin wrapper so callers can set attributes without checking ``None``/OTel presence."""

    def __init__(self, span: Any) -> None:
        """Wrap ``span``, or ``None`` for a no-op handle."""
        self._span = span

    def set_attribute(self, key: str, value: Any) -> None:
        """Set a span attribute; a no-op if this handle wraps no real span."""
        if self._span is not None:
            self._span.set_attribute(key, value)


def make_tracer(
    enabled: bool,
    *,
    tracer_provider: Any = None,
    schema_url: str | None = None,
    attributes: Mapping[str, Any] | None = None,
) -> Tracer:
    """Build a :class:`Tracer` (see its docstring for ``tracer_provider``/``schema_url``/
    ``attributes``)."""
    return Tracer(
        enabled=enabled,
        tracer_provider=tracer_provider,
        schema_url=schema_url,
        attributes=attributes,
    )
