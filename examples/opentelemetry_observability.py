"""Wiring a real OpenTelemetry SDK: traces, metrics, and trace/log correlation (§25.2). Run:

DBKIT_DSN=postgresql+psycopg://localhost/postgres python examples/opentelemetry_observability.py

Requires the ``otel`` extra plus an SDK to actually export anything (dbkit only depends on
``opentelemetry-api``, which is a no-op without an SDK):

    pip install dbkit-sql[otel] opentelemetry-sdk

dbkit itself never configures a ``TracerProvider``/``MeterProvider`` — that's the
application's job (this example does it with in-memory exporters so the output is visible
without a real collector). dbkit only *acquires* a tracer/meter from whatever provider you
hand it, which is exactly what real OTel instrumentation libraries are supposed to do.
"""

from __future__ import annotations

import asyncio
import logging
import os

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from dbkit import AsyncDatabase, DatabaseTarget, sql
from dbkit.observability.metrics import try_otel_metrics_sink
from dbkit.observability.tracing import make_tracer

DSN = os.environ.get("DBKIT_DSN", "postgresql+psycopg://localhost/postgres")
TARGET = DatabaseTarget(database="app", role="write")


class _CorrelationHandler(logging.Handler):
    """Prints the structured payload of every dbkit log record, including trace_id/span_id."""

    def emit(self, record: logging.LogRecord) -> None:
        payload = getattr(record, "dbkit", None)
        if payload is not None:
            print(f"  log: {payload}")


def _print_spans(exporter: InMemorySpanExporter) -> None:
    for span in exporter.get_finished_spans():
        ctx = span.get_span_context()
        trace_id = format(ctx.trace_id, "032x")
        span_id = format(ctx.span_id, "016x")
        print(f"  span: {span.name} kind={span.kind.name} trace_id={trace_id} span_id={span_id}")
        print(f"        attrs={dict(span.attributes)}")


def _print_metrics(reader: InMemoryMetricReader) -> None:
    for rm in reader.get_metrics_data().resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                for point in metric.data.data_points:
                    value = getattr(point, "value", None)
                    if value is None:
                        value = getattr(point, "sum", None)
                    print(f"  metric: {metric.name}={value} attrs={dict(point.attributes)}")


async def main() -> None:
    # 1. Application-owned providers, bound to dbkit via constructor injection (never the
    #    process-global provider — this keeps the example self-contained and repeatable).
    span_exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(span_exporter))

    metric_reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[metric_reader])

    tracer = make_tracer(enabled=True, tracer_provider=tracer_provider)
    metrics = try_otel_metrics_sink(meter_provider=meter_provider)

    # 2. Trace/log correlation: a handler that prints trace_id/span_id alongside every
    #    lifecycle event dbkit logs (query timing, transaction outcome, pool warnings, ...).
    dbkit_logger = logging.getLogger("dbkit")
    dbkit_logger.setLevel(logging.DEBUG)
    dbkit_logger.addHandler(_CorrelationHandler())

    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": DSN}}},
            "defaults": {
                # Thresholds set unreasonably low (production default is 500ms/5s) purely so
                # this example's ordinary-latency queries trigger the log lines below, showing
                # them tagged with the same trace_id/span_id as the enclosing span.
                "observability": {"tracing": True, "slow_query_ms": 0.0},
                "long_transaction_warning_seconds": 0.0,
            },
        },
        metrics=metrics,
        tracer=tracer,
    )
    await db.start()
    try:
        print("--- a read, inside a span (kind=CLIENT); the slow-query log below is tagged ---")
        print("--- with that span's trace_id/span_id ---")
        await db.fetch_value(sql("SELECT 1"), target=TARGET)

        print("\n--- a transaction: one span, one set of metrics, one correlated log ---")
        async with db.transaction(target=TARGET) as tx:
            await tx.execute(sql("SELECT 1"))

        print("\nfinished spans:")
        _print_spans(span_exporter)
        print("\ncollected metrics:")
        _print_metrics(metric_reader)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
