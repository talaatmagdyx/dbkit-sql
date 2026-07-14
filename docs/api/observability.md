# Observability

Structured logging, the pluggable metrics protocol (Prometheus or OpenTelemetry Metrics
adapters), and OpenTelemetry tracing (§25). All three are graceful no-ops by default.

::: dbkit.observability.Tracer

::: dbkit.observability.SpanHandle

::: dbkit.observability.make_tracer

::: dbkit.observability.MetricsSink

::: dbkit.observability.NoopMetrics

::: dbkit.observability.try_prometheus_sink

::: dbkit.observability.try_otel_metrics_sink

::: dbkit.observability.log_event

::: dbkit.observability.slow_query_warning

::: dbkit.observability.long_transaction_warning
