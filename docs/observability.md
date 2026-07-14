# Dashboards and Alerting

dbkit exposes metric *names* (`observability/metrics.py`) via a pluggable `MetricsSink` — a
Prometheus adapter (`try_prometheus_sink()`) or an OpenTelemetry Metrics adapter
(`try_otel_metrics_sink()`). This page gives a starting-point Grafana dashboard and
Prometheus/Alertmanager rules so you don't have to figure out which of the ~15 metrics matter
from scratch. All metric names below are exactly what `PrometheusMetrics`/`OTelMetrics` emit —
no additional prefix is added.

## Metrics reference

| Metric | Type | What it tells you |
|---|---|---|
| `db_operation_total` / `db_operation_duration_seconds` | counter / histogram | Read/write throughput and latency, per `database`/`shard`/`role`/`operation` |
| `db_operation_errors_total` | counter, labeled `error_category` | Error rate by category (connection, timeout, integrity, ...) |
| `db_operation_retries_total` | counter | How often the retry executor actually retries |
| `db_pool_wait_seconds` | histogram | Time spent waiting for a pool checkout — the #1 pool-pressure signal |
| `db_pool_size` / `db_pool_checked_out` / `db_pool_overflow` | gauges | Point-in-time pool occupancy (from `pool_status()`/CLI `pools`, or wired to your own scrape loop) |
| `db_connection_hold_duration_seconds` | histogram | How long a connection is checked out — a long tail here indicates a leak or a slow-query problem |
| `db_transaction_total` / `db_transaction_duration_seconds` | counter / histogram | Explicit transaction throughput and duration |
| `db_transaction_rollback_total` | counter | Rollback + cancellation rate |
| `db_commit_unknown_total` | counter | Truly ambiguous commit outcomes (§15) — **should be ~zero** |
| `db_circuit_breaker_state` | gauge, 0/1/2 | Per-target breaker state: closed/half_open/open |
| `db_stream_rows_total` / `db_bulk_rows_total` | counters | Streaming/bulk throughput |

## Example Grafana dashboard

Import this JSON directly (Dashboards → New → Import → paste JSON), then point it at your
Prometheus data source:

```json
{
  "title": "dbkit",
  "schemaVersion": 39,
  "tags": ["dbkit"],
  "timezone": "browser",
  "time": { "from": "now-1h", "to": "now" },
  "refresh": "30s",
  "panels": [
    {
      "id": 1, "title": "Pool wait P99", "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 0 },
      "targets": [{
        "expr": "histogram_quantile(0.99, sum(rate(db_pool_wait_seconds_bucket[5m])) by (le, database))",
        "legendFormat": "{{database}}"
      }]
    },
    {
      "id": 2, "title": "Operation error rate", "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 0 },
      "targets": [{
        "expr": "sum(rate(db_operation_errors_total[5m])) by (database, error_category)",
        "legendFormat": "{{database}} / {{error_category}}"
      }]
    },
    {
      "id": 3, "title": "Circuit breaker state (0=closed 1=half_open 2=open)", "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 },
      "targets": [{
        "expr": "max(db_circuit_breaker_state) by (database, shard, role)",
        "legendFormat": "{{database}}/{{shard}}/{{role}}"
      }]
    },
    {
      "id": 4, "title": "Commit-unknown rate", "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 8 },
      "targets": [{
        "expr": "sum(rate(db_commit_unknown_total[5m])) by (database)",
        "legendFormat": "{{database}}"
      }]
    },
    {
      "id": 5, "title": "Transaction rollback rate", "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 16 },
      "targets": [{
        "expr": "sum(rate(db_transaction_rollback_total[5m])) by (database)",
        "legendFormat": "{{database}}"
      }]
    },
    {
      "id": 6, "title": "Connection hold duration P99", "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 16 },
      "targets": [{
        "expr": "histogram_quantile(0.99, sum(rate(db_connection_hold_duration_seconds_bucket[5m])) by (le, database))",
        "legendFormat": "{{database}}"
      }]
    }
  ]
}
```

## Example Prometheus alerting rules

```yaml
groups:
  - name: dbkit
    rules:
      - alert: DbkitCommitUnknown
        expr: increase(db_commit_unknown_total[5m]) > 0
        for: 0m
        labels:
          severity: page
        annotations:
          summary: "dbkit: commit outcome unknown for {{ $labels.database }}"
          description: >
            A connection failed during COMMIT and the write's outcome is genuinely ambiguous
            (§15). This never auto-retries. Page immediately — check the affected write path
            for a possible duplicate/missing write.

      - alert: DbkitCircuitOpen
        expr: max_over_time(db_circuit_breaker_state[1m]) == 2
        for: 30s
        labels:
          severity: warning
        annotations:
          summary: "dbkit: circuit open for {{ $labels.database }}/{{ $labels.shard }}/{{ $labels.role }}"
          description: "The circuit breaker has been open for at least 30s — the target is failing infra-category checks."

      - alert: DbkitPoolWaitHigh
        expr: histogram_quantile(0.99, sum(rate(db_pool_wait_seconds_bucket[5m])) by (le, database)) > 0.5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "dbkit: pool wait P99 > 500ms for {{ $labels.database }}"
          description: "Sustained pool pressure — consider raising pool size or investigating slow queries holding connections."

      - alert: DbkitRollbackRateHigh
        expr: sum(rate(db_transaction_rollback_total[5m])) by (database) / clamp_min(sum(rate(db_transaction_total[5m])) by (database), 1) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "dbkit: >5% of transactions rolling back for {{ $labels.database }}"

      - alert: DbkitConnectionHoldLong
        expr: histogram_quantile(0.99, sum(rate(db_connection_hold_duration_seconds_bucket[5m])) by (le, database)) > 5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "dbkit: connections held > 5s (P99) for {{ $labels.database }}"
          description: "Possible connection leak or a slow query holding a connection — check `dbkit pools` / structured logs for `database.pool.long_hold`."
```

See `docs/api/observability.md` for the full metrics/tracing/logging API, and `docs/troubleshooting.md`
for what to do when one of these fires.
