# Integrations

Database-side primitives for message-driven consumers (§28) and micro-batching (§17.1).
dbkit is scoped as database-only and does not ship a broker-facing adapter — an application
wires these into its own consumer loop.

::: dbkit.integrations.inbox_ddl

::: dbkit.integrations.partitioned_inbox_ddl

::: dbkit.integrations.inbox_month_partition_ddl

::: dbkit.integrations.process_once

::: dbkit.integrations.ack_after_commit

::: dbkit.integrations.BatchCollector
