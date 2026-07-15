# Dynamic Registration

Register databases whose DSNs are discovered at **runtime** — per-tenant shards resolved from
a service registry, credentials rotated by a secret manager — without maintaining an
application-side engine registry.

## Bootstrap dynamic-first

An explicitly empty `databases: {}` mapping is valid; every database arrives later:

```python
from dbkit import AsyncDatabase, DatabaseTarget, Query, sql

db = AsyncDatabase.from_config({
    "databases": {},                # dynamic-first
    "max_databases": 500,           # LRU cap on dynamic registrations (optional)
    "max_engines": 64,              # LRU cap on live engines/pools (optional)
    "evict_lru_engines": True,
    "defaults": {
        "pool": {"size": 10, "max_overflow": 0, "timeout_seconds": 2.0},
        "retry": {"attempts": 2},
        "circuit_breaker": {"enabled": True},
    },
})
await db.start()
```

## `ensure_database` — the per-request pattern

Idempotent: a **lock-free no-op when the config is unchanged** (a few µs), so call it in
front of every query routed by runtime topology:

```python
shard = my_topology.resolve(tenant_id)          # -> name + DSN, however you store it

await db.ensure_database(shard.name, {
    "primary": {"url": shard.dsn, "required": False},
    "concurrency": {"reads": 20},
})
rows = await db.fetch_all(
    LIST_ORDERS, {"tenant": tenant_id},
    target=DatabaseTarget(database=shard.name, role="read"),
)
```

A **changed** config re-registers in place: old engines are disposed (idle-only — in-flight
work finishes), and the next query connects with the new settings. This covers host moves
*and password-only rotations* with no restart.

## Explicit control

```python
replaced = await db.register_database("tenant-42", config)   # register or replace
await db.unregister_database("tenant-42")                    # engines + resilience state freed

async with db.database_scope("migration-src", config) as target:   # scoped lifetime
    rows = await db.fetch_all(EXPORT, target=target)
# auto-unregistered — pools released even on error
```

!!! warning "Scope `database_scope` to a unit of work, never to a request"
    Engines/pools are created inside the block and disposed on exit — a per-request scope
    recreates connections every request and defeats pooling. Long-lived services use
    `ensure_database`/`register_database` and keep shards registered.

## Guarantees

- **Copy-on-write config swap** — readers never observe a partially updated database map;
  the query hot path takes no registration lock (~350 ns regardless of shard count).
- **Admission control** — registering past `connection_budget.maximum_per_process`
  (with `enforce_at_startup: true`) raises before anything is swapped in.
- **`max_databases` LRU** — least-recently-*ensured* dynamic databases are fully purged
  (engines, concurrency limiter, circuit breakers, replica-selector entry) beyond the cap.
  Statically configured databases are never evicted.
- **Replicas** participate immediately: registered replicas update the built-in
  round-robin/weighted selectors (custom selectors may implement `set_replicas`).

## API reference

::: dbkit.AsyncDatabase.ensure_database

::: dbkit.AsyncDatabase.register_database

::: dbkit.AsyncDatabase.unregister_database

::: dbkit.AsyncDatabase.database_scope

::: dbkit.DatabaseConfig.from_dict
