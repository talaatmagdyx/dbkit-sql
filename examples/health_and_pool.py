"""Health checks, pool introspection, and config loading (§10, §26, §30). Run:

DBKIT_DSN=postgresql+psycopg://localhost/postgres python examples/health_and_pool.py
"""

from __future__ import annotations

import asyncio
import os

from dbkit import AsyncDatabase, DatabaseTarget, DbkitConfig, sql

DSN = os.environ.get("DBKIT_DSN", "postgresql+psycopg://localhost/postgres")
TARGET = DatabaseTarget(database="app", role="read")


async def main() -> None:
    # Config can be built from a dict (as in the other examples) or validated standalone.
    config = DbkitConfig.from_dict(
        {
            "environment": "example",
            "databases": {"app": {"primary": {"url": DSN}}},
            "defaults": {"pool": {"size": 5, "max_overflow": 2}},
            "connection_budget": {"maximum_per_process": 50, "enforce_at_startup": True},
        }
    )
    print(f"connection budget report: {config.connection_budget_report(replicas=3)}")
    print(f"config (secrets redacted): {config.redacted().databases['app'].primary.url}")

    db = AsyncDatabase(config)
    await db.start()
    try:
        # Readiness: verifies every required target answers SELECT 1.
        report = await db.health()
        print(f"\nhealth: live={report.live} ready={report.ready}")
        for t in report.targets:
            print(f"  target {t.key}: healthy={t.healthy}")

        # Drive some load, then inspect the pool.
        await asyncio.gather(*[db.fetch_value(sql("SELECT 1"), target=TARGET) for _ in range(20)])
        for snap in db.pool_status():
            d = snap.to_dict()
            print(f"\npool {d['key']}:")
            print(
                f"  size={d['size']} checked_out={d['checked_out']} "
                f"utilization={d['utilization']:.0%}"
            )
            print(
                f"  created={d['created']} closed={d['closed']} invalidations={d['invalidations']}"
            )
    finally:
        await db.close()
        print("\nafter close(): all engines disposed, no leaked connections")


if __name__ == "__main__":
    asyncio.run(main())
