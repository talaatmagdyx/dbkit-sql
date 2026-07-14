"""Automatic retries and the circuit breaker (§14, §16). Run:

DBKIT_DSN=postgresql+psycopg://localhost/postgres python examples/retries_and_circuit_breaker.py
"""

from __future__ import annotations

import asyncio
import os

from dbkit import AsyncDatabase, DatabaseTarget, Query, sql
from dbkit.errors import DatabaseCircuitOpenError, DatabaseConnectionError

DSN = os.environ.get("DBKIT_DSN", "postgresql+psycopg://localhost/postgres")
TARGET = DatabaseTarget(database="app", role="write")


async def demo_retry_on_serialization_failure() -> None:
    """An idempotent read that fails with a transient SQLSTATE 40001 twice, then succeeds.
    dbkit retries it transparently — the caller sees only the final result."""
    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": DSN}}},
            "defaults": {"retry": {"attempts": 5, "initial_delay_ms": 5, "retry_reads": True}},
        }
    )
    await db.start()
    try:
        await db.execute(sql("DROP SEQUENCE IF EXISTS dbkit_demo_seq"), target=TARGET)
        await db.execute(sql("CREATE SEQUENCE dbkit_demo_seq"), target=TARGET)
        await db.execute(
            sql(
                """
                CREATE OR REPLACE FUNCTION dbkit_flaky_demo() RETURNS int
                LANGUAGE plpgsql AS $$
                DECLARE cur int;
                BEGIN
                    cur := nextval('dbkit_demo_seq');
                    IF cur < 3 THEN
                        RAISE EXCEPTION 'transient failure (attempt %)', cur
                            USING ERRCODE = '40001';
                    END IF;
                    RETURN cur;
                END;
                $$;
                """
            ),
            target=TARGET,
        )
        flaky = Query(
            name="demo.flaky_read",
            statement=sql("SELECT dbkit_flaky_demo()"),
            operation="read",
            idempotent=True,
        )
        value = await db.fetch_value(flaky, target=TARGET)
        print(f"retry demo: succeeded on attempt {value} (caller never saw the first 2 failures)")
    finally:
        await db.close()


async def demo_writes_not_retried_by_default() -> None:
    """Writes are NOT retried unless declared idempotent — this is the safe default (§14.2)."""
    db = AsyncDatabase.from_config({"databases": {"app": {"primary": {"url": DSN}}}})
    await db.start()
    try:
        write = Query(name="demo.write", statement=sql("SELECT 1"), operation="write")
        print(f"write query idempotent={write.idempotent} -> retry_writes default is False")
        print("a serialization failure on this write would propagate immediately, not retry")
    finally:
        await db.close()


async def demo_circuit_breaker_opens() -> None:
    """After enough connection failures, the breaker opens and fails fast instead of hammering
    a downed backend with new connection attempts."""
    dead_dsn = "postgresql+psycopg://nobody@127.0.0.1:1/none"
    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": dead_dsn}}},
            "defaults": {
                "query_timeout_seconds": 1.0,
                "pool": {"connect_timeout_seconds": 1, "pre_ping": True},
                "retry": {"attempts": 1},
                "circuit_breaker": {
                    "enabled": True,
                    "failure_threshold": 3,
                    "window_seconds": 30,
                    "open_seconds": 30,
                },
            },
        }
    )
    db._started = True  # skip startup (which would fail against the dead target)
    try:
        for i in range(6):
            try:
                await db.fetch_value(sql("SELECT 1"), target=TARGET)
            except DatabaseCircuitOpenError:
                print(f"attempt {i + 1}: circuit OPEN — failed fast, no connection attempt made")
                break
            except DatabaseConnectionError:
                print(f"attempt {i + 1}: connection failed (counted toward the breaker)")
    finally:
        await db.close()


async def main() -> None:
    await demo_retry_on_serialization_failure()
    print()
    await demo_writes_not_retried_by_default()
    print()
    await demo_circuit_breaker_opens()


if __name__ == "__main__":
    asyncio.run(main())
