"""Shared fixtures. Integration tests need a real PostgreSQL, discovered in this order:

1. ``DBKIT_TEST_DSN`` environment variable (e.g. from docker-compose or CI services), or
2. a ``testcontainers`` PostgreSQL container (if the package and Docker are available).

If neither is available, integration tests are skipped.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest


def _sync_dsn(async_dsn: str) -> str:
    # psycopg drives both; the same URL works for sync and async engines.
    return async_dsn


@pytest.fixture(scope="session")
def pg_dsn() -> Iterator[str]:
    dsn = os.environ.get("DBKIT_TEST_DSN")
    if dsn:
        yield dsn
        return

    try:
        from testcontainers.postgres import PostgresContainer
    except Exception:
        pytest.skip("no DBKIT_TEST_DSN and testcontainers not installed")
        return

    try:
        with PostgresContainer("postgres:16", driver="psycopg") as pg:
            yield pg.get_connection_url()
    except Exception as exc:
        pytest.skip(f"could not start PostgreSQL test container: {exc}")


@pytest.fixture
def base_config(pg_dsn: str) -> dict:
    return {
        "environment": "test",
        "defaults": {
            "query_timeout_seconds": 5.0,
            "transaction_timeout_seconds": 5.0,
            "pool": {"size": 3, "max_overflow": 2, "timeout_seconds": 2.0},
            "observability": {"slow_query_ms": 100000.0},  # keep test logs quiet
        },
        "databases": {"app": {"primary": {"url": pg_dsn}}},
    }
