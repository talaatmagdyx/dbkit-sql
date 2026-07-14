"""Shared fixtures. Integration tests need a real PostgreSQL, discovered in this order:

1. ``DBKIT_TEST_DSN`` environment variable (e.g. from docker-compose or CI services), or
2. a ``testcontainers`` PostgreSQL container (if the package and Docker are available).

If neither is available, integration tests are skipped.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

import pytest


class RecordingMetrics:
    """A :class:`dbkit.observability.metrics.MetricsSink` test double that records every call
    instead of forwarding to a real backend, so tests can assert exactly what was emitted."""

    def __init__(self) -> None:
        self.incr_calls: list[tuple[str, float, dict[str, str]]] = []
        self.observe_calls: list[tuple[str, float, dict[str, str]]] = []
        self.gauge_calls: list[tuple[str, float, dict[str, str]]] = []

    def incr(self, name: str, value: float = 1.0, labels: Any = None) -> None:
        self.incr_calls.append((name, value, dict(labels or {})))

    def observe(self, name: str, value: float, labels: Any = None) -> None:
        self.observe_calls.append((name, value, dict(labels or {})))

    def gauge(self, name: str, value: float, labels: Any = None) -> None:
        self.gauge_calls.append((name, value, dict(labels or {})))

    def count(self, name: str) -> int:
        return sum(1 for n, _, _ in self.incr_calls if n == name)


@pytest.fixture
def recording_metrics() -> RecordingMetrics:
    return RecordingMetrics()


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
def requires_psycopg(pg_dsn: str) -> None:
    """Skip a test that exercises the psycopg-only raw-driver escape hatch (COPY, pipeline
    mode, PgBouncer autoprep control) — these have no asyncpg equivalent (§7.3, §19.2)."""
    from urllib.parse import urlsplit

    scheme = urlsplit(pg_dsn).scheme
    driver = scheme.split("+", 1)[1] if "+" in scheme else scheme
    if driver not in ("psycopg", "psycopg2"):
        pytest.skip(f"requires the psycopg driver for this raw-driver escape hatch; got {driver!r}")


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
