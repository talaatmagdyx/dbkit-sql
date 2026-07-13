"""Shared utilities for real-PostgreSQL benchmarks."""

from __future__ import annotations

import contextlib
import logging
import os
import time
from collections.abc import Callable, Iterator
from typing import Any

logging.getLogger("dbkit").setLevel(logging.ERROR)
logging.getLogger("sqlalchemy").setLevel(logging.CRITICAL)

IMAGE = "postgres:16"


def resolve_dsn(cli_url: str | None = None) -> tuple[str, Any]:
    """Return ``(dsn, container_or_None)``. Order: CLI arg > env var > testcontainers.

    The caller is responsible for stopping the container (use :func:`dsn_context`).
    """
    dsn = cli_url or os.environ.get("DBKIT_BENCH_DSN") or os.environ.get("DBKIT_TEST_DSN")
    if dsn:
        return dsn, None
    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer(IMAGE, driver="psycopg")
    container.start()
    return container.get_connection_url(), container


@contextlib.contextmanager
def dsn_context(cli_url: str | None = None) -> Iterator[str]:
    dsn, container = resolve_dsn(cli_url)
    try:
        yield dsn
    finally:
        if container is not None:
            with contextlib.suppress(Exception):
                container.stop()


def time_block(fn: Callable[[], None]) -> float:
    """Run ``fn`` once, return elapsed seconds (monotonic)."""
    start = time.monotonic()
    fn()
    return time.monotonic() - start


def interleave_ab(
    label_a: str,
    run_a: Callable[[], float],
    label_b: str,
    run_b: Callable[[], float],
    reps: int,
) -> tuple[list[float], list[float]]:
    """Run A and B alternately rep-by-rep so drift biases both sides equally (§33.2)."""
    a_samples: list[float] = []
    b_samples: list[float] = []
    for _ in range(reps):
        a_samples.append(run_a())
        b_samples.append(run_b())
    return a_samples, b_samples


def rule(title: str = "", width: int = 72) -> None:
    if title:
        print(f"\n{title}")
    print("=" * width)
