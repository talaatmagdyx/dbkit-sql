# This file is GENERATED from ../_async/resilience.py by tools/run_unasync.py.
# Do not edit by hand. Run `make unasync` after changing the async source.

"""Retry executor and concurrency limiter (§14, §17).

The retry *decisions* (idempotency gating, backoff shape) live in the pure ``_core.policies``
module; this is the async loop that applies them, respects the caller deadline, and drives the
circuit breaker. The concurrency limiter bounds how many operations may use the database at
once, independently of pool size — and is acquired *before* pool checkout so queueing happens
in cheap asyncio waiters, not held connections (§17).
"""

from __future__ import annotations

import contextlib
import random
import time
from collections.abc import Awaitable, Callable
from typing import TypeVar

from .._core.circuit import CircuitBreaker, counts_as_failure
from .._core.config import RetryConfig
from .._core.errors import DatabaseCircuitOpenError, DatabaseError
from .._core.policies import backoff_delay_ms, should_retry
from .._core.query import Query
from ..observability import metrics as m
from ..observability.metrics import MetricsSink
from ._compat import sleep

T = TypeVar("T")


def run_with_retries(
    operation: Callable[[], Awaitable[T]],
    *,
    query: Query | None,
    retry: RetryConfig,
    breaker: CircuitBreaker | None,
    metrics: MetricsSink,
    labels: dict[str, str],
    idempotent_override: bool | None = None,
    deadline: float | None = None,
) -> T:
    """Run ``operation`` with conservative, idempotency-gated retries (§14).

    Each attempt runs a *fresh* operation (new connection/transaction) — the caller supplies a
    thunk that re-does the work. Non-retryable errors, exhausted budgets, unknown-commit
    outcomes, and non-idempotent writes propagate immediately.
    """
    attempt = 0
    while True:
        attempt += 1
        now = time.monotonic()
        if breaker is not None and not breaker.allow(now):
            raise DatabaseCircuitOpenError(
                "circuit is open for this target",
                database_name=labels.get("database"),
                role=labels.get("role"),
                query_name=labels.get("query_name"),
            )
        try:
            result = operation()
        except DatabaseError as err:
            if breaker is not None and counts_as_failure(err):
                breaker.on_failure(time.monotonic())
            if not should_retry(
                err,
                query=query,
                config=retry,
                attempt=attempt,
                idempotent_override=idempotent_override,
            ):
                raise
            delay = backoff_delay_ms(attempt, retry, rand=random.random()) / 1000.0
            # never sleep past the caller deadline
            if deadline is not None and time.monotonic() + delay >= deadline:
                raise
            metrics.incr(m.OP_RETRIES, labels={**labels, "retry_attempt": str(attempt)})
            sleep(delay)
            continue
        else:
            if breaker is not None:
                breaker.on_success(time.monotonic())
            return result


class ConcurrencyLimiter:
    """Per-tier semaphores bounding concurrent database use (§17).

    Tiers: ``"database"`` (overall), ``"reads"``, ``"writes"``, ``"bulk"``. A tier with no
    configured limit is unbounded (a no-op context).
    """

    def __init__(self, limits: dict[str, int | None]) -> None:
        # threading.Semaphore in the async build; threading.Semaphore after unasync.
        import threading

        self._sems: dict[str, threading.Semaphore] = {
            tier: threading.Semaphore(n) for tier, n in limits.items() if n and n > 0
        }

    def acquire(self, tier: str) -> contextlib.AbstractContextManager[None]:
        sem = self._sems.get(tier)
        if sem is None:
            return contextlib.nullcontext()
        return sem
