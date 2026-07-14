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
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TypeVar

from .._core.circuit import CircuitBreaker, CircuitState, counts_as_failure
from .._core.config import RetryConfig
from .._core.errors import DatabaseCircuitOpenError, DatabaseError, DatabaseOverloadedError
from .._core.policies import backoff_delay_ms, should_retry
from .._core.query import Query
from ..observability import metrics as m
from ..observability.metrics import MetricsSink
from ._compat import semaphore_acquire, sleep

T = TypeVar("T")

#: Numeric value of each circuit-breaker state, for the ``CIRCUIT_STATE`` gauge (§16).
_CIRCUIT_STATE_VALUE = {
    CircuitState.CLOSED: 0.0,
    CircuitState.HALF_OPEN: 1.0,
    CircuitState.OPEN: 2.0,
}


def _emit_circuit_state(
    breaker: CircuitBreaker, now: float, metrics: MetricsSink, labels: dict[str, str]
) -> None:
    metrics.gauge(m.CIRCUIT_STATE, _CIRCUIT_STATE_VALUE[breaker.state(now)], labels=labels)


async def run_with_retries(
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

    Total time spent retrying (backoff delays only, not attempt execution time) is bounded by
    ``retry.maximum_total_ms`` regardless of ``attempts``/``deadline`` — without this, a caller
    with a generous ``attempts`` count and no explicit ``deadline`` has no bound on how long a
    single logical call can spend retrying (performance review §5/§7).
    """
    attempt = 0
    started = time.monotonic()
    while True:
        attempt += 1
        now = time.monotonic()
        if breaker is not None:
            allowed = breaker.allow(now)
            _emit_circuit_state(breaker, now, metrics, labels)
            if not allowed:
                raise DatabaseCircuitOpenError(
                    "circuit is open for this target",
                    database_name=labels.get("database"),
                    role=labels.get("role"),
                    query_name=labels.get("query_name"),
                )
        try:
            result = await operation()
        except DatabaseError as err:
            if breaker is not None:
                if counts_as_failure(err):
                    breaker.on_failure(time.monotonic())
                _emit_circuit_state(breaker, time.monotonic(), metrics, labels)
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
            # never sleep past the retry budget, independent of any caller deadline
            elapsed_ms = (time.monotonic() - started) * 1000.0
            if elapsed_ms + delay * 1000.0 >= retry.maximum_total_ms:
                raise
            metrics.incr(m.OP_RETRIES, labels={**labels, "retry_attempt": str(attempt)})
            await sleep(delay)
            continue
        else:
            if breaker is not None:
                breaker.on_success(time.monotonic())
                _emit_circuit_state(breaker, time.monotonic(), metrics, labels)
            return result


class ConcurrencyLimiter:
    """Per-tier semaphores bounding concurrent database use (§17).

    Tiers: ``"database"`` (overall), ``"reads"``, ``"writes"``, ``"bulk"``. A tier with no
    configured limit is unbounded (a no-op context).
    """

    def __init__(self, limits: dict[str, int | None]) -> None:
        """``limits`` maps tier name to its cap; a missing/``None``/``<=0`` limit is unbounded."""
        # asyncio.Semaphore in the async build; threading.Semaphore after unasync.
        import asyncio

        self._sems: dict[str, asyncio.Semaphore] = {
            tier: asyncio.Semaphore(n) for tier, n in limits.items() if n and n > 0
        }

    @contextlib.asynccontextmanager
    async def acquire(self, tier: str, *, timeout: float | None = None) -> AsyncIterator[None]:
        """Hold one slot of ``tier`` for the block's duration (no-op if unbounded).

        Raises :class:`DatabaseOverloadedError` if no slot frees up within ``timeout`` —
        without this bound, a saturated tier would queue callers indefinitely, invisibly to
        dbkit's own timeout/deadline machinery (§17).
        """
        sem = self._sems.get(tier)
        if sem is None:
            yield
            return
        if not await semaphore_acquire(sem, timeout):
            raise DatabaseOverloadedError(
                f"concurrency limit reached for tier {tier!r}; no slot freed up within {timeout}s"
            )
        try:
            yield
        finally:
            sem.release()
