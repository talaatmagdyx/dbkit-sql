from __future__ import annotations

import asyncio

import pytest

from dbkit._async.resilience import ConcurrencyLimiter, run_with_retries
from dbkit._core.circuit import CircuitBreaker
from dbkit._core.config import RetryConfig
from dbkit._core.query import Query, sql
from dbkit.errors import (
    DatabaseCircuitOpenError,
    DatabaseSerializationError,
    DatabaseUniqueViolationError,
)
from dbkit.observability.metrics import NoopMetrics

READ = Query(name="r", statement=sql("SELECT 1"), operation="read", idempotent=True)
WRITE = Query(name="w", statement=sql("INSERT"), operation="write")
LABELS = {"database": "app", "role": "primary", "query_name": "r"}


def _retry(**kw) -> RetryConfig:
    base = {"attempts": 3, "initial_delay_ms": 1, "maximum_delay_ms": 2, "retry_reads": True}
    base.update(kw)
    return RetryConfig(**base)


async def test_retries_then_succeeds() -> None:
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise DatabaseSerializationError("conflict")
        return "ok"

    result = await run_with_retries(
        op, query=READ, retry=_retry(), breaker=None, metrics=NoopMetrics(), labels=LABELS
    )
    assert result == "ok"
    assert calls == 3


async def test_non_retryable_raises_immediately() -> None:
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        raise DatabaseUniqueViolationError("dup")

    with pytest.raises(DatabaseUniqueViolationError):
        await run_with_retries(
            op, query=READ, retry=_retry(), breaker=None, metrics=NoopMetrics(), labels=LABELS
        )
    assert calls == 1


async def test_budget_exhausted_raises_last_error() -> None:
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        raise DatabaseSerializationError("conflict")

    with pytest.raises(DatabaseSerializationError):
        await run_with_retries(
            op,
            query=READ,
            retry=_retry(attempts=2),
            breaker=None,
            metrics=NoopMetrics(),
            labels=LABELS,
        )
    assert calls == 2


async def test_write_not_retried_by_default() -> None:
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        raise DatabaseSerializationError("conflict")

    with pytest.raises(DatabaseSerializationError):
        await run_with_retries(
            op,
            query=WRITE,
            retry=_retry(retry_writes=False),
            breaker=None,
            metrics=NoopMetrics(),
            labels=LABELS,
        )
    assert calls == 1


async def test_breaker_opens_and_blocks() -> None:
    breaker = CircuitBreaker(failure_threshold=3, window_seconds=100, open_seconds=100)

    async def failing() -> str:
        raise DatabaseSerializationError("x")

    # DatabaseSerializationError is CONCURRENCY category -> does NOT trip the breaker by design;
    # use a connection error to trip it.
    from dbkit.errors import DatabaseConnectionError

    async def conn_fail() -> str:
        raise DatabaseConnectionError("down")

    for _ in range(3):
        with pytest.raises(DatabaseConnectionError):
            await run_with_retries(
                conn_fail,
                query=READ,
                retry=_retry(attempts=1),
                breaker=breaker,
                metrics=NoopMetrics(),
                labels=LABELS,
            )
    # breaker now open -> fast fail
    with pytest.raises(DatabaseCircuitOpenError):
        await run_with_retries(
            failing,
            query=READ,
            retry=_retry(attempts=1),
            breaker=breaker,
            metrics=NoopMetrics(),
            labels=LABELS,
        )


async def test_concurrency_limiter_bounds_parallelism() -> None:
    limiter = ConcurrencyLimiter({"reads": 2})
    active = 0
    peak = 0

    async def work() -> None:
        nonlocal active, peak
        async with limiter.acquire("reads"):
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.05)
            active -= 1

    await asyncio.gather(*[work() for _ in range(6)])
    assert peak <= 2


async def test_concurrency_limiter_unbounded_tier_is_noop() -> None:
    limiter = ConcurrencyLimiter({"reads": None})
    async with limiter.acquire("reads"):
        pass  # must not raise
    async with limiter.acquire("writes"):  # unknown tier -> no-op
        pass
