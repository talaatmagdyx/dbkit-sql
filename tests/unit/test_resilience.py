from __future__ import annotations

import asyncio

import pytest

from dbkit._async.resilience import ConcurrencyLimiter, run_with_retries
from dbkit._core.circuit import CircuitBreaker
from dbkit._core.config import RetryConfig
from dbkit._core.query import Query, sql
from dbkit.errors import (
    DatabaseCircuitOpenError,
    DatabaseConnectionError,
    DatabaseSerializationError,
    DatabaseUniqueViolationError,
)
from dbkit.observability.metrics import CIRCUIT_STATE, NoopMetrics

from ..conftest import RecordingMetrics

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


async def test_maximum_total_ms_bounds_total_retry_time_regardless_of_attempts_remaining() -> None:
    """§7 performance-review finding: ``maximum_total_ms`` must be a real ceiling on total time
    spent retrying, not a dead config field — even when ``attempts`` and ``deadline`` would
    otherwise allow more retries."""
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        raise DatabaseSerializationError("conflict")

    retry = RetryConfig(
        attempts=1000,  # effectively unbounded by attempt count alone
        initial_delay_ms=10,
        maximum_delay_ms=10,
        jitter="none",
        maximum_total_ms=25,  # budget exhausts after ~2-3 attempts at a 10ms backoff
    )
    with pytest.raises(DatabaseSerializationError):
        await run_with_retries(
            op, query=READ, retry=retry, breaker=None, metrics=NoopMetrics(), labels=LABELS
        )
    # far fewer than 1000 attempts — the budget cut it short, not the attempt count
    assert 1 < calls < 10


async def test_maximum_total_ms_does_not_cut_short_a_call_that_finishes_within_budget() -> None:
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise DatabaseSerializationError("conflict")
        return "ok"

    retry = RetryConfig(
        attempts=5,
        initial_delay_ms=1,
        maximum_delay_ms=2,
        maximum_total_ms=750,  # default — plenty of headroom for 2 tiny retries
    )
    result = await run_with_retries(
        op, query=READ, retry=retry, breaker=None, metrics=NoopMetrics(), labels=LABELS
    )
    assert result == "ok"
    assert calls == 3


async def test_breaker_opens_and_blocks() -> None:
    breaker = CircuitBreaker(failure_threshold=3, window_seconds=100, open_seconds=100)

    async def failing() -> str:
        raise DatabaseSerializationError("x")

    # DatabaseSerializationError is CONCURRENCY category -> does NOT trip the breaker by design;
    # use a connection error to trip it.
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


async def test_circuit_state_gauge_reflects_open_after_threshold_breach(
    recording_metrics: RecordingMetrics,
) -> None:
    """The CIRCUIT_STATE gauge is the signal an on-call engineer needs to answer "is the
    breaker open right now" — it must flip to OPEN (2.0) the moment the breaker trips."""
    breaker = CircuitBreaker(failure_threshold=1, window_seconds=100, open_seconds=100)

    async def conn_fail() -> str:
        raise DatabaseConnectionError("down")

    with pytest.raises(DatabaseConnectionError):
        await run_with_retries(
            conn_fail,
            query=READ,
            retry=_retry(attempts=1),
            breaker=breaker,
            metrics=recording_metrics,
            labels=LABELS,
        )
    gauge_values = [v for n, v, _ in recording_metrics.gauge_calls if n == CIRCUIT_STATE]
    assert gauge_values[0] == 0.0  # CLOSED, observed before the failing call
    assert gauge_values[-1] == 2.0  # OPEN, observed right after the failure trips it


def test_circuit_state_gauge_values_match_every_breaker_state() -> None:
    """Direct, clock-injectable check that every :class:`CircuitState` maps to the documented
    gauge value (0=closed, 1=half_open, 2=open), independent of real wall-clock timing."""
    from dbkit._async.resilience import _emit_circuit_state

    metrics = RecordingMetrics()
    breaker = CircuitBreaker(
        failure_threshold=1, window_seconds=10, open_seconds=5, half_open_max_calls=1
    )

    _emit_circuit_state(breaker, 0.0, metrics, LABELS)  # CLOSED
    breaker.on_failure(0.0)
    _emit_circuit_state(breaker, 0.0, metrics, LABELS)  # OPEN
    _emit_circuit_state(breaker, 6.0, metrics, LABELS)  # cooldown elapsed -> HALF_OPEN
    breaker.on_success(6.0)
    _emit_circuit_state(breaker, 6.0, metrics, LABELS)  # CLOSED again

    values = [v for n, v, _ in metrics.gauge_calls if n == CIRCUIT_STATE]
    assert values == [0.0, 2.0, 1.0, 0.0]


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


async def test_concurrency_limiter_saturated_tier_raises_overloaded_within_timeout() -> None:
    """A saturated tier must fail with a classified, observable error within the requested
    timeout — not queue the caller indefinitely (§17)."""
    from dbkit.errors import DatabaseOverloadedError

    limiter = ConcurrencyLimiter({"writes": 1})
    holder_ready = asyncio.Event()
    release_holder = asyncio.Event()

    async def hold_the_slot() -> None:
        async with limiter.acquire("writes"):
            holder_ready.set()
            await release_holder.wait()

    holder_task = asyncio.create_task(hold_the_slot())
    await holder_ready.wait()
    try:
        start = asyncio.get_event_loop().time()
        with pytest.raises(DatabaseOverloadedError, match="writes"):
            async with limiter.acquire("writes", timeout=0.05):
                pass
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed < 1.0  # bounded, not hung
    finally:
        release_holder.set()
        await holder_task


async def test_concurrency_limiter_acquires_once_a_slot_frees_within_timeout() -> None:
    limiter = ConcurrencyLimiter({"writes": 1})
    entered = False

    async def hold_briefly() -> None:
        async with limiter.acquire("writes"):
            await asyncio.sleep(0.05)

    holder_task = asyncio.create_task(hold_briefly())
    await asyncio.sleep(0.01)  # let the holder acquire first
    async with limiter.acquire("writes", timeout=1.0):
        entered = True
    assert entered
    await holder_task
