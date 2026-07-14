"""Unit tests for ``shield_from_cancellation`` (§12.3, performance review Finding #3).

The old ``cancellation_shield()`` was a literal no-op — its "best-effort protection" docstring
claimed a guarantee the implementation never provided. These tests inject a cancellation
precisely while the protected work is in flight (not just at the call boundary), which is the
scenario the no-op implementation could never have passed.
"""

from __future__ import annotations

import asyncio

import pytest

from dbkit._async._compat import shield_from_cancellation


async def test_shield_runs_protected_work_to_completion_despite_a_mid_flight_cancellation() -> None:
    completed = False

    async def slow_cleanup() -> None:
        nonlocal completed
        await asyncio.sleep(0.1)  # the cancellation lands while this await is in flight
        completed = True

    async def caller() -> None:
        await shield_from_cancellation(slow_cleanup())

    task = asyncio.ensure_future(caller())
    await asyncio.sleep(0.02)  # let `caller` start awaiting the shield
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    # The protected cleanup must have run to completion before the cancellation was allowed
    # to propagate — this is exactly what the old no-op shield could not guarantee.
    assert completed is True


async def test_shield_does_not_swallow_the_cancellation() -> None:
    """A shield must defer cancellation, never eat it — the caller still needs to know its
    task was cancelled so it doesn't keep running as if nothing happened."""

    async def instant_cleanup() -> None:
        return None

    async def caller() -> str:
        await shield_from_cancellation(instant_cleanup())
        return "unreachable if cancellation is correctly re-raised"

    task = asyncio.ensure_future(caller())
    await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


async def test_shield_propagates_a_real_exception_from_protected_work() -> None:
    """Shielding must not turn a genuine failure in the protected work into a silent success —
    only cancellation gets special (deferred, not swallowed) handling."""

    async def failing_cleanup() -> None:
        raise RuntimeError("cleanup itself failed")

    with pytest.raises(RuntimeError, match="cleanup itself failed"):
        await shield_from_cancellation(failing_cleanup())


async def test_shield_returns_the_protected_coroutines_result_on_the_happy_path() -> None:
    async def compute() -> int:
        return 42

    assert await shield_from_cancellation(compute()) == 42
