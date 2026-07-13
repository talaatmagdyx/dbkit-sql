from __future__ import annotations

from dbkit._core.circuit import CircuitBreaker, CircuitState, counts_as_failure
from dbkit.errors import (
    DatabaseConnectionError,
    DatabaseUniqueViolationError,
)


def test_counts_as_failure_only_infrastructure() -> None:
    assert counts_as_failure(DatabaseConnectionError("x")) is True
    # integrity / programming errors are the caller's fault; they must not trip the breaker
    assert counts_as_failure(DatabaseUniqueViolationError("x")) is False


def test_opens_after_threshold() -> None:
    cb = CircuitBreaker(failure_threshold=3, window_seconds=10, open_seconds=5)
    t = 100.0
    assert cb.allow(t) is True
    for _ in range(3):
        cb.on_failure(t)
    assert cb.state(t) == CircuitState.OPEN
    assert cb.allow(t) is False


def test_window_prunes_old_failures() -> None:
    cb = CircuitBreaker(failure_threshold=3, window_seconds=10, open_seconds=5)
    cb.on_failure(100.0)
    cb.on_failure(105.0)
    # third failure is outside the 10s window from the first two -> still only 1 recent
    cb.on_failure(120.0)
    assert cb.state(120.0) == CircuitState.CLOSED


def test_half_open_then_close_on_success() -> None:
    cb = CircuitBreaker(
        failure_threshold=2, window_seconds=10, open_seconds=5, half_open_max_calls=1
    )
    cb.on_failure(100.0)
    cb.on_failure(100.0)
    assert cb.state(100.0) == CircuitState.OPEN
    # after cooldown -> half-open, one trial allowed
    assert cb.state(106.0) == CircuitState.HALF_OPEN
    assert cb.allow(106.0) is True
    assert cb.allow(106.0) is False  # only one trial
    cb.on_success(106.0)
    assert cb.state(106.0) == CircuitState.CLOSED


def test_half_open_reopens_on_failure() -> None:
    cb = CircuitBreaker(failure_threshold=1, window_seconds=10, open_seconds=5)
    cb.on_failure(100.0)
    assert cb.state(106.0) == CircuitState.HALF_OPEN
    cb.allow(106.0)
    cb.on_failure(106.0)
    assert cb.state(106.0) == CircuitState.OPEN
    assert cb.allow(106.0) is False
