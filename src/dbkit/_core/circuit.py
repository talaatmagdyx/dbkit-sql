"""Circuit breaker (§16).

One breaker per ``database + shard + role``. It is a pure state machine with the clock
injected (``now`` is a monotonic timestamp passed in), so it is shared by both frontends and
fully unit-testable without sleeping.

States:

* ``CLOSED``    — calls flow; failures inside a rolling window are counted.
* ``OPEN``      — calls are rejected fast for ``open_seconds`` after the threshold is crossed.
* ``HALF_OPEN`` — a limited number of trial calls are allowed; a success closes the breaker,
  a failure re-opens it.

Only *infrastructure* failures should be reported (connection/pool/availability/timeout) — not
integrity, programming, or permission errors, which are the caller's fault and would trip the
breaker for no reason (§16).
"""

from __future__ import annotations

import enum
from collections import deque

from .errors import DatabaseError, ErrorCategory

# Categories that indicate the *backend* is unhealthy and should count toward opening (§16).
TRIPPING_CATEGORIES = frozenset(
    {
        ErrorCategory.AVAILABILITY,
        ErrorCategory.CONNECTION,
        ErrorCategory.POOL,
        ErrorCategory.TIMEOUT,
    }
)


def counts_as_failure(error: DatabaseError) -> bool:
    """Whether an error should count toward opening the breaker."""
    return error.category in TRIPPING_CATEGORIES


class CircuitState(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """A single breaker. All methods take an explicit monotonic ``now``."""

    def __init__(
        self,
        *,
        failure_threshold: int = 10,
        window_seconds: float = 30.0,
        open_seconds: float = 10.0,
        half_open_max_calls: int = 2,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.window_seconds = window_seconds
        self.open_seconds = open_seconds
        self.half_open_max_calls = half_open_max_calls
        self._state = CircuitState.CLOSED
        self._failures: deque[float] = deque()
        self._opened_at: float = 0.0
        self._half_open_calls = 0

    # -- introspection ------------------------------------------------------------ #

    def state(self, now: float) -> CircuitState:
        """Current state, advancing OPEN -> HALF_OPEN if the cooldown has elapsed."""
        if self._state == CircuitState.OPEN and now - self._opened_at >= self.open_seconds:
            self._state = CircuitState.HALF_OPEN
            self._half_open_calls = 0
        return self._state

    # -- gate --------------------------------------------------------------------- #

    def allow(self, now: float) -> bool:
        """Whether a call may proceed. In HALF_OPEN, only a limited number of trials pass."""
        state = self.state(now)
        if state == CircuitState.CLOSED:
            return True
        if state == CircuitState.OPEN:
            return False
        # HALF_OPEN: admit up to half_open_max_calls trial calls.
        if self._half_open_calls < self.half_open_max_calls:
            self._half_open_calls += 1
            return True
        return False

    # -- feedback ----------------------------------------------------------------- #

    def on_success(self, now: float) -> None:
        if self._state == CircuitState.HALF_OPEN:
            self._reset()
        elif self._state == CircuitState.CLOSED:
            self._prune(now)

    def on_failure(self, now: float) -> None:
        if self._state == CircuitState.HALF_OPEN:
            self._open(now)
            return
        self._failures.append(now)
        self._prune(now)
        if len(self._failures) >= self.failure_threshold:
            self._open(now)

    # -- internals ---------------------------------------------------------------- #

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._failures and self._failures[0] < cutoff:
            self._failures.popleft()

    def _open(self, now: float) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = now
        self._failures.clear()
        self._half_open_calls = 0

    def _reset(self) -> None:
        self._state = CircuitState.CLOSED
        self._failures.clear()
        self._half_open_calls = 0
