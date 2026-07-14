"""Pure policy decisions: timeout resolution and retry eligibility (§12, §14).

These functions contain no I/O and no async — they are the shared brain used by both the
sync and async execution pipelines.
"""

from __future__ import annotations

from .config import RetryConfig
from .errors import DatabaseError
from .query import Query


def resolve_timeout(
    call_timeout: float | None,
    query: Query | None,
    default_timeout: float | None,
) -> float | None:
    """Pick the effective per-operation timeout (§12.1).

    Precedence: explicit call argument > ``Query.timeout`` > config default. ``None`` at every
    level means "no dbkit-imposed timeout" (the DB/pool timeouts still apply).
    """
    if call_timeout is not None:
        return call_timeout
    if query is not None and query.timeout is not None:
        return query.timeout
    return default_timeout


def remaining_deadline(deadline: float | None, now: float) -> float | None:
    """Seconds left until ``deadline`` (a monotonic timestamp), or ``None`` if unset."""
    if deadline is None:
        return None
    return deadline - now


def effective_timeout(
    call_timeout: float | None,
    query: Query | None,
    default_timeout: float | None,
    deadline: float | None,
    now: float,
) -> float | None:
    """Combine the resolved timeout with any caller deadline, taking the tighter bound (§12.2)."""
    base = resolve_timeout(call_timeout, query, default_timeout)
    remaining = remaining_deadline(deadline, now)
    candidates = [c for c in (base, remaining) if c is not None]
    if not candidates:
        return None
    return min(candidates)


def is_idempotent(query: Query | None, idempotent_override: bool | None) -> bool:
    """Whether the call may be retried: ``idempotent_override`` if given, else the query's."""
    if idempotent_override is not None:
        return idempotent_override
    return bool(query and query.idempotent)


def should_retry(
    error: DatabaseError,
    *,
    query: Query | None,
    config: RetryConfig,
    attempt: int,
    idempotent_override: bool | None = None,
) -> bool:
    """Decide whether an operation may be retried after ``error`` (§14).

    ``attempt`` is 1-based (the attempt that just failed). A retry is permitted only when the
    error is retryable, the budget allows another attempt, and — for writes — the operation is
    known-idempotent and write retries are enabled.
    """
    if attempt >= config.attempts:
        return False
    if not error.retryable:
        return False
    # A genuinely unknown commit outcome is never retried automatically (§15).
    if error.transaction_state_unknown:
        return False

    is_write = bool(query and query.is_write)
    if is_write:
        if not config.retry_writes:
            return False
        if not is_idempotent(query, idempotent_override):
            return False
    else:
        if not config.retry_reads:
            return False
    return True


def backoff_delay_ms(attempt: int, config: RetryConfig, rand: float) -> float:
    """Exponential backoff with optional full jitter (§14.4).

    ``attempt`` is 1-based; ``rand`` is a caller-supplied value in ``[0, 1)`` (injected so the
    function stays pure and testable). Returns milliseconds, capped by ``maximum_delay_ms``.
    """
    exp = config.initial_delay_ms * (config.multiplier ** max(attempt - 1, 0))
    capped = min(exp, config.maximum_delay_ms)
    if config.jitter == "full":
        return capped * rand
    return capped
