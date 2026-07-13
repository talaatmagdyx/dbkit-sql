from __future__ import annotations

from dbkit import Query, sql
from dbkit._core.config import RetryConfig
from dbkit._core.policies import (
    backoff_delay_ms,
    effective_timeout,
    resolve_timeout,
    should_retry,
)
from dbkit.errors import (
    DatabaseSerializationError,
    DatabaseUniqueViolationError,
)

READ = Query(name="r", statement=sql("SELECT 1"), operation="read", idempotent=True)
WRITE = Query(name="w", statement=sql("INSERT"), operation="write")
WRITE_IDEMPOTENT = Query(name="wi", statement=sql("INSERT"), operation="write", idempotent=True)


def test_timeout_precedence() -> None:
    q = Query(name="q", statement=sql("X"), timeout=2.0)
    assert resolve_timeout(0.5, q, 5.0) == 0.5  # call wins
    assert resolve_timeout(None, q, 5.0) == 2.0  # query next
    assert resolve_timeout(None, None, 5.0) == 5.0  # default last
    assert resolve_timeout(None, None, None) is None


def test_effective_timeout_takes_tighter_deadline() -> None:
    # base 5s, deadline gives 1s remaining -> 1s
    assert effective_timeout(5.0, None, None, deadline=101.0, now=100.0) == 1.0
    # base 0.5s tighter than 1s remaining -> 0.5s
    assert effective_timeout(0.5, None, None, deadline=101.0, now=100.0) == 0.5


def test_should_retry_read_serialization() -> None:
    cfg = RetryConfig(attempts=3)
    err = DatabaseSerializationError("x")
    assert should_retry(err, query=READ, config=cfg, attempt=1) is True
    # budget exhausted
    assert should_retry(err, query=READ, config=cfg, attempt=3) is False


def test_should_not_retry_non_retryable() -> None:
    cfg = RetryConfig(attempts=3)
    err = DatabaseUniqueViolationError("dup")
    assert should_retry(err, query=READ, config=cfg, attempt=1) is False


def test_should_not_retry_writes_by_default() -> None:
    cfg = RetryConfig(attempts=3, retry_writes=False)
    err = DatabaseSerializationError("x")
    assert should_retry(err, query=WRITE_IDEMPOTENT, config=cfg, attempt=1) is False


def test_retry_idempotent_write_when_enabled() -> None:
    cfg = RetryConfig(attempts=3, retry_writes=True)
    err = DatabaseSerializationError("x")
    assert should_retry(err, query=WRITE_IDEMPOTENT, config=cfg, attempt=1) is True
    # non-idempotent write still not retried
    assert should_retry(err, query=WRITE, config=cfg, attempt=1) is False


def test_commit_unknown_never_retried() -> None:
    cfg = RetryConfig(attempts=3, retry_reads=True)
    err = DatabaseSerializationError("x")
    err.transaction_state_unknown = True
    assert should_retry(err, query=READ, config=cfg, attempt=1) is False


def test_backoff_growth_and_cap() -> None:
    cfg = RetryConfig(initial_delay_ms=10, multiplier=2, maximum_delay_ms=50, jitter="none")
    assert backoff_delay_ms(1, cfg, rand=1.0) == 10
    assert backoff_delay_ms(2, cfg, rand=1.0) == 20
    assert backoff_delay_ms(3, cfg, rand=1.0) == 40
    assert backoff_delay_ms(4, cfg, rand=1.0) == 50  # capped


def test_backoff_full_jitter() -> None:
    cfg = RetryConfig(initial_delay_ms=100, multiplier=1, maximum_delay_ms=100, jitter="full")
    assert backoff_delay_ms(1, cfg, rand=0.5) == 50.0
    assert backoff_delay_ms(1, cfg, rand=0.0) == 0.0
