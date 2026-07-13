"""Property-based invariants (hypothesis) for the pure-logic kernel.

These assert properties that must hold for *all* inputs, not just hand-picked examples:
redaction never leaks, classification is total, timeout resolution is a lower bound, backoff is
bounded and monotonic, and the connection-budget is the exact sum of pool ceilings.
"""

from __future__ import annotations

import string

from hypothesis import given, settings
from hypothesis import strategies as st

from dbkit import DbkitConfig, sql
from dbkit._core.config import RetryConfig
from dbkit._core.errors import classify
from dbkit._core.errors.base import DatabaseError
from dbkit._core.errors.redaction import REDACTED, is_sensitive_key, redact_dsn, redact_params
from dbkit._core.errors.sqlstate import error_class_for_sqlstate
from dbkit._core.policies import backoff_delay_ms, effective_timeout, resolve_timeout
from dbkit._core.query import Query

# --- redaction --------------------------------------------------------------------- #

_identifiers = st.text(alphabet=string.ascii_letters + "_", min_size=1, max_size=20)


@given(
    params=st.dictionaries(_identifiers, st.integers() | st.text(), max_size=10),
    sensitive=st.sets(_identifiers, max_size=5),
)
def test_redact_params_never_leaks_flagged_values(params, sensitive) -> None:
    out = redact_params(params, sensitive=sensitive)
    for key in params:
        if key in sensitive or is_sensitive_key(key):
            assert out[key] == REDACTED
        else:
            assert out[key] == params[key]
    # never adds or drops keys
    assert set(out) == set(params)


# Realistic URL-password characters: no delimiters (@ / :) and no whitespace (a real DSN
# would percent-encode those). The redactor deliberately stops at whitespace.
_pw_alphabet = string.ascii_letters + string.digits + "-._~%!$&'()*+,;="


@given(pw=st.text(alphabet=_pw_alphabet, min_size=1, max_size=40))
def test_redact_dsn_removes_password(pw) -> None:
    dsn = f"postgresql+psycopg://user:{pw}@host:5432/db"
    out = redact_dsn(dsn)
    # the credential no longer appears in ``:pw@`` position (pw may be a substring of
    # "user"/"host" by chance, so assert the exact credential pattern is gone)
    assert f":{pw}@" not in out
    assert REDACTED in out
    assert "user" in out and "host" in out


# --- classifier totality ----------------------------------------------------------- #

_sqlstate = st.text(alphabet=string.ascii_uppercase + string.digits, min_size=5, max_size=5)


@given(code=_sqlstate)
def test_sqlstate_lookup_is_total(code) -> None:
    result = error_class_for_sqlstate(code)
    assert result is None or (isinstance(result, type) and issubclass(result, DatabaseError))


class _FakeDriverError(Exception):
    def __init__(self, sqlstate: str) -> None:
        super().__init__("driver error")
        self.sqlstate = sqlstate


@given(code=_sqlstate)
@settings(max_examples=200)
def test_classify_always_returns_database_error(code) -> None:
    err = classify(_FakeDriverError(code), query_name="q", database_name="app")
    assert isinstance(err, DatabaseError)
    assert isinstance(err.retryable, bool)
    assert err.sqlstate == code
    # context is always propagated
    assert err.query_name == "q"
    assert err.database_name == "app"


# --- timeout resolution ------------------------------------------------------------- #

_opt_pos = st.one_of(st.none(), st.floats(min_value=0.001, max_value=1000, allow_nan=False))


@given(call=_opt_pos, qt=_opt_pos, default=_opt_pos)
def test_resolve_timeout_precedence(call, qt, default) -> None:
    q = Query(name="q", statement=sql("SELECT 1"), timeout=qt)
    resolved = resolve_timeout(call, q, default)
    expected = call if call is not None else (qt if qt is not None else default)
    assert resolved == expected


@given(
    call=_opt_pos,
    default=_opt_pos,
    deadline_remaining=st.floats(min_value=0.001, max_value=1000, allow_nan=False),
)
def test_effective_timeout_is_lower_bound(call, default, deadline_remaining) -> None:
    now = 1000.0
    deadline = now + deadline_remaining
    eff = effective_timeout(call, None, default, deadline, now)
    # the base timeout follows precedence: call wins over default (query is None here).
    base = call if call is not None else default
    # effective timeout never exceeds the active base or the deadline remaining.
    for bound in (base, deadline_remaining):
        if bound is not None:
            assert eff is not None and eff <= bound + 1e-9


# --- backoff ------------------------------------------------------------------------ #


@given(
    attempt=st.integers(min_value=1, max_value=12),
    rand=st.floats(min_value=0.0, max_value=1.0),
    initial=st.floats(min_value=1, max_value=100),
    maximum=st.floats(min_value=100, max_value=10000),
)
def test_backoff_never_exceeds_maximum(attempt, rand, initial, maximum) -> None:
    for jitter in ("full", "none"):
        cfg2 = RetryConfig(
            initial_delay_ms=initial, maximum_delay_ms=maximum, multiplier=2.0, jitter=jitter
        )
        delay = backoff_delay_ms(attempt, cfg2, rand=rand)
        assert 0.0 <= delay <= maximum + 1e-6


@given(
    a=st.integers(min_value=1, max_value=8),
    b=st.integers(min_value=1, max_value=8),
)
def test_backoff_monotonic_without_jitter(a, b) -> None:
    cfg = RetryConfig(initial_delay_ms=10, maximum_delay_ms=1e9, multiplier=2.0, jitter="none")
    lo, hi = sorted((a, b))
    assert backoff_delay_ms(lo, cfg, rand=1.0) <= backoff_delay_ms(hi, cfg, rand=1.0)


# --- connection budget -------------------------------------------------------------- #


@given(
    n_dbs=st.integers(min_value=1, max_value=6),
    size=st.integers(min_value=0, max_value=50),
    overflow=st.integers(min_value=0, max_value=50),
    replicas=st.integers(min_value=0, max_value=4),
)
def test_connection_budget_is_sum_of_pools(n_dbs, size, overflow, replicas) -> None:
    databases = {}
    for i in range(n_dbs):
        databases[f"db{i}"] = {
            "primary": {"url": "postgresql+psycopg://h/x"},
            "replicas": [
                {"name": f"r{j}", "url": "postgresql+psycopg://h/x"} for j in range(replicas)
            ],
        }
    cfg = DbkitConfig.from_dict(
        {"defaults": {"pool": {"size": size, "max_overflow": overflow}}, "databases": databases}
    )
    per_target = size + overflow
    n_targets = n_dbs * (1 + replicas)
    assert cfg.max_connections_per_process() == per_target * n_targets
    report = cfg.connection_budget_report(replicas=3)
    assert report["cluster_total"] == report["per_process"] * 3
