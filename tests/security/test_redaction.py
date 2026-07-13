"""Security: secrets never leak into messages, logs, traces, or metrics (§13.4, §29).

Black-box tests using only public APIs. No database required.
"""

from __future__ import annotations

import logging

import pytest

from dbkit import DbkitConfig, Query, sql
from dbkit._core.errors import classify
from dbkit._core.errors.redaction import redact_params
from dbkit.errors import DatabaseProgrammingError
from dbkit.observability.logging import redacted_params_for_log

SECRET = "hunter2-super-secret"
DSN = f"postgresql+psycopg://app:{SECRET}@db.internal:5432/app"


def test_bare_string_sql_is_rejected() -> None:
    """The single biggest injection footgun: passing an un-parameterized f-string."""
    from dbkit._core.query import coerce_statement

    user_input = "1; DROP TABLE users"
    with pytest.raises(DatabaseProgrammingError, match="wrapped with sql"):
        coerce_statement(f"SELECT * FROM users WHERE id = {user_input}")


def test_config_redacted_hides_dsn_password() -> None:
    cfg = DbkitConfig.from_dict({"databases": {"app": {"primary": {"url": DSN}}}})
    red = cfg.redacted()
    assert SECRET not in red.databases["app"].primary.url
    # ... and it is never present in any string representation of the redacted config
    assert SECRET not in repr(red)


def test_error_message_never_contains_dsn_password() -> None:
    class _DriverError(Exception):
        def __init__(self) -> None:
            super().__init__(f'could not connect to "{DSN}"')

    err = classify(_DriverError(), query_name="q", database_name="app")
    assert SECRET not in str(err)
    assert SECRET not in repr(err.to_dict())


def test_sensitive_params_masked_by_name_and_hint() -> None:
    params = {"user_id": 42, "password": SECRET, "api_key": "abc", "note": "public"}
    out = redact_params(params, sensitive={"note"})
    assert out["user_id"] == 42
    assert out["password"] == "***"
    assert out["api_key"] == "***"
    assert out["note"] == "***"  # explicitly declared sensitive
    assert SECRET not in repr(out)


def test_query_sensitive_parameters_are_redacted() -> None:
    q = Query(
        name="auth.login",
        statement=sql("SELECT 1 WHERE token = :access_token"),
        sensitive_parameters={"access_token"},
    )
    out = redact_params({"access_token": SECRET}, sensitive=set(q.sensitive_parameters))
    assert out["access_token"] == "***"


def test_parameter_logging_disabled_by_default() -> None:
    # With log_parameters=False (the default), nothing is returned for logging at all.
    assert (
        redacted_params_for_log({"password": SECRET}, log_parameters=False, sensitive=None) is None
    )
    # When explicitly enabled, secrets are still masked.
    out = redacted_params_for_log({"password": SECRET}, log_parameters=True, sensitive=None)
    assert out == {"password": "***"}


def test_log_event_payload_has_no_secrets(caplog: pytest.LogCaptureFixture) -> None:
    from dbkit.observability.logging import log_event

    with caplog.at_level(logging.INFO, logger="dbkit"):
        log_event(logging.INFO, "database.query.completed", query_name="auth.login", rows=1)
    # query *name* is fine to log; there is no parameter/DSN content in the event.
    assert SECRET not in caplog.text
