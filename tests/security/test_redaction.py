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


def test_config_repr_never_contains_dsn_password() -> None:
    cfg = DbkitConfig.from_dict({"databases": {"app": {"primary": {"url": DSN}}}})
    for obj in (cfg, cfg.databases["app"], cfg.databases["app"].primary):
        assert SECRET not in repr(obj)
        assert SECRET not in str(obj)
    assert cfg.databases["app"].primary.url == DSN  # the real URL is still there for the engine


def test_engine_creation_error_never_contains_dsn_password(monkeypatch: pytest.MonkeyPatch) -> None:
    import dbkit._async.engine as engine_module
    from dbkit.errors import DatabaseConfigurationError

    def boom(url: str, **_: object) -> None:
        raise ValueError(f"cannot parse {url}")  # a driver error that echoes the URL

    monkeypatch.setattr(engine_module, "create_async_engine", boom)
    cfg = DbkitConfig.from_dict({"databases": {"app": {"primary": {"url": DSN}}}})
    registry = engine_module.AsyncEngineRegistry.__new__(engine_module.AsyncEngineRegistry)
    registry._config = cfg  # type: ignore[attr-defined]
    with pytest.raises(DatabaseConfigurationError) as caught:
        registry._build_engine(cfg.databases["app"].primary)
    assert SECRET not in str(caught.value)


@pytest.mark.parametrize(
    "url",
    [
        f"postgresql+psycopg://app@db/app?password={SECRET}",
        f"postgresql+psycopg://app@db/app?sslmode=require&sslpassword={SECRET}",
        f"postgresql+psycopg://app:ab/{SECRET}+ef==@db/app",  # base64 secrets often contain '/'
        f"postgresql+psycopg://:{SECRET}@db/app",  # password-only userinfo
        f"postgresql+psycopg://app:{SECRET.replace('-', '%2F')}@db/app",  # percent-encoded
    ],
)
def test_every_password_form_is_redacted_in_config_and_messages(url: str) -> None:
    from dbkit._core.config import TargetConfig
    from dbkit._core.errors.redaction import sanitize_message

    secret = SECRET.replace("-", "%2F") if "%2F" in url else SECRET
    assert secret not in repr(TargetConfig(url=url))
    assert secret not in sanitize_message(f"could not connect to {url}: timeout")


def test_free_text_password_with_spaces_and_keyword_dsn_are_redacted() -> None:
    from dbkit._core.errors.redaction import sanitize_message

    assert "pa ss" not in sanitize_message("x postgresql://u:pa ss@h/d failed")
    assert SECRET not in sanitize_message(f"host=db user=app password={SECRET} dbname=app")
    assert SECRET not in sanitize_message(f"host=db password='{SECRET} x' dbname=app")


def test_redaction_keeps_host_port_and_database_readable() -> None:
    from dbkit._core.errors.redaction import sanitize_message

    message = "could not connect to postgresql://db.internal:5432/app: connection refused"
    assert sanitize_message(message) == message


def test_engine_creation_error_keeps_no_secret_in_its_chain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import traceback

    from dbkit._async import engine as async_engine
    from dbkit._sync import engine as sync_engine
    from dbkit.errors import DatabaseConfigurationError

    def boom(url: str, **_: object) -> None:
        raise ValueError(f"cannot parse {url}")

    cases = (
        (async_engine, "create_async_engine", async_engine.AsyncEngineRegistry),
        (sync_engine, "create_engine", sync_engine.EngineRegistry),
    )
    for module, factory, registry_cls in cases:
        monkeypatch.setattr(module, factory, boom)
        cfg = DbkitConfig.from_dict({"databases": {"app": {"primary": {"url": DSN}}}})
        registry = registry_cls.__new__(registry_cls)
        registry._config = cfg  # type: ignore[attr-defined]
        with pytest.raises(DatabaseConfigurationError) as caught:
            registry._build_engine(cfg.databases["app"].primary)
        err = caught.value
        assert err.__context__ is None and err.__cause__ is None
        assert SECRET not in "".join(traceback.format_exception(err))


@pytest.mark.parametrize(
    "text",
    [
        f"host=db password=ab#{SECRET} dbname=app",  # '#' and '&' are ordinary in a keyword DSN
        f"host=db password=ab&{SECRET} dbname=app",
        f"postgresql+psycopg://app@db/app?password=ab#{SECRET}",  # SQLAlchemy keeps the '#' part
        f"PGPASSWORD={SECRET} psql",
        f"db_password={SECRET}",
        f"{{'password': '{SECRET}'}}",
        f'{{"password":"{SECRET}"}}',
    ],
)
def test_other_password_spellings_are_redacted(text: str) -> None:
    from dbkit._core.errors.redaction import sanitize_message

    assert SECRET not in sanitize_message(text)


@pytest.mark.parametrize(
    "hostile",
    ["a://:" * 200_000, "a" * 1_000_000, "password='" * 100_000, "x" + "a.b-c" * 200_000 + "://"],
)
def test_redaction_time_stays_bounded_on_hostile_messages(hostile: str) -> None:
    import time

    from dbkit._core.errors.redaction import redact_dsn, sanitize_message

    start = time.perf_counter()
    sanitize_message(hostile)
    redact_dsn(hostile[:20_000])  # config values are not capped; the bounded patterns must hold
    assert time.perf_counter() - start < 1.0


@pytest.mark.parametrize(
    "text",
    [
        f"host=db password=ab,{SECRET} dbname=app",  # ',' and '}' are ordinary in a keyword value
        f"host=db password=ab}}{SECRET} dbname=app",
        f"PGPASSWORD=ab,{SECRET} psql",
        f"https://h/x?db_password={SECRET}",  # any query key containing "password"
        f"https://h/x?a=1&PGPASSWORD={SECRET}",
        f"postgresql+psycopg://app@db/app?%70assword={SECRET}",  # SQLAlchemy decodes the key
        "password=" + "A" * 1100 + SECRET,  # longer than any bound: redacted whole
        "password='" + "A" * 1100 + f" {SECRET}'",
        "connect postgresql://app:" + "A" * 1100 + f"{SECRET}@db/x",
    ],
)
def test_more_password_spellings_and_long_values_are_redacted(text: str) -> None:
    from dbkit._core.errors.redaction import sanitize_message

    assert SECRET not in sanitize_message(text)


def test_a_password_cut_by_the_message_cap_is_still_redacted() -> None:
    from dbkit._core.errors.redaction import _MAX_MESSAGE, sanitize_message

    for pad in range(_MAX_MESSAGE - 40, _MAX_MESSAGE - 10):
        out = sanitize_message("x" * pad + f" postgresql://app:{SECRET}@db/app trailing")
        assert SECRET[:6] not in out and out.endswith("...(truncated)")


def test_ordinary_query_parameters_are_left_alone() -> None:
    from dbkit._core.errors.redaction import sanitize_message

    assert sanitize_message("GET /x?page=2&sort=name") == "GET /x?page=2&sort=name"


@pytest.mark.parametrize(
    "text",
    [
        f"password: {SECRET}",  # unquoted key with ':'
        f'PGPASSWORD="ab {SECRET}" psql',  # double-quoted '=' value with a space
        f'"password"="{SECRET}"',  # quoted key with '='
        f"'password' = '{SECRET}'",
        f"GET /login?next=/cb?password={SECRET}",  # nested query
        f"?a=b?db_password={SECRET}",
        f'Cfg(password="o\'b {SECRET}")',  # repr double-quotes a value containing a quote
        f"line 3: password: {SECRET}",
        f"DB_PASSWORD : {SECRET}",
        f"redirect https://app/cb?next=/login?password={SECRET}",
    ],
)
def test_round4_spellings_are_redacted(text: str) -> None:
    from dbkit._core.errors.redaction import sanitize_message

    assert SECRET not in sanitize_message(text)


def test_text_past_the_cap_never_reaches_the_output() -> None:
    from dbkit._core.errors.redaction import _MAX_MESSAGE, sanitize_message

    # A shrinking redaction early in the message must not pull a later DSN password into view.
    msg = "password=" + "A" * 6000 + " " + "x" * 7328 + f" postgresql://app:{SECRET}@db/app"
    out = sanitize_message(msg)
    assert SECRET not in out and len(out) <= _MAX_MESSAGE + len(" ...(truncated)")


def test_redaction_time_stays_bounded_on_long_scheme_runs() -> None:
    import time

    from dbkit._core.errors.redaction import redact_dsn, sanitize_message

    hostile = ("a" * 32 + "://:") * 50_000
    start = time.perf_counter()
    sanitize_message(hostile)
    redact_dsn(hostile[:20_000])
    assert time.perf_counter() - start < 1.0


@pytest.mark.parametrize(
    "message",
    [
        "could not connect to postgresql://db.internal:5432/app for ops@corp.example",
        "postgresql://db:5432 refused; contact dba@example.com",
        "postgresql://db:5432/app?sslmode=require failed for app@db",
    ],
)
def test_a_password_less_url_stays_readable_before_an_unrelated_at(message: str) -> None:
    from dbkit._core.errors.redaction import sanitize_message

    assert sanitize_message(message) == message


def test_a_numeric_looking_password_is_still_redacted() -> None:
    from dbkit._core.errors.redaction import sanitize_message

    assert "5432x" not in sanitize_message("postgresql://app:5432x@db/app failed")
    assert "1234/abc" not in sanitize_message("postgresql://app:1234/abc@db/app failed")
