from __future__ import annotations

from dbkit._core.errors import classify
from dbkit._core.errors.redaction import redact_dsn, redact_params, sanitize_message
from dbkit._core.errors.sqlstate import error_class_for_sqlstate
from dbkit.errors import (
    DatabaseConnectionError,
    DatabaseDeadlockError,
    DatabaseError,
    DatabaseQueryTimeoutError,
    DatabaseSerializationError,
    DatabaseUniqueViolationError,
)


class FakePsycopgError(Exception):
    def __init__(self, msg: str, sqlstate: str) -> None:
        super().__init__(msg)
        self.sqlstate = sqlstate


def test_sqlstate_table() -> None:
    assert error_class_for_sqlstate("23505") is DatabaseUniqueViolationError
    assert error_class_for_sqlstate("40001") is DatabaseSerializationError
    assert error_class_for_sqlstate("40P01") is DatabaseDeadlockError
    assert error_class_for_sqlstate("57014") is DatabaseQueryTimeoutError
    # unknown exact code falls back to class family
    assert error_class_for_sqlstate("23999") is not None
    assert error_class_for_sqlstate(None) is None


def test_classify_prefers_sqlstate() -> None:
    err = classify(FakePsycopgError("dup", "23505"), query_name="q", database_name="app")
    assert isinstance(err, DatabaseUniqueViolationError)
    assert err.sqlstate == "23505"
    assert err.query_name == "q"
    assert err.database_name == "app"
    assert err.retryable is False  # integrity errors are not retryable


def test_classify_serialization_is_retryable() -> None:
    err = classify(FakePsycopgError("conflict", "40001"))
    assert isinstance(err, DatabaseSerializationError)
    assert err.retryable is True


def test_classify_timeout() -> None:
    err = classify(TimeoutError())
    assert isinstance(err, DatabaseQueryTimeoutError)


def test_classify_os_error() -> None:
    err = classify(ConnectionRefusedError("refused"))
    assert isinstance(err, DatabaseConnectionError)


def test_classify_passthrough() -> None:
    original = DatabaseUniqueViolationError("x")
    assert classify(original, query_name="q") is original
    assert original.query_name == "q"


def test_redact_dsn() -> None:
    out = redact_dsn("postgresql+psycopg://user:supersecret@host:5432/db")
    assert "supersecret" not in out
    assert "user" in out and "host" in out


def test_redact_params() -> None:
    out = redact_params(
        {"id": 1, "password": "x", "access_token": "y", "name": "ok"},
        sensitive={"name"},
    )
    assert out["id"] == 1
    assert out["password"] == "***"
    assert out["access_token"] == "***"
    assert out["name"] == "***"  # explicitly marked sensitive


def test_sanitize_message() -> None:
    msg = "could not connect to postgresql+psycopg://u:pw@h/db"
    assert "pw" not in sanitize_message(msg)


def test_error_to_dict_is_secret_free() -> None:
    err = DatabaseError("boom", sqlstate="08006", database_name="app")
    d = err.to_dict()
    assert d["sqlstate"] == "08006"
    assert d["database"] == "app"
    assert "original" not in d
