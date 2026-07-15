"""dbkit.integrations.fastapi — category-driven RFC 7807 responses."""

from __future__ import annotations

import pytest

starlette = pytest.importorskip("starlette")

from starlette.applications import Starlette  # noqa: E402
from starlette.routing import Route  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

from dbkit.errors import (  # noqa: E402
    DatabaseError,
    DatabasePoolTimeoutError,
    DatabaseQueryTimeoutError,
    DatabaseSyntaxError,
    ErrorCategory,
)
from dbkit.integrations.fastapi import install_exception_handlers  # noqa: E402


def make_client(error: Exception, **install_kwargs: object) -> TestClient:
    async def boom(request):
        raise error

    app = Starlette(routes=[Route("/q", boom)])
    install_exception_handlers(app, **install_kwargs)
    return TestClient(app, raise_server_exceptions=False)


def test_pool_timeout_is_503_with_retry_after() -> None:
    client = make_client(DatabasePoolTimeoutError("pool exhausted"), retry_after_seconds=5)
    resp = client.get("/q")
    assert resp.status_code == 503
    assert resp.headers["Retry-After"] == "5"
    assert resp.headers["content-type"].startswith("application/problem+json")
    body = resp.json()
    assert body["type"] == "urn:problem:database-overloaded"
    assert body["instance"] == "/q"
    assert "detail" not in body  # expose_detail defaults off


def test_query_timeout_is_504() -> None:
    client = make_client(DatabaseQueryTimeoutError("statement timeout"))
    resp = client.get("/q")
    assert resp.status_code == 504
    assert resp.json()["type"] == "urn:problem:database-timeout"


def test_programming_error_is_500() -> None:
    client = make_client(DatabaseSyntaxError("bad sql"))
    resp = client.get("/q")
    assert resp.status_code == 500
    assert resp.json()["type"] == "urn:problem:database-error"


def test_classification_is_category_driven() -> None:
    error = DatabaseError("future subclass", category=ErrorCategory.POOL)
    client = make_client(error)
    assert client.get("/q").status_code == 503


def test_expose_detail_includes_message() -> None:
    client = make_client(DatabasePoolTimeoutError("pool exhausted"), expose_detail=True)
    assert client.get("/q").json()["detail"] == "pool exhausted"


def test_non_database_errors_untouched() -> None:
    client = make_client(RuntimeError("unrelated"))
    assert client.get("/q").status_code == 500
    # Starlette's default plain-text 500, not our problem+json handler
    assert not client.get("/q").headers["content-type"].startswith("application/problem+json")


def test_retry_after_derived_from_breaker_config() -> None:
    from dbkit import AsyncDatabase

    db = AsyncDatabase.from_config(
        {
            "databases": {},
            "defaults": {"circuit_breaker": {"enabled": True, "open_seconds": 7.5}},
        }
    )
    client = make_client(DatabasePoolTimeoutError("pool exhausted"), database=db)
    assert client.get("/q").headers["Retry-After"] == "8"  # ceil(7.5)


def test_retry_after_default_without_database() -> None:
    client = make_client(DatabasePoolTimeoutError("pool exhausted"))
    assert client.get("/q").headers["Retry-After"] == "2"
