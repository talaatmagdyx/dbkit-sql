from __future__ import annotations

import pytest
from sqlalchemy import select, text

from dbkit import Query, sql
from dbkit._core.query import QueryRegistry, coerce_statement
from dbkit.errors import DatabaseProgrammingError


def test_sql_wraps_string() -> None:
    clause = sql("SELECT 1")
    assert isinstance(clause, text("x").__class__)


def test_sql_rejects_non_string() -> None:
    with pytest.raises(DatabaseProgrammingError):
        sql(select(1))  # type: ignore[arg-type]


def test_coerce_rejects_bare_string() -> None:
    with pytest.raises(DatabaseProgrammingError, match="wrapped with sql"):
        coerce_statement("SELECT 1")


def test_coerce_accepts_core_and_text() -> None:
    assert coerce_statement(sql("SELECT 1")) is not None
    assert coerce_statement(select(1)) is not None


def test_query_requires_name() -> None:
    with pytest.raises(DatabaseProgrammingError):
        Query(name="", statement=sql("SELECT 1"))


def test_query_is_write() -> None:
    assert Query(name="a", statement=sql("X"), operation="write").is_write
    assert Query(name="b", statement=sql("X"), operation="ddl").is_write
    assert not Query(name="c", statement=sql("X"), operation="read").is_write


def test_query_coerce() -> None:
    q = Query(name="x", statement=sql("SELECT 1"))
    assert coerce_statement(q) is q.statement


def test_registry_dedup() -> None:
    reg = QueryRegistry()
    q = Query(name="a.b", statement=sql("SELECT 1"))
    assert reg.register(q) is q
    assert reg.register(q) is q  # same object OK
    assert reg.get("a.b") is q
    assert reg.names() == ["a.b"]


def test_registry_conflict() -> None:
    reg = QueryRegistry()
    reg.register(Query(name="a.b", statement=sql("SELECT 1")))
    with pytest.raises(DatabaseProgrammingError, match="duplicate"):
        reg.register(Query(name="a.b", statement=sql("SELECT 2")))
