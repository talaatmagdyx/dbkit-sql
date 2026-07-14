from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypedDict

import pytest

from dbkit._core.result import build_mapper
from dbkit.errors import DatabaseMappingError


class FakeRow(dict):
    """Stand-in for SQLAlchemy RowMapping in pure-unit tests (it is a Mapping)."""

    def values(self):  # type: ignore[override]
        return list(super().values())


def row(**kw: Any) -> FakeRow:
    return FakeRow(kw)


def test_identity_mapper() -> None:
    r = row(id=1)
    assert build_mapper(None)(r) is r


def test_dict_mapper() -> None:
    r = row(id=1, name="a")
    out = build_mapper(dict)(r)
    assert out == {"id": 1, "name": "a"} and type(out) is dict


def test_dataclass_mapper() -> None:
    @dataclass
    class User:
        id: int
        name: str

    out = build_mapper(User)(row(id=1, name="a", extra="ignored"))
    assert out == User(id=1, name="a")


def test_dataclass_mapper_missing_field_errors() -> None:
    @dataclass
    class User:
        id: int
        name: str

    with pytest.raises(DatabaseMappingError):
        build_mapper(User)(row(id=1))  # missing name


def test_typeddict_mapper() -> None:
    class UserTD(TypedDict):
        id: int
        name: str

    out = build_mapper(UserTD)(row(id=1, name="a"))
    assert out == {"id": 1, "name": "a"}


def test_callable_mapper() -> None:
    out = build_mapper(lambda r: r["id"] * 2)(row(id=21))
    assert out == 42


def test_callable_mapper_error_wrapped() -> None:
    def boom(_r: Any) -> Any:
        raise ValueError("nope")

    with pytest.raises(DatabaseMappingError, match="custom mapper raised"):
        build_mapper(boom)(row(id=1))


def test_pydantic_mapper() -> None:
    pydantic = pytest.importorskip("pydantic")

    class User(pydantic.BaseModel):
        id: int
        name: str

    out = build_mapper(User)(row(id=1, name="a"))
    assert out.id == 1 and out.name == "a"
