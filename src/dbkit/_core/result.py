"""Typed results and row mappers (§8.3, §8.4).

Cardinality enforcement (exactly-one / at-most-one / scalar) is *not* reimplemented here — the
async/sync facades call SQLAlchemy's own native ``Result.one()`` / ``.one_or_none()`` /
``.scalar_one()`` / ``.scalars().all()`` directly (see ``_async/connection.py``), which already
provide these exact semantics. This module only covers what SQLAlchemy has no opinion on:
mapping a row to an application type (dataclass, TypedDict, pydantic model, callable).
"""

from __future__ import annotations

import dataclasses
import typing
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeVar

from sqlalchemy import RowMapping

from .errors import DatabaseMappingError

T = TypeVar("T")

#: A caller-supplied mapper receives one row mapping and returns a mapped object.
RowMapper = Callable[[RowMapping], Any]


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    """Standard result returned by write methods (§8.3)."""

    row_count: int
    query_name: str
    database_name: str
    duration_ms: float
    inserted_primary_key: object | None = None
    returned_rows: Sequence[RowMapping] = field(default_factory=tuple)
    shard_id: str | None = None
    role: str | None = None
    retry_attempt: int = 0


def _is_typed_dict(obj: object) -> bool:
    return isinstance(obj, type) and typing.is_typeddict(obj)


def build_mapper(map_to: Any) -> RowMapper:
    """Return a function mapping a ``RowMapping`` to the requested type (§8.4).

    Supported ``map_to`` values:

    * ``None`` or ``RowMapping`` — identity (zero-copy default).
    * ``dict`` — a plain dict copy.
    * a dataclass type — constructed from matching column names.
    * a ``TypedDict`` type — a dict with the row's contents.
    * a pydantic model (has ``model_validate``) — validated from the row.
    * any other callable — invoked as ``mapper(row)``.
    """
    if map_to is None or map_to is RowMapping:
        return lambda row: row

    if map_to is dict:
        return lambda row: dict(row)

    if dataclasses.is_dataclass(map_to) and isinstance(map_to, type):
        field_names = {f.name for f in dataclasses.fields(map_to)}

        def _map_dataclass(row: RowMapping) -> Any:
            try:
                return map_to(**{k: row[k] for k in row if k in field_names})
            except Exception as exc:
                raise DatabaseMappingError(
                    f"failed to map row to {map_to.__name__}: {exc}", original=exc
                ) from exc

        return _map_dataclass

    if _is_typed_dict(map_to):
        return lambda row: dict(row)

    if isinstance(map_to, type) and hasattr(map_to, "model_validate"):

        def _map_pydantic(row: RowMapping) -> Any:
            try:
                return map_to.model_validate(dict(row))
            except Exception as exc:
                raise DatabaseMappingError(
                    f"failed to validate row into {map_to.__name__}: {exc}", original=exc
                ) from exc

        return _map_pydantic

    if callable(map_to):

        def _map_callable(row: RowMapping) -> Any:
            try:
                return map_to(row)
            except Exception as exc:
                raise DatabaseMappingError(f"custom mapper raised: {exc}", original=exc) from exc

        return _map_callable

    raise DatabaseMappingError(f"unsupported map_to value: {map_to!r}")


def map_rows(rows: Sequence[RowMapping], map_to: Any) -> list[Any]:
    mapper = build_mapper(map_to)
    return [mapper(r) for r in rows]
