"""Pure batch-sizing logic for bulk operations (§19.1).

Sizing is bounded by three limits so a single batch can't blow up memory or hit PostgreSQL's
65535 bind-parameter ceiling: max rows, max total bind parameters, and (optionally) an
estimated payload-byte budget. This module is I/O-free and shared by both frontends.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

# PostgreSQL caps a single statement at 65535 bind parameters.
PG_MAX_BIND_PARAMS = 65535

FailureMode = Literal["atomic", "best_effort", "split_on_failure"]
#: "execute_many" binds N params/row (SQLAlchemy's own multi-row translation, any dialect).
#: "unnest" binds one array param per column regardless of row count (PostgreSQL only, §19).
InsertStrategy = Literal["execute_many", "unnest"]


@dataclass(frozen=True, slots=True)
class BulkLimits:
    max_rows: int = 1000
    max_params: int = PG_MAX_BIND_PARAMS
    max_payload_bytes: int | None = None


def resolve_batch_rows(
    n_columns: int,
    requested: int | None,
    limits: BulkLimits,
) -> int:
    """Largest safe batch size in rows, given per-row column count and the limits."""
    if n_columns <= 0:
        return max(requested or limits.max_rows, 1)
    by_params = max(limits.max_params // n_columns, 1)
    ceiling = min(requested or limits.max_rows, limits.max_rows, by_params)
    return max(ceiling, 1)


def iter_batches(rows: Sequence[Any], batch_size: int) -> Iterator[Sequence[Any]]:
    """Yield contiguous ``batch_size`` slices of ``rows``."""
    if batch_size <= 0:
        batch_size = 1
    for start in range(0, len(rows), batch_size):
        yield rows[start : start + batch_size]


def column_names(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    """Ordered column names inferred from the first row; every row must share them."""
    if not rows:
        return []
    first = rows[0]
    return list(first.keys())
