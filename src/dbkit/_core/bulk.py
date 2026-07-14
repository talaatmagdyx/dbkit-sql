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
    """The three independent caps a bulk batch is sized against (§19.1)."""

    max_rows: int = 1000
    max_params: int = PG_MAX_BIND_PARAMS
    max_payload_bytes: int | None = None


def estimate_row_bytes(row: Mapping[str, Any]) -> int:
    """Cheap upper-bound estimate of one row's serialized payload size, in bytes.

    Not exact (actual bind-parameter wire encoding varies by driver/type), but good enough to
    bound a batch's total payload size to roughly ``max_payload_bytes`` rather than not bounding
    it at all — the field existed on :class:`BulkLimits`/``BulkConfig`` but ``resolve_batch_rows``
    never read it, so a batch of many small-column, wide-value (e.g. large ``text``/``bytea``)
    rows had no actual byte-size ceiling despite the config implying one (performance review §9).
    """
    total = 0
    for value in row.values():
        if value is None:
            continue
        if isinstance(value, (bytes, bytearray)):
            total += len(value)
        else:
            total += len(str(value).encode("utf-8", errors="ignore"))
    return total


def resolve_batch_rows(
    n_columns: int,
    requested: int | None,
    limits: BulkLimits,
    *,
    sample_row: Mapping[str, Any] | None = None,
) -> int:
    """Largest safe batch size in rows, given per-row column count and the limits.

    When ``limits.max_payload_bytes`` is set and a ``sample_row`` is supplied, the ceiling is
    also bounded by an estimated per-row byte size (:func:`estimate_row_bytes`) — a single
    representative row is enough since batches are homogeneous (same columns, similar value
    sizes), and re-estimating every row would cost more than the batching itself saves.
    """
    if n_columns <= 0:
        return max(requested or limits.max_rows, 1)
    by_params = max(limits.max_params // n_columns, 1)
    ceiling = min(requested or limits.max_rows, limits.max_rows, by_params)
    if limits.max_payload_bytes is not None and sample_row is not None:
        row_bytes = estimate_row_bytes(sample_row)
        if row_bytes > 0:
            by_bytes = max(limits.max_payload_bytes // row_bytes, 1)
            ceiling = min(ceiling, by_bytes)
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
