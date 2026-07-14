from __future__ import annotations

import asyncio

import pytest

from dbkit._core.bulk import (
    PG_MAX_BIND_PARAMS,
    BulkLimits,
    column_names,
    iter_batches,
    resolve_batch_rows,
)
from dbkit.integrations import BatchCollector


def test_resolve_batch_rows_respects_param_ceiling() -> None:
    # 100 columns -> at most 65535 // 100 = 655 rows even if more requested
    assert resolve_batch_rows(100, 5000, BulkLimits(max_rows=5000)) == PG_MAX_BIND_PARAMS // 100


def test_resolve_batch_rows_respects_requested_and_max() -> None:
    assert resolve_batch_rows(2, 500, BulkLimits(max_rows=1000)) == 500
    assert resolve_batch_rows(2, 5000, BulkLimits(max_rows=1000)) == 1000  # capped by max_rows


def test_resolve_batch_rows_never_zero() -> None:
    assert resolve_batch_rows(70000, 1000, BulkLimits()) >= 1


def test_iter_batches() -> None:
    assert [list(b) for b in iter_batches(list(range(5)), 2)] == [[0, 1], [2, 3], [4]]
    assert list(iter_batches([], 10)) == []


def test_column_names() -> None:
    assert column_names([{"a": 1, "b": 2}, {"a": 3, "b": 4}]) == ["a", "b"]
    assert column_names([]) == []


async def test_batch_collector_flushes_on_size() -> None:
    flushed: list[list[int]] = []

    async def flush(items):
        flushed.append(list(items))

    bc = BatchCollector(flush, max_size=3, max_delay_ms=10_000)
    for i in range(7):
        await bc.add(i)
    await bc.close()
    # 3 + 3 on size, 1 on close
    assert flushed == [[0, 1, 2], [3, 4, 5], [6]]


async def test_batch_collector_flushes_on_timer() -> None:
    flushed: list[list[int]] = []

    async def flush(items):
        flushed.append(list(items))

    bc = BatchCollector(flush, max_size=100, max_delay_ms=20)
    await bc.add(1)
    await bc.add(2)
    await asyncio.sleep(0.1)  # let the timer fire
    assert flushed == [[1, 2]]
    await bc.close()


async def test_batch_collector_rejects_after_close() -> None:
    async def flush(items):
        pass

    bc = BatchCollector(flush, max_size=10)
    await bc.close()
    with pytest.raises(RuntimeError):
        await bc.add(1)
