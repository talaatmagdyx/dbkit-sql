"""A cancellation during COMMIT propagates; it is never swallowed or reclassified."""

from __future__ import annotations

import asyncio

import pytest

from dbkit._async.transaction import _AsyncTransactionManager


class _Trans:
    def __init__(self, exc: BaseException) -> None:
        self.exc = exc
        self.rolled_back = False

    async def commit(self) -> None:
        raise self.exc

    async def rollback(self) -> None:
        self.rolled_back = True


def _tx(trans: _Trans) -> _AsyncTransactionManager:
    tx = _AsyncTransactionManager.__new__(_AsyncTransactionManager)
    tx._trans = trans  # type: ignore[assignment]
    tx._database, tx._shard_id, tx._role, tx._query_name = "db", "s", "write", None
    return tx


async def test_cancellation_during_commit_is_reraised_without_a_rollback() -> None:
    trans = _Trans(asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await _tx(trans)._commit()
    assert (
        not trans.rolled_back
    )  # the COMMIT may have landed; a rollback on that connection proves nothing
