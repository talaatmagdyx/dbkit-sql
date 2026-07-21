"""Unit tests for the transactional-outbox helpers + advisory-lock dialect guard (no database)."""

from __future__ import annotations

import pytest

from dbkit import errors
from dbkit._async.transaction import AsyncTransactionScope
from dbkit.integrations import (
    drain,
    enqueue,
    outbox_ddl,
    outbox_month_partition_ddl,
    partitioned_outbox_ddl,
)
from dbkit.integrations.outbox import DEFAULT_OUTBOX_TABLE


class TestOutboxDdl:
    def test_default_table_and_columns(self) -> None:
        ddl = outbox_ddl()
        assert DEFAULT_OUTBOX_TABLE in ddl
        for col in ("id", "topic", "payload", "created_at", "sent_at"):
            assert col in ddl
        assert "JSONB" in ddl
        assert "WHERE sent_at IS NULL" in ddl  # partial index for the relay scan

    def test_custom_table_name(self) -> None:
        ddl = outbox_ddl("engagement_outbox")
        assert "engagement_outbox" in ddl
        assert "engagement_outbox_unsent_idx" in ddl

    def test_partitioned_keys_on_created_at(self) -> None:
        ddl = partitioned_outbox_ddl()
        assert "PARTITION BY RANGE (created_at)" in ddl
        assert "PRIMARY KEY (id, created_at)" in ddl

    def test_month_partition_bounds(self) -> None:
        ddl = outbox_month_partition_ddl(2026, 7)
        assert "outbox_messages_2026_07" in ddl
        assert "FROM ('2026-07-01') TO ('2026-08-01')" in ddl

    def test_month_partition_december_rolls_to_next_year(self) -> None:
        ddl = outbox_month_partition_ddl(2026, 12)
        assert "FROM ('2026-12-01') TO ('2027-01-01')" in ddl


class TestExports:
    def test_public_helpers_importable(self) -> None:
        # all four outbox symbols are re-exported from dbkit.integrations
        assert callable(enqueue)
        assert callable(drain)
        assert callable(outbox_ddl)
        assert callable(partitioned_outbox_ddl)


class TestAdvisoryLockDialectGuard:
    async def test_advisory_lock_rejects_non_postgres(self) -> None:
        scope = AsyncTransactionScope(
            conn=None,  # type: ignore[arg-type]  # guard trips before conn is touched
            is_postgres=False,
            default_timeout=None,
            database="db",
            shard_id="s",
            role="write",
        )
        with pytest.raises(errors.DatabaseUnsupportedOperationError):
            await scope.advisory_xact_lock("k")
        with pytest.raises(errors.DatabaseUnsupportedOperationError):
            await scope.try_advisory_xact_lock(42)
