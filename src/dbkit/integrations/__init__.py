"""Integration helpers for message-driven workloads (§28) and micro-batching (§17.1)."""

from __future__ import annotations

from .batch import BatchCollector
from .inbox import (
    ack_after_commit,
    inbox_ddl,
    inbox_month_partition_ddl,
    partitioned_inbox_ddl,
    process_once,
)

__all__ = [
    "BatchCollector",
    "ack_after_commit",
    "inbox_ddl",
    "inbox_month_partition_ddl",
    "partitioned_inbox_ddl",
    "process_once",
]
