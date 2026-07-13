"""Routing: targets, resolved routes, and resolver protocols (§21-23).

Phase 1 resolves a named database to its primary. Shard and replica resolution slot in at
:class:`ShardResolver` / :class:`ReplicaSelector` in Phase 4 without changing the public
:class:`DatabaseTarget` surface.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable

from .errors import DatabaseRoutingError

Role = Literal["write", "read", "prefer_replica", "primary_only"]


@dataclass(frozen=True, slots=True)
class DatabaseTarget:
    """Where an operation should run (§8.2, §22.1).

    ``role`` selects primary vs replica. ``shard_key`` (any hashable business value, e.g. an
    organization id) is resolved to a concrete shard by a :class:`ShardResolver` in Phase 4;
    in Phase 1 it is carried through and ignored by the single-shard resolver.
    """

    database: str
    role: Role = "write"
    shard_key: object | None = None

    def __post_init__(self) -> None:
        if not self.database:
            raise DatabaseRoutingError("DatabaseTarget.database must be a non-empty name")

    @property
    def wants_replica(self) -> bool:
        return self.role in ("read", "prefer_replica")

    @property
    def requires_primary(self) -> bool:
        return self.role in ("write", "primary_only")


@dataclass(frozen=True, slots=True)
class ResolvedRoute:
    """The concrete destination an operation was routed to — used for engine keying,
    metrics labels, and trace attributes."""

    database: str
    shard_id: str
    role: Literal["primary", "replica"]
    replica_name: str | None = None


@runtime_checkable
class ShardResolver(Protocol):
    """Resolve a ``(database, shard_key)`` pair to a concrete shard id (§22.2)."""

    def resolve(self, database: str, shard_key: object) -> str: ...


@runtime_checkable
class ReplicaSelector(Protocol):
    """Choose a replica name for a read, or ``None`` to use the primary (§23)."""

    def select(self, database: str, shard_id: str) -> str | None: ...


class SingleShardResolver:
    """Phase 1 resolver: every key maps to the single ``"default"`` shard."""

    def resolve(self, database: str, shard_key: object) -> str:
        return "default"
