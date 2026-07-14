"""Routing: targets, resolved routes, and resolver protocols (§21-23).

Shard and replica resolution implement :class:`ShardResolver` / :class:`ReplicaSelector`
without changing the public :class:`DatabaseTarget` surface (§22, §23).
"""

from __future__ import annotations

import hashlib
import random as _random
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable

from .errors import DatabaseConfigurationError, DatabaseRoutingError

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
    """Default resolver for single-shard deployments: every key maps to ``"default"``."""

    def resolve(self, database: str, shard_key: object) -> str:
        return "default"


class HashShardResolver:
    """Hash-based sharding (§22.2): ``shard_key`` -> ``{prefix}-NNN``.

    Uses SHA-256 rather than the builtin ``hash()`` — Python randomizes ``hash()`` for
    str/bytes per process (``PYTHONHASHSEED``), which would route the same key to different
    shards across restarts or app instances. Write routing must be deterministic (§22.3).
    """

    def __init__(self, num_shards: int, *, prefix: str = "shard") -> None:
        if num_shards < 1:
            raise DatabaseConfigurationError("num_shards must be >= 1")
        self.num_shards = num_shards
        self.prefix = prefix

    def resolve(self, database: str, shard_key: object) -> str:
        if shard_key is None:
            raise DatabaseRoutingError(
                f"hash sharding requires a shard_key for database {database!r}"
            )
        digest = hashlib.sha256(str(shard_key).encode("utf-8")).digest()
        bucket = int.from_bytes(digest[:8], "big") % self.num_shards
        return f"{self.prefix}-{bucket:03d}"


@dataclass(frozen=True, slots=True)
class ShardRange:
    """A range boundary: keys ``< upper_bound`` (and >= the previous range's bound) map here."""

    upper_bound: int
    shard_id: str


class RangeShardResolver:
    """Range-based sharding (§22.2): an ordered set of upper bounds, e.g. by tenant-id range."""

    def __init__(self, ranges: Sequence[ShardRange]) -> None:
        if not ranges:
            raise DatabaseConfigurationError("RangeShardResolver requires at least one range")
        self._ranges = sorted(ranges, key=lambda r: r.upper_bound)

    def resolve(self, database: str, shard_key: object) -> str:
        if shard_key is None:
            raise DatabaseRoutingError(
                f"range sharding requires a shard_key for database {database!r}"
            )
        for r in self._ranges:
            if shard_key < r.upper_bound:  # type: ignore[operator]
                return r.shard_id
        raise DatabaseRoutingError(
            f"shard_key {shard_key!r} exceeds the highest configured range "
            f"for database {database!r}"
        )


class DirectoryShardResolver:
    """Explicit ``shard_key -> shard_id`` directory lookup (§22.2).

    Missing mappings fail closed (§22.3) rather than silently defaulting to some shard.
    """

    def __init__(self, directory: Mapping[object, str]) -> None:
        self._directory: dict[object, str] = dict(directory)

    def resolve(self, database: str, shard_key: object) -> str:
        try:
            return self._directory[shard_key]
        except KeyError:
            raise DatabaseRoutingError(
                f"no shard mapping for key {shard_key!r} in database {database!r}"
            ) from None

    def set_mapping(self, shard_key: object, shard_id: str) -> None:
        """Update the directory, e.g. after an explicit tenant-migration workflow (§22.3)."""
        self._directory[shard_key] = shard_id


class CallableShardResolver:
    """Adapts a plain ``fn(database, shard_key) -> shard_id`` callback to :class:`ShardResolver`."""

    def __init__(self, fn: Callable[[str, object], str]) -> None:
        self._fn = fn

    def resolve(self, database: str, shard_key: object) -> str:
        return self._fn(database, shard_key)


class RoundRobinReplicaSelector:
    """Cycles through a database's configured replica names in order (§23).

    Constructed with an explicit ``{database: [replica_name, ...]}`` mapping rather than a
    config object, so it stays decoupled and independently testable.
    """

    def __init__(self, replicas: Mapping[str, Sequence[str]]) -> None:
        self._names: dict[str, list[str]] = {db: list(names) for db, names in replicas.items()}
        self._counters: dict[str, int] = dict.fromkeys(self._names, 0)

    def select(self, database: str, shard_id: str) -> str | None:
        names = self._names.get(database) or []
        if not names:
            return None
        idx = self._counters.get(database, 0) % len(names)
        self._counters[database] = self._counters.get(database, 0) + 1
        return names[idx]


class WeightedReplicaSelector:
    """Weighted-random replica selection using each replica's configured weight (§23)."""

    def __init__(
        self,
        replicas: Mapping[str, Sequence[tuple[str, int]]],
        *,
        rand: Callable[[], float] | None = None,
    ) -> None:
        self._rand = rand or _random.random
        self._replicas: dict[str, list[tuple[str, int]]] = {
            db: list(specs) for db, specs in replicas.items()
        }

    def select(self, database: str, shard_id: str) -> str | None:
        specs = self._replicas.get(database) or []
        if not specs:
            return None
        total = sum(max(w, 1) for _, w in specs)
        r = self._rand() * total
        upto = 0.0
        for name, weight in specs:
            upto += max(weight, 1)
            if r < upto:
                return name
        return specs[-1][0]
