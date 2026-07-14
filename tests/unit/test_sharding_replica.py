from __future__ import annotations

import pytest

from dbkit._core.routing import (
    CallableShardResolver,
    DirectoryShardResolver,
    HashShardResolver,
    RangeShardResolver,
    RoundRobinReplicaSelector,
    ShardRange,
    WeightedReplicaSelector,
)
from dbkit.errors import DatabaseConfigurationError, DatabaseRoutingError

# --- HashShardResolver --------------------------------------------------------------- #


def test_hash_resolver_is_deterministic_across_instances() -> None:
    a = HashShardResolver(8)
    b = HashShardResolver(8)
    assert a.resolve("app", "tenant-42") == b.resolve("app", "tenant-42")


def test_hash_resolver_distributes_across_shards() -> None:
    resolver = HashShardResolver(4)
    seen = {resolver.resolve("app", f"tenant-{i}") for i in range(200)}
    assert len(seen) > 1  # not all landing on the same shard


def test_hash_resolver_uses_prefix() -> None:
    resolver = HashShardResolver(2, prefix="db")
    assert resolver.resolve("app", "x").startswith("db-")


def test_hash_resolver_requires_key() -> None:
    with pytest.raises(DatabaseRoutingError):
        HashShardResolver(4).resolve("app", None)


def test_hash_resolver_rejects_invalid_num_shards() -> None:
    with pytest.raises(DatabaseConfigurationError):
        HashShardResolver(0)


def test_hash_resolver_cache_returns_consistent_results() -> None:
    """Performance review §10: repeated resolves for the same key must not recompute SHA-256
    every call, but the cached result must be identical to a fresh (uncached) computation."""
    resolver = HashShardResolver(4)
    first = resolver.resolve("app", "tenant-1")
    second = resolver.resolve("app", "tenant-1")  # served from cache
    assert first == second
    assert resolver._cache["tenant-1"] == first


def test_hash_resolver_cache_is_bounded() -> None:
    """The cache must not grow unboundedly for a high-cardinality key space (mirrors the
    engine registry's own max_engines/evict_lru bound)."""
    resolver = HashShardResolver(4)
    resolver._CACHE_SIZE = 100  # shrink for a fast test
    for i in range(500):
        resolver.resolve("app", f"tenant-{i}")
    assert len(resolver._cache) <= 100


def test_hash_resolver_cache_eviction_keeps_correctness() -> None:
    """Evicting a cache entry must not change the result on the next resolve for that key."""
    resolver = HashShardResolver(4)
    resolver._CACHE_SIZE = 10
    baseline = {f"tenant-{i}": resolver.resolve("app", f"tenant-{i}") for i in range(3)}
    for i in range(3, 100):  # evict everything from the baseline out of the cache
        resolver.resolve("app", f"tenant-{i}")
    for key, expected in baseline.items():
        assert resolver.resolve("app", key) == expected


# --- RangeShardResolver -------------------------------------------------------------- #


def test_range_resolver_picks_correct_bucket() -> None:
    resolver = RangeShardResolver(
        [ShardRange(100, "shard-a"), ShardRange(200, "shard-b"), ShardRange(300, "shard-c")]
    )
    assert resolver.resolve("app", 50) == "shard-a"
    assert resolver.resolve("app", 150) == "shard-b"
    assert resolver.resolve("app", 250) == "shard-c"


def test_range_resolver_out_of_range_fails_closed() -> None:
    resolver = RangeShardResolver([ShardRange(100, "shard-a")])
    with pytest.raises(DatabaseRoutingError):
        resolver.resolve("app", 999)


def test_range_resolver_requires_at_least_one_range() -> None:
    with pytest.raises(DatabaseConfigurationError):
        RangeShardResolver([])


def test_range_resolver_requires_key() -> None:
    resolver = RangeShardResolver([ShardRange(100, "shard-a")])
    with pytest.raises(DatabaseRoutingError):
        resolver.resolve("app", None)


# --- DirectoryShardResolver ----------------------------------------------------------- #


def test_directory_resolver_lookup() -> None:
    resolver = DirectoryShardResolver({"tenant-a": "shard-1", "tenant-b": "shard-2"})
    assert resolver.resolve("app", "tenant-a") == "shard-1"


def test_directory_resolver_missing_key_fails_closed() -> None:
    resolver = DirectoryShardResolver({})
    with pytest.raises(DatabaseRoutingError, match="no shard mapping"):
        resolver.resolve("app", "unknown-tenant")


def test_directory_resolver_set_mapping() -> None:
    resolver = DirectoryShardResolver({})
    resolver.set_mapping("tenant-a", "shard-9")
    assert resolver.resolve("app", "tenant-a") == "shard-9"


# --- CallableShardResolver ------------------------------------------------------------ #


def test_callable_resolver_wraps_function() -> None:
    resolver = CallableShardResolver(lambda db, key: f"{db}-{key}")
    assert resolver.resolve("app", "x") == "app-x"


# --- RoundRobinReplicaSelector --------------------------------------------------------- #


def test_round_robin_cycles_through_replicas() -> None:
    selector = RoundRobinReplicaSelector({"app": ["r1", "r2", "r3"]})
    picks = [selector.select("app", "default") for _ in range(6)]
    assert picks == ["r1", "r2", "r3", "r1", "r2", "r3"]


def test_round_robin_no_replicas_returns_none() -> None:
    selector = RoundRobinReplicaSelector({})
    assert selector.select("app", "default") is None


def test_round_robin_unknown_database_returns_none() -> None:
    selector = RoundRobinReplicaSelector({"app": ["r1"]})
    assert selector.select("other", "default") is None


# --- WeightedReplicaSelector ----------------------------------------------------------- #


def test_weighted_selector_respects_weights_with_injected_rand() -> None:
    selector = WeightedReplicaSelector({"app": [("r1", 1), ("r2", 3)]}, rand=lambda: 0.0)
    assert selector.select("app", "default") == "r1"  # r < 1 picks the first bucket

    selector2 = WeightedReplicaSelector({"app": [("r1", 1), ("r2", 3)]}, rand=lambda: 0.99)
    assert selector2.select("app", "default") == "r2"  # 0.99*4=3.96 falls in r2's bucket


def test_weighted_selector_no_replicas_returns_none() -> None:
    selector = WeightedReplicaSelector({})
    assert selector.select("app", "default") is None


def test_weighted_selector_zero_weight_still_gets_min_one_slot() -> None:
    selector = WeightedReplicaSelector({"app": [("r1", 0), ("r2", 0)]}, rand=lambda: 0.0)
    # both weights clamp to >=1 so the total is never zero (no division by zero)
    assert selector.select("app", "default") in ("r1", "r2")
