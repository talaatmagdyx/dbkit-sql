"""Dynamic database registration (§22.4) — register/replace/unregister at runtime.

Engines are lazy (no network I/O until a query runs), so these tests exercise the
full registration path against fake DSNs without a live server.
"""

from __future__ import annotations

import asyncio

import pytest

from dbkit import AsyncDatabase, DatabaseConfig, DatabaseTarget
from dbkit._core.routing import ResolvedRoute, RoundRobinReplicaSelector, WeightedReplicaSelector
from dbkit.errors import DatabaseConfigurationError, DatabaseRoutingError

BASE_CONFIG = {
    "databases": {"app": {"primary": {"url": "postgresql+psycopg://h/app"}}},
}


def shard_config(name: str) -> dict:
    return {"primary": {"url": f"postgresql+psycopg://h/{name}", "required": False}}


# --- register ------------------------------------------------------------------------ #


async def test_register_makes_database_routable() -> None:
    db = AsyncDatabase.from_config(BASE_CONFIG)
    with pytest.raises(DatabaseRoutingError):
        db._resolve(DatabaseTarget(database="shard-7"))

    replaced = await db.register_database("shard-7", shard_config("shard7"))
    assert replaced is False
    route = db._resolve(DatabaseTarget(database="shard-7"))
    assert route.database == "shard-7"
    assert route.role == "primary"


async def test_register_accepts_database_config_instance() -> None:
    db = AsyncDatabase.from_config(BASE_CONFIG)
    cfg = DatabaseConfig.from_dict(shard_config("shard8"), name="shard-8")
    await db.register_database("shard-8", cfg)
    assert "shard-8" in db.config.databases


async def test_register_validates_config() -> None:
    db = AsyncDatabase.from_config(BASE_CONFIG)
    with pytest.raises(DatabaseConfigurationError):
        await db.register_database("bad", {})  # no primary target


async def test_register_after_start_creates_required_engine() -> None:
    db = AsyncDatabase.from_config(BASE_CONFIG)
    await db.start()
    engines_before = db.engine_count
    await db.register_database(
        "shard-9", {"primary": {"url": "postgresql+psycopg://h/s9", "required": True}}
    )
    assert db.engine_count == engines_before + 1
    await db.close()


async def test_register_not_required_stays_lazy() -> None:
    db = AsyncDatabase.from_config(BASE_CONFIG)
    await db.start()
    engines_before = db.engine_count
    await db.register_database("shard-10", shard_config("s10"))
    assert db.engine_count == engines_before  # engine created on first use
    await db.close()


async def test_concurrent_registration_is_single_flight() -> None:
    db = AsyncDatabase.from_config(BASE_CONFIG)
    await asyncio.gather(*[db.register_database("shard-x", shard_config("sx")) for _ in range(10)])
    assert "shard-x" in db.config.databases
    assert len([n for n in db.config.databases if n == "shard-x"]) == 1


# --- replace ------------------------------------------------------------------------- #


async def test_replace_disposes_old_engines_and_swaps_config() -> None:
    db = AsyncDatabase.from_config(BASE_CONFIG)
    await db.start()
    await db.register_database(
        "shard-r", {"primary": {"url": "postgresql+psycopg://h/old", "required": True}}
    )
    assert db.engine_count == 2
    replaced = await db.register_database(
        "shard-r", {"primary": {"url": "postgresql+psycopg://h/new", "required": True}}
    )
    assert replaced is True
    assert db.engine_count == 2  # old engine disposed, new one created
    assert "new" in db.config.databases["shard-r"].primary.url
    await db.close()


async def test_replace_resets_limiter() -> None:
    db = AsyncDatabase.from_config(BASE_CONFIG)
    await db.register_database("shard-l", dict(shard_config("sl"), concurrency={"reads": 5}))
    limiter_before = db._executor.limiter_for("shard-l")
    await db.register_database("shard-l", dict(shard_config("sl"), concurrency={"reads": 9}))
    limiter_after = db._executor.limiter_for("shard-l")
    assert limiter_after is not limiter_before


# --- unregister ---------------------------------------------------------------------- #


async def test_unregister_removes_database_and_engines() -> None:
    db = AsyncDatabase.from_config(BASE_CONFIG)
    await db.start()
    await db.register_database(
        "shard-u", {"primary": {"url": "postgresql+psycopg://h/su", "required": True}}
    )
    assert db.engine_count == 2
    assert await db.unregister_database("shard-u") is True
    assert db.engine_count == 1
    with pytest.raises(DatabaseRoutingError):
        db._resolve(DatabaseTarget(database="shard-u"))
    await db.close()


async def test_unregister_unknown_returns_false() -> None:
    db = AsyncDatabase.from_config(BASE_CONFIG)
    assert await db.unregister_database("nope") is False


# --- replica selector updates ---------------------------------------------------------- #


async def test_register_with_replicas_updates_selector() -> None:
    db = AsyncDatabase.from_config(BASE_CONFIG)
    await db.register_database(
        "shard-rep",
        {
            "primary": {"url": "postgresql+psycopg://h/p", "required": False},
            "replicas": [
                {"name": "r1", "url": "postgresql+psycopg://h/r1"},
                {"name": "r2", "url": "postgresql+psycopg://h/r2"},
            ],
        },
    )
    route = db._resolve(DatabaseTarget(database="shard-rep", role="read"))
    assert route.role == "replica"
    assert route.replica_name in {"r1", "r2"}

    await db.unregister_database("shard-rep")


def test_round_robin_set_replicas_rotation() -> None:
    selector = RoundRobinReplicaSelector({})
    selector.set_replicas("db", ["a", "b"])
    assert [selector.select("db", "default") for _ in range(3)] == ["a", "b", "a"]
    selector.set_replicas("db", [])
    assert selector.select("db", "default") is None


def test_weighted_set_replicas() -> None:
    selector = WeightedReplicaSelector({}, rand=lambda: 0.0)
    selector.set_replicas("db", [("a", 1), ("b", 1)])
    assert selector.select("db", "default") == "a"
    selector.set_replicas("db", [])
    assert selector.select("db", "default") is None


# --- engine registry ------------------------------------------------------------------- #


async def test_dispose_database_only_touches_matching_engines() -> None:
    db = AsyncDatabase.from_config(
        {
            "databases": {
                "a": {"primary": {"url": "postgresql+psycopg://h/a"}},
                "b": {"primary": {"url": "postgresql+psycopg://h/b"}},
            }
        }
    )
    await db.start()
    assert db.engine_count == 2
    dropped = await db._registry.dispose_database("a")
    assert dropped == 1
    assert db.engine_count == 1
    await db.close()


async def test_registered_database_usable_via_resolved_route() -> None:
    # End-to-end short of the network: engine builds for a registered database.
    db = AsyncDatabase.from_config(BASE_CONFIG)
    await db.register_database("shard-e", shard_config("se"))
    entry = await db._registry.get(
        ResolvedRoute(database="shard-e", shard_id="default", role="primary")
    )
    assert entry.key.database == "shard-e"
    await db.close()


# --- database_scope context manager ---------------------------------------------------- #


async def test_database_scope_registers_and_cleans_up() -> None:
    db = AsyncDatabase.from_config(BASE_CONFIG)
    await db.start()
    async with db.database_scope("shard-cm", shard_config("scm")) as target:
        assert target.database == "shard-cm"
        assert "shard-cm" in db.config.databases
        route = db._resolve(target)
        assert route.role == "primary"
    assert "shard-cm" not in db.config.databases
    with pytest.raises(DatabaseRoutingError):
        db._resolve(DatabaseTarget(database="shard-cm"))
    await db.close()


async def test_database_scope_cleans_up_on_error() -> None:
    db = AsyncDatabase.from_config(BASE_CONFIG)
    with pytest.raises(RuntimeError, match="boom"):
        async with db.database_scope("shard-err", shard_config("serr")):
            raise RuntimeError("boom")
    assert "shard-err" not in db.config.databases


async def test_database_scope_disposes_engines_on_exit() -> None:
    db = AsyncDatabase.from_config(BASE_CONFIG)
    await db.start()
    engines_before = db.engine_count
    async with db.database_scope(
        "shard-eng", {"primary": {"url": "postgresql+psycopg://h/seng", "required": True}}
    ):
        assert db.engine_count == engines_before + 1
    assert db.engine_count == engines_before  # pool released with the scope
    await db.close()


# --- ensure_database (idempotent) ------------------------------------------------------ #


async def test_ensure_database_registers_once() -> None:
    db = AsyncDatabase.from_config(BASE_CONFIG)
    assert await db.ensure_database("shard-i", shard_config("si")) is True
    assert await db.ensure_database("shard-i", shard_config("si")) is False  # unchanged: no-op
    assert "shard-i" in db.config.databases


async def test_ensure_database_reregisters_on_config_change() -> None:
    db = AsyncDatabase.from_config(BASE_CONFIG)
    await db.start()
    await db.ensure_database(
        "shard-c", {"primary": {"url": "postgresql+psycopg://u:pw1@h/sc", "required": True}}
    )
    engines = db.engine_count
    # Password-only rotation changes the URL -> re-register, engines rebuilt
    changed = await db.ensure_database(
        "shard-c", {"primary": {"url": "postgresql+psycopg://u:pw2@h/sc", "required": True}}
    )
    assert changed is True
    assert db.engine_count == engines  # old disposed, new created
    assert "pw2" in db.config.databases["shard-c"].primary.url
    await db.close()


async def test_dynamic_first_empty_databases_config() -> None:
    db = AsyncDatabase.from_config({"databases": {}})
    await db.start()  # no required engines; valid dynamic-first bootstrap
    await db.ensure_database("shard-d", shard_config("sd"))
    assert db._resolve(DatabaseTarget(database="shard-d")).database == "shard-d"
    await db.close()


async def test_ensure_database_concurrent_same_config() -> None:
    db = AsyncDatabase.from_config({"databases": {}})
    results = await asyncio.gather(
        *[db.ensure_database("shard-g", shard_config("sg")) for _ in range(10)]
    )
    assert "shard-g" in db.config.databases
    assert any(results)  # at least one performed the registration


# --- connection budget admission -------------------------------------------------------- #


async def test_register_rejected_when_process_budget_exceeded() -> None:
    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": "postgresql+psycopg://h/app"}}},
            "defaults": {"pool": {"size": 10, "max_overflow": 0}},
            "connection_budget": {"maximum_per_process": 15, "enforce_at_startup": True},
        }
    )
    # app already allows 10; adding another 10-connection shard would exceed 15
    with pytest.raises(DatabaseConfigurationError, match="budget"):
        await db.register_database("shard-b", shard_config("sb"))
    assert "shard-b" not in db.config.databases  # nothing swapped in


async def test_register_allowed_within_budget() -> None:
    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": "postgresql+psycopg://h/app"}}},
            "defaults": {"pool": {"size": 10, "max_overflow": 0}},
            "connection_budget": {"maximum_per_process": 30, "enforce_at_startup": True},
        }
    )
    await db.register_database("shard-b", shard_config("sb"))
    assert "shard-b" in db.config.databases


# --- max_databases LRU ------------------------------------------------------------------ #


async def test_max_databases_evicts_lru_dynamic() -> None:
    db = AsyncDatabase.from_config({"databases": {}, "max_databases": 2})
    await db.register_database("s1", shard_config("s1"))
    await db.register_database("s2", shard_config("s2"))
    await db.register_database("s3", shard_config("s3"))  # evicts s1
    assert set(db.config.databases) == {"s2", "s3"}


async def test_ensure_touch_protects_from_eviction() -> None:
    db = AsyncDatabase.from_config({"databases": {}, "max_databases": 2})
    await db.register_database("s1", shard_config("s1"))
    await db.register_database("s2", shard_config("s2"))
    await db.ensure_database("s1", shard_config("s1"))  # touch s1 -> s2 is now LRU
    await db.register_database("s3", shard_config("s3"))
    assert set(db.config.databases) == {"s1", "s3"}


async def test_static_databases_never_evicted() -> None:
    db = AsyncDatabase.from_config({"databases": BASE_CONFIG["databases"], "max_databases": 1})
    await db.register_database("s1", shard_config("s1"))
    await db.register_database("s2", shard_config("s2"))  # evicts s1, never "app"
    assert set(db.config.databases) == {"app", "s2"}


async def test_eviction_disposes_engines() -> None:
    db = AsyncDatabase.from_config({"databases": {}, "max_databases": 1})
    await db.start()
    await db.register_database(
        "s1", {"primary": {"url": "postgresql+psycopg://h/s1", "required": True}}
    )
    assert db.engine_count == 1
    await db.register_database(
        "s2", {"primary": {"url": "postgresql+psycopg://h/s2", "required": True}}
    )
    assert set(db.config.databases) == {"s2"}
    assert db.engine_count == 1  # s1's engine disposed with its eviction
    await db.close()
