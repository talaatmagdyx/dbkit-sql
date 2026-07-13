from __future__ import annotations

import asyncio

import pytest

from dbkit import DbkitConfig
from dbkit._async.engine import AsyncEngineRegistry
from dbkit._core.keys import EngineKey
from dbkit._core.routing import ResolvedRoute
from dbkit.errors import DatabaseConfigurationError, DatabaseRoutingError

# A URL that builds a valid engine object without ever connecting.
CFG = DbkitConfig.from_dict(
    {
        "environment": "test",
        "databases": {"app": {"primary": {"url": "postgresql+psycopg://u:p@localhost:5432/app"}}},
    }
)

ROUTE = ResolvedRoute(database="app", shard_id="default", role="primary")


def test_engine_key_str() -> None:
    key = EngineKey("prod", "app", "s3", "primary", "psycopg")
    assert str(key) == "prod:app:s3:primary:psycopg"


async def test_lazy_single_engine() -> None:
    reg = AsyncEngineRegistry(CFG)
    assert reg.count == 0
    entry = await reg.get(ROUTE)
    assert reg.count == 1
    # same route returns cached engine
    assert (await reg.get(ROUTE)) is entry
    assert reg.count == 1
    await reg.dispose_all()
    assert reg.count == 0


async def test_concurrent_creation_is_race_free() -> None:
    reg = AsyncEngineRegistry(CFG)
    entries = await asyncio.gather(*[reg.get(ROUTE) for _ in range(50)])
    assert reg.count == 1
    assert all(e is entries[0] for e in entries)
    await reg.dispose_all()


async def test_unknown_database_fails_closed() -> None:
    reg = AsyncEngineRegistry(CFG)
    with pytest.raises(DatabaseRoutingError):
        await reg.get(ResolvedRoute(database="nope", shard_id="default", role="primary"))


async def test_max_engines_enforced() -> None:
    cfg = DbkitConfig.from_dict(
        {
            "databases": {
                "a": {"primary": {"url": "postgresql+psycopg://h/a"}},
                "b": {"primary": {"url": "postgresql+psycopg://h/b"}},
            }
        }
    )
    reg = AsyncEngineRegistry(cfg, max_engines=1)
    await reg.get(ResolvedRoute(database="a", shard_id="default", role="primary"))
    with pytest.raises(DatabaseConfigurationError, match="engine limit"):
        await reg.get(ResolvedRoute(database="b", shard_id="default", role="primary"))
    await reg.dispose_all()
