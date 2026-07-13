"""The sync frontend is generated from the async one; assert they stay in lock-step."""

from __future__ import annotations

import inspect
from pathlib import Path

from dbkit import AsyncDatabase, Database

SYNC_DIR = Path(__file__).resolve().parents[2] / "src" / "dbkit" / "_sync"

PUBLIC = [
    "start",
    "close",
    "require_ready",
    "fetch_one",
    "fetch_optional",
    "fetch_all",
    "fetch_value",
    "fetch_values",
    "execute",
    "execute_many",
    "connection",
    "transaction",
    "health",
    "pool_status",
    "from_config",
]


def test_both_expose_same_public_api() -> None:
    for name in PUBLIC:
        assert hasattr(AsyncDatabase, name), f"AsyncDatabase missing {name}"
        assert hasattr(Database, name), f"Database missing {name}"


def test_async_methods_are_coroutines_sync_are_not() -> None:
    assert inspect.iscoroutinefunction(AsyncDatabase.fetch_one)
    assert not inspect.iscoroutinefunction(Database.fetch_one)


def test_generated_sync_has_no_async_keywords() -> None:
    offenders: list[str] = []
    for path in SYNC_DIR.rglob("*.py"):
        if path.name == "_compat.py":  # hand-written, may legitimately differ
            continue
        text = path.read_text()
        for lineno, line in enumerate(text.splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if "async def " in line or "await " in line or "async with " in line:
                offenders.append(f"{path.name}:{lineno}: {stripped}")
    assert not offenders, "async constructs leaked into generated sync code:\n" + "\n".join(
        offenders
    )
