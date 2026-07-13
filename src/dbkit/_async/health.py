"""Liveness and readiness checks (§26).

Liveness verifies the process/event loop is functioning and must not require any database.
Readiness verifies that *required* database targets answer a trivial ``SELECT 1`` within a
strict, short timeout.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from .._core.errors import classify
from ._compat import timeout_scope


@dataclass
class TargetHealth:
    key: str
    healthy: bool
    error: str | None = None


@dataclass
class HealthReport:
    live: bool
    ready: bool
    targets: list[TargetHealth] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "live": self.live,
            "ready": self.ready,
            "targets": [
                {"key": t.key, "healthy": t.healthy, "error": t.error} for t in self.targets
            ],
        }


async def ping(engine: AsyncEngine, *, timeout: float) -> None:
    """Run ``SELECT 1`` with a strict timeout; raises a classified error on failure (§26.3)."""
    try:
        async with timeout_scope(timeout), engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception as exc:
        raise classify(exc) from exc
