# This file is GENERATED from ../_async/health.py by tools/run_unasync.py.
# Do not edit by hand. Run `make unasync` after changing the async source.

"""Liveness and readiness checks (§26).

Liveness verifies the process/event loop is functioning and must not require any database.
Readiness verifies that *required* database targets answer a trivial ``SELECT 1`` within a
strict, short timeout.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy import text
from sqlalchemy import Engine

from .._core.errors import classify
from ._compat import timeout_scope


@dataclass
class TargetHealth:
    """Readiness result for one required target."""

    key: str
    healthy: bool
    error: str | None = None


@dataclass
class HealthReport:
    """The overall result of :meth:`~dbkit.Database.health`."""

    live: bool
    ready: bool
    targets: list[TargetHealth] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        """A JSON-serializable representation, e.g. for the CLI/HTTP health endpoint."""
        return {
            "live": self.live,
            "ready": self.ready,
            "targets": [
                {"key": t.key, "healthy": t.healthy, "error": t.error} for t in self.targets
            ],
        }


def ping(engine: Engine, *, timeout: float) -> None:
    """Run ``SELECT 1`` with a strict timeout; raises a classified error on failure (§26.3)."""
    try:
        with timeout_scope(timeout), engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        raise classify(exc) from exc
