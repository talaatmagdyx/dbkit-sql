"""Engine registry keys (§9).

One engine exists per unique ``environment:database:shard:role:driver`` tuple within a
process. The key is a plain string so it is cheap to hash, log, and expose as a metric label.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EngineKey:
    """Identifies one engine: ``environment:database:shard:role:driver`` (§9)."""

    environment: str
    database: str
    shard_id: str
    role: str  # "primary" | "replica:<name>"
    driver: str

    def __str__(self) -> str:
        """``environment:database:shard_id:role:driver``."""
        return f"{self.environment}:{self.database}:{self.shard_id}:{self.role}:{self.driver}"

    @property
    def label(self) -> str:
        """Alias for ``str(self)``, for use as a metrics/log label."""
        return str(self)
