"""Engine registry keys (§9).

One engine exists per unique ``environment:database:shard:role:driver`` tuple within a
process. The key is a plain string so it is cheap to hash, log, and expose as a metric label.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EngineKey:
    environment: str
    database: str
    shard_id: str
    role: str  # "primary" | "replica:<name>"
    driver: str

    def __str__(self) -> str:
        return f"{self.environment}:{self.database}:{self.shard_id}:{self.role}:{self.driver}"

    @property
    def label(self) -> str:
        return str(self)
