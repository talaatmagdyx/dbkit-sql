"""dbkit — a thin, high-throughput SQL toolkit over SQLAlchemy Core (sync + async).

Public API::

    from dbkit import AsyncDatabase, Database, Query, sql, DatabaseTarget
    from dbkit import errors
"""

from __future__ import annotations

from . import errors
from ._async.database import AsyncDatabase
from ._core.config import (
    CircuitBreakerConfig,
    ConcurrencyConfig,
    ConnectionBudgetConfig,
    DatabaseConfig,
    DbkitConfig,
    Defaults,
    ObservabilityConfig,
    PoolConfig,
    RetryConfig,
    TargetConfig,
)
from ._core.query import Query, QueryRegistry, default_registry, sql
from ._core.result import ExecutionResult
from ._core.routing import DatabaseTarget
from ._sync.database import Database

__version__ = "0.1.0.dev0"

__all__ = [
    "AsyncDatabase",
    "CircuitBreakerConfig",
    "ConcurrencyConfig",
    "ConnectionBudgetConfig",
    "Database",
    "DatabaseConfig",
    "DatabaseTarget",
    "DbkitConfig",
    "Defaults",
    "ExecutionResult",
    "ObservabilityConfig",
    "PoolConfig",
    "Query",
    "QueryRegistry",
    "RetryConfig",
    "TargetConfig",
    "__version__",
    "default_registry",
    "errors",
    "sql",
]
