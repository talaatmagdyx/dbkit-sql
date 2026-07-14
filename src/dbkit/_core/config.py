"""Configuration model, loaders, validation, and the connection-budget calculator (§30, §10.3).

Config is plain frozen dataclasses so there is no hard pydantic dependency. It can be built
from a dict, a YAML file (``[yaml]`` extra), or environment variables, with ``${VAR}`` /
``${VAR:-default}`` expansion. :meth:`DbkitConfig.redacted` produces a secret-free copy for
logging and diagnostics (§29, §30).
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Any
from urllib.parse import urlsplit

from .errors import DatabaseConfigurationError
from .errors.redaction import redact_dsn

_ENV_RE = re.compile(r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>[^}]*))?\}")


def expand_env(value: str, environ: Mapping[str, str] | None = None) -> str:
    """Expand ``${VAR}`` and ``${VAR:-default}`` against the environment."""
    env = environ if environ is not None else os.environ

    def _sub(m: re.Match[str]) -> str:
        name = m.group("name")
        if name in env:
            return env[name]
        default = m.group("default")
        if default is not None:
            return default
        raise DatabaseConfigurationError(
            f"environment variable {name!r} referenced in config is not set"
        )

    return _ENV_RE.sub(_sub, value)


def _expand_tree(obj: Any, environ: Mapping[str, str] | None) -> Any:
    if isinstance(obj, str):
        return expand_env(obj, environ)
    if isinstance(obj, Mapping):
        return {k: _expand_tree(v, environ) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_expand_tree(v, environ) for v in obj]
    return obj


@dataclass(frozen=True, slots=True)
class PoolConfig:
    """Connection pool settings, forwarded to SQLAlchemy's engine (§10.1-10.2)."""

    size: int = 10
    max_overflow: int = 5
    timeout_seconds: float = 5.0
    recycle_seconds: int = 1800
    pre_ping: bool = True
    use_lifo: bool = True
    reset_on_return: str = "rollback"  # "rollback" | "commit" | None
    connect_timeout_seconds: float = 10.0
    # Warn when a connection is held longer than this (leak detection, §10.5).
    long_hold_warning_seconds: float = 2.0
    # Use NullPool (no pooling) for external-pooler deployments like PgBouncer.
    disable_pooling: bool = False

    @property
    def max_connections(self) -> int:
        """Ceiling of concurrent connections this pool can open."""
        if self.disable_pooling:
            return 0  # unbounded / delegated to external pooler; excluded from budget math
        return self.size + self.max_overflow

    def validate(self) -> None:
        if self.size < 0:
            raise DatabaseConfigurationError("pool.size must be >= 0")
        if self.max_overflow < 0:
            raise DatabaseConfigurationError("pool.max_overflow must be >= 0")
        if self.timeout_seconds <= 0:
            raise DatabaseConfigurationError("pool.timeout_seconds must be > 0")
        if self.reset_on_return not in ("rollback", "commit", None, "none"):
            raise DatabaseConfigurationError(
                "pool.reset_on_return must be one of rollback|commit|none"
            )


@dataclass(frozen=True, slots=True)
class RetryConfig:
    """Retry policy (§14). Writes are not retried unless explicitly enabled *and* idempotent."""

    attempts: int = 2
    initial_delay_ms: float = 20.0
    maximum_delay_ms: float = 250.0
    multiplier: float = 2.0
    jitter: str = "full"  # "full" | "none"
    maximum_total_ms: float = 750.0
    retry_reads: bool = True
    retry_writes: bool = False

    def validate(self) -> None:
        if self.attempts < 1:
            raise DatabaseConfigurationError("retry.attempts must be >= 1")
        if self.jitter not in ("full", "none"):
            raise DatabaseConfigurationError("retry.jitter must be 'full' or 'none'")


@dataclass(frozen=True, slots=True)
class ConcurrencyConfig:
    """Optional per-target concurrency limits, independent of pool size (§17)."""

    database: int | None = None
    reads: int | None = None
    writes: int | None = None
    bulk_writes: int | None = None
    expensive_queries: int | None = None


@dataclass(frozen=True, slots=True)
class BulkConfig:
    """Batch-sizing defaults for bulk operations (§19.1)."""

    default_batch_rows: int = 1000
    max_batch_rows: int = 10000
    max_payload_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class CircuitBreakerConfig:
    """Per db+shard+role circuit breaker settings (§16)."""

    enabled: bool = False
    failure_threshold: int = 10
    window_seconds: float = 30.0
    open_seconds: float = 10.0
    half_open_max_calls: int = 2

    def validate(self) -> None:
        if self.failure_threshold < 1:
            raise DatabaseConfigurationError("circuit_breaker.failure_threshold must be >= 1")


@dataclass(frozen=True, slots=True)
class ObservabilityConfig:
    metrics: bool = True
    tracing: bool = False
    log_parameters: bool = False  # never log bound params in production (§25.3)
    slow_query_ms: float = 500.0


@dataclass(frozen=True, slots=True)
class ConnectionBudgetConfig:
    """A cap on how many connections this process may open across all its engines (§10.3)."""

    maximum_per_process: int | None = None
    enforce_at_startup: bool = False


@dataclass(frozen=True, slots=True)
class Defaults:
    driver: str = "psycopg"
    query_timeout_seconds: float = 2.0
    transaction_timeout_seconds: float = 5.0
    #: Warn when an explicit transaction is held open longer than this (§10.5, §16).
    long_transaction_warning_seconds: float = 5.0
    pool: PoolConfig = field(default_factory=PoolConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    bulk: BulkConfig = field(default_factory=BulkConfig)
    observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)

    def validate(self) -> None:
        self.pool.validate()
        self.retry.validate()
        self.circuit_breaker.validate()


@dataclass(frozen=True, slots=True)
class TargetConfig:
    """A single physical database endpoint (primary or one replica)."""

    url: str
    name: str = "primary"
    required: bool = True
    weight: int = 1
    pool: PoolConfig | None = None  # falls back to defaults.pool

    def resolved_pool(self, defaults: Defaults) -> PoolConfig:
        return self.pool or defaults.pool

    @property
    def driver(self) -> str:
        """Driver name parsed from the URL, e.g. ``psycopg`` or ``asyncpg``."""
        scheme = urlsplit(self.url).scheme
        return scheme.split("+", 1)[1] if "+" in scheme else scheme

    @property
    def dialect(self) -> str:
        return urlsplit(self.url).scheme.split("+", 1)[0]

    def validate(self) -> None:
        if not self.url:
            raise DatabaseConfigurationError(f"target {self.name!r} has an empty url")
        if not urlsplit(self.url).scheme:
            raise DatabaseConfigurationError(
                f"target {self.name!r} url is missing a dialect scheme (e.g. postgresql+psycopg://)"
            )


@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    """A logically named database: one primary and zero or more read replicas (§21)."""

    primary: TargetConfig
    replicas: tuple[TargetConfig, ...] = ()
    concurrency: ConcurrencyConfig = field(default_factory=ConcurrencyConfig)
    connection_budget: ConnectionBudgetConfig = field(default_factory=ConnectionBudgetConfig)

    def validate(self) -> None:
        self.primary.validate()
        for r in self.replicas:
            r.validate()

    def all_targets(self) -> tuple[TargetConfig, ...]:
        return (self.primary, *self.replicas)

    def max_connections(self, defaults: Defaults) -> int:
        """This database's own connection ceiling: sum of primary + every replica's pool (§10.3)."""
        return sum(t.resolved_pool(defaults).max_connections for t in self.all_targets())

    def enforce_connection_budget(self, defaults: Defaults, *, database_name: str) -> None:
        """Fail startup if this database alone exceeds its own configured budget (§10.3)."""
        limit = self.connection_budget.maximum_per_process
        if limit is None or not self.connection_budget.enforce_at_startup:
            return
        actual = self.max_connections(defaults)
        if actual > limit:
            raise DatabaseConfigurationError(
                f"connection budget exceeded for database {database_name!r}: configured "
                f"pools allow up to {actual} connections but the budget is {limit}"
            )


@dataclass(frozen=True, slots=True)
class DbkitConfig:
    """Root configuration object (§30)."""

    databases: Mapping[str, DatabaseConfig]
    environment: str = "default"
    defaults: Defaults = field(default_factory=Defaults)
    connection_budget: ConnectionBudgetConfig = field(default_factory=ConnectionBudgetConfig)
    #: Process-wide cap on live engines (across all databases/shards/tenants), for dynamic
    #: per-tenant deployments where the number of distinct tenants may be unbounded (§22.4).
    max_engines: int | None = None
    #: When True and ``max_engines`` is reached, evict (dispose) the least-recently-used
    #: engine instead of failing. Default False: exceeding the cap is a configuration error.
    evict_lru_engines: bool = False

    # -- construction ------------------------------------------------------------- #

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        environ: Mapping[str, str] | None = None,
        expand: bool = True,
    ) -> DbkitConfig:
        raw = _expand_tree(dict(data), environ) if expand else dict(data)
        try:
            config = _build_config(raw)
        except DatabaseConfigurationError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise DatabaseConfigurationError(f"invalid configuration: {exc}") from exc
        config.validate()
        return config

    @classmethod
    def from_yaml(cls, path: str, *, environ: Mapping[str, str] | None = None) -> DbkitConfig:
        try:
            import yaml
        except ImportError as exc:  # pragma: no cover
            raise DatabaseConfigurationError(
                "from_yaml requires PyYAML — install dbkit[yaml]"
            ) from exc
        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        if not isinstance(data, Mapping):
            raise DatabaseConfigurationError(f"{path} did not contain a mapping")
        # Allow a top-level 'dbkit:' wrapper key for embedding in larger config files.
        if "dbkit" in data and isinstance(data["dbkit"], Mapping):
            data = data["dbkit"]
        return cls.from_dict(data, environ=environ)

    # -- validation & budget ------------------------------------------------------ #

    def validate(self) -> None:
        if not self.databases:
            raise DatabaseConfigurationError("configuration defines no databases")
        self.defaults.validate()
        for name, db in self.databases.items():
            if not name:
                raise DatabaseConfigurationError("database name must be non-empty")
            db.validate()
            db.enforce_connection_budget(self.defaults, database_name=name)
        self.enforce_connection_budget()

    def max_connections_per_process(self) -> int:
        """Sum of every target's pool ceiling — the worst-case connections this process opens."""
        total = 0
        for db in self.databases.values():
            for target in db.all_targets():
                total += target.resolved_pool(self.defaults).max_connections
        return total

    def connection_budget_report(self, replicas: int = 1) -> dict[str, int]:
        """Cluster-wide connection projection (§10.3): ``per_process * replicas``."""
        per_process = self.max_connections_per_process()
        return {
            "per_process": per_process,
            "app_replicas": replicas,
            "cluster_total": per_process * max(replicas, 1),
        }

    def enforce_connection_budget(self) -> None:
        """Fail startup if a configured per-process budget is exceeded (§10.3)."""
        limit = self.connection_budget.maximum_per_process
        if limit is None or not self.connection_budget.enforce_at_startup:
            return
        actual = self.max_connections_per_process()
        if actual > limit:
            raise DatabaseConfigurationError(
                f"connection budget exceeded: configured pools allow up to {actual} "
                f"connections per process but the budget is {limit}"
            )

    # -- redaction ---------------------------------------------------------------- #

    def redacted(self) -> DbkitConfig:
        """Return a copy with all target URLs password-redacted (§30)."""
        return replace(
            self,
            databases={
                name: replace(
                    db,
                    primary=replace(db.primary, url=redact_dsn(db.primary.url)),
                    replicas=tuple(replace(r, url=redact_dsn(r.url)) for r in db.replicas),
                )
                for name, db in self.databases.items()
            },
        )


# --- dict -> dataclass builders ---------------------------------------------------- #


def _pool(data: Mapping[str, Any] | None) -> PoolConfig | None:
    if data is None:
        return None
    return PoolConfig(
        size=int(data.get("size", 10)),
        max_overflow=int(data.get("max_overflow", 5)),
        timeout_seconds=float(data.get("timeout_seconds", 5.0)),
        recycle_seconds=int(data.get("recycle_seconds", 1800)),
        pre_ping=bool(data.get("pre_ping", True)),
        use_lifo=bool(data.get("use_lifo", True)),
        reset_on_return=data.get("reset_on_return", "rollback"),
        connect_timeout_seconds=float(data.get("connect_timeout_seconds", 10.0)),
        long_hold_warning_seconds=float(data.get("long_hold_warning_seconds", 2.0)),
        disable_pooling=bool(data.get("disable_pooling", False)),
    )


def _retry(data: Mapping[str, Any] | None) -> RetryConfig:
    if data is None:
        return RetryConfig()
    return RetryConfig(
        attempts=int(data.get("attempts", 2)),
        initial_delay_ms=float(data.get("initial_delay_ms", 20.0)),
        maximum_delay_ms=float(data.get("maximum_delay_ms", 250.0)),
        multiplier=float(data.get("multiplier", 2.0)),
        jitter=data.get("jitter", "full"),
        maximum_total_ms=float(data.get("maximum_total_ms", 750.0)),
        retry_reads=bool(data.get("retry_reads", True)),
        retry_writes=bool(data.get("retry_writes", False)),
    )


def _bulk(data: Mapping[str, Any] | None) -> BulkConfig:
    if data is None:
        return BulkConfig()
    payload = data.get("max_payload_bytes")
    return BulkConfig(
        default_batch_rows=int(data.get("default_batch_rows", 1000)),
        max_batch_rows=int(data.get("max_batch_rows", 10000)),
        max_payload_bytes=int(payload) if payload is not None else None,
    )


def _circuit_breaker(data: Mapping[str, Any] | None) -> CircuitBreakerConfig:
    if data is None:
        return CircuitBreakerConfig()
    return CircuitBreakerConfig(
        enabled=bool(data.get("enabled", False)),
        failure_threshold=int(data.get("failure_threshold", 10)),
        window_seconds=float(data.get("window_seconds", 30.0)),
        open_seconds=float(data.get("open_seconds", 10.0)),
        half_open_max_calls=int(data.get("half_open_max_calls", 2)),
    )


def _observability(data: Mapping[str, Any] | None) -> ObservabilityConfig:
    if data is None:
        return ObservabilityConfig()
    return ObservabilityConfig(
        metrics=bool(data.get("metrics", True)),
        tracing=bool(data.get("tracing", False)),
        log_parameters=bool(data.get("log_parameters", False)),
        slow_query_ms=float(data.get("slow_query_ms", 500.0)),
    )


def _budget(data: Mapping[str, Any] | None) -> ConnectionBudgetConfig:
    if data is None:
        return ConnectionBudgetConfig()
    limit = data.get("maximum_per_process", data.get("maximum_application_share"))
    return ConnectionBudgetConfig(
        maximum_per_process=int(limit) if limit is not None else None,
        enforce_at_startup=bool(data.get("enforce_at_startup", False)),
    )


def _concurrency(data: Mapping[str, Any] | None) -> ConcurrencyConfig:
    if data is None:
        return ConcurrencyConfig()
    return ConcurrencyConfig(
        database=data.get("database"),
        reads=data.get("reads"),
        writes=data.get("writes"),
        bulk_writes=data.get("bulk", data.get("bulk_writes")),
        expensive_queries=data.get("expensive_queries"),
    )


def _defaults(data: Mapping[str, Any] | None) -> Defaults:
    if data is None:
        return Defaults()
    return Defaults(
        driver=data.get("driver", "psycopg"),
        query_timeout_seconds=float(data.get("query_timeout_seconds", 2.0)),
        transaction_timeout_seconds=float(data.get("transaction_timeout_seconds", 5.0)),
        long_transaction_warning_seconds=float(data.get("long_transaction_warning_seconds", 5.0)),
        pool=_pool(data.get("pool")) or PoolConfig(),
        retry=_retry(data.get("retry")),
        circuit_breaker=_circuit_breaker(data.get("circuit_breaker")),
        bulk=_bulk(data.get("bulk")),
        observability=_observability(data.get("observability")),
    )


def _target(data: Mapping[str, Any], *, name: str) -> TargetConfig:
    if "url" not in data:
        raise DatabaseConfigurationError(f"target {name!r} is missing 'url'")
    return TargetConfig(
        url=data["url"],
        name=data.get("name", name),
        required=bool(data.get("required", True)),
        weight=int(data.get("weight", 1)),
        pool=_pool(data.get("pool")),
    )


def _database(name: str, data: Mapping[str, Any]) -> DatabaseConfig:
    if "primary" not in data:
        raise DatabaseConfigurationError(f"database {name!r} is missing a 'primary' target")
    replicas_raw = data.get("replicas", [])
    replicas = tuple(
        _target(r, name=r.get("name", f"replica-{i}")) for i, r in enumerate(replicas_raw)
    )
    return DatabaseConfig(
        primary=_target(data["primary"], name="primary"),
        replicas=replicas,
        concurrency=_concurrency(data.get("concurrency")),
        connection_budget=_budget(data.get("connection_budget")),
    )


def _build_config(raw: Mapping[str, Any]) -> DbkitConfig:
    databases_raw = raw.get("databases")
    if not databases_raw:
        raise DatabaseConfigurationError("configuration must define a 'databases' mapping")
    databases = {name: _database(name, cfg) for name, cfg in databases_raw.items()}
    max_engines = raw.get("max_engines")
    return DbkitConfig(
        databases=databases,
        environment=raw.get("environment", "default"),
        defaults=_defaults(raw.get("defaults")),
        connection_budget=_budget(raw.get("connection_budget")),
        max_engines=int(max_engines) if max_engines is not None else None,
        evict_lru_engines=bool(raw.get("evict_lru_engines", False)),
    )
