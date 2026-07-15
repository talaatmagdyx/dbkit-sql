"""Normalized error hierarchy (§13).

Every failure dbkit surfaces — from SQLAlchemy, the DBAPI driver, the network, timeouts,
routing, or configuration — is normalized into a :class:`DatabaseError` subclass with a
stable ``code``, a :class:`ErrorCategory`, retryability, and connection/transaction
outcome certainty. Messages are sanitized so secrets never leak (§13.4, §29).
"""

from __future__ import annotations

import enum
from typing import Any


class ErrorCategory(enum.Enum):
    """Coarse grouping used for metrics labels and retry/circuit-breaker decisions."""

    CONFIGURATION = "configuration"
    ROUTING = "routing"
    AVAILABILITY = "availability"
    CONNECTION = "connection"
    POOL = "pool"
    TIMEOUT = "timeout"
    LOCK = "lock"
    CONCURRENCY = "concurrency"
    INTEGRITY = "integrity"
    PROGRAMMING = "programming"
    PERMISSION = "permission"
    TRANSACTION = "transaction"
    CANCELLED = "cancelled"
    RESULT = "result"
    RESILIENCE = "resilience"
    UNSUPPORTED = "unsupported"


#: Categories meaning the pool/limiter/breaker shed load or the backend is unreachable —
#: the request is retryable and an HTTP API should answer 503 (+ Retry-After).
OVERLOAD_CATEGORIES: frozenset[ErrorCategory] = frozenset(
    {
        ErrorCategory.POOL,
        ErrorCategory.CONCURRENCY,
        ErrorCategory.RESILIENCE,
        ErrorCategory.AVAILABILITY,
        ErrorCategory.CONNECTION,
    }
)

#: Categories meaning the query exceeded its time budget — an HTTP API should answer 504.
TIMEOUT_CATEGORIES: frozenset[ErrorCategory] = frozenset(
    {
        ErrorCategory.TIMEOUT,
        ErrorCategory.CANCELLED,
        ErrorCategory.LOCK,
    }
)


class DatabaseError(Exception):
    """Base class for every error dbkit raises (§13.1).

    Attributes are populated by the classifier or by the raising call site. ``original``
    holds the underlying exception for internal inspection; it is never rendered into the
    user-facing message.
    """

    #: Stable internal code, e.g. ``"unique_violation"``. Subclasses set a default.
    code: str = "database_error"
    #: Coarse category. Subclasses set a default.
    category: ErrorCategory = ErrorCategory.AVAILABILITY
    #: Whether an operation raising this error is *potentially* retryable (policy still
    #: applies idempotency gating on top of this).
    retryable: bool = False

    def __init__(
        self,
        message: str = "",
        *,
        code: str | None = None,
        category: ErrorCategory | None = None,
        retryable: bool | None = None,
        connection_invalidated: bool = False,
        transaction_state_unknown: bool = False,
        database_name: str | None = None,
        shard_id: str | None = None,
        role: str | None = None,
        query_name: str | None = None,
        sqlstate: str | None = None,
        original: BaseException | None = None,
    ) -> None:
        """Any keyword left ``None`` falls back to the subclass default or is left unset.

        ``original`` and the routing/query context fields are for internal inspection and
        logging — never interpolated into the exception message itself (§13.4, §29).
        """
        self.code = code or type(self).code
        self.category = category or type(self).category
        self.retryable = type(self).retryable if retryable is None else retryable
        self.connection_invalidated = connection_invalidated
        self.transaction_state_unknown = transaction_state_unknown
        self.database_name = database_name
        self.shard_id = shard_id
        self.role = role
        self.query_name = query_name
        self.sqlstate = sqlstate
        self.original = original
        super().__init__(message or self._default_message())

    def _default_message(self) -> str:
        parts = [self.code]
        if self.query_name:
            parts.append(f"query={self.query_name}")
        if self.database_name:
            parts.append(f"database={self.database_name}")
        if self.sqlstate:
            parts.append(f"sqlstate={self.sqlstate}")
        return " ".join(parts)

    def with_context(
        self,
        *,
        database_name: str | None = None,
        shard_id: str | None = None,
        role: str | None = None,
        query_name: str | None = None,
    ) -> DatabaseError:
        """Attach routing/query context discovered by an outer layer. Returns self."""
        if database_name is not None:
            self.database_name = database_name
        if shard_id is not None:
            self.shard_id = shard_id
        if role is not None:
            self.role = role
        if query_name is not None:
            self.query_name = query_name
        return self

    def to_dict(self) -> dict[str, Any]:
        """Safe, secret-free representation for logs/traces (§13.4)."""
        return {
            "code": self.code,
            "category": self.category.value,
            "retryable": self.retryable,
            "connection_invalidated": self.connection_invalidated,
            "transaction_state_unknown": self.transaction_state_unknown,
            "database": self.database_name,
            "shard": self.shard_id,
            "role": self.role,
            "query_name": self.query_name,
            "sqlstate": self.sqlstate,
            "message": str(self),
        }


# --- Configuration & routing -------------------------------------------------------- #


class DatabaseConfigurationError(DatabaseError):
    """Invalid or missing configuration, raised at startup before any connection is made."""

    code = "configuration_error"
    category = ErrorCategory.CONFIGURATION


class DatabaseRoutingError(DatabaseError):
    """A shard/replica/target could not be resolved (e.g. an unmapped shard key)."""

    code = "routing_error"
    category = ErrorCategory.ROUTING


# --- Availability & connectivity ---------------------------------------------------- #


class DatabaseUnavailableError(DatabaseError):
    """The database is reachable but not currently able to serve requests."""

    code = "unavailable"
    category = ErrorCategory.AVAILABILITY
    retryable = True


class DatabaseConnectionError(DatabaseError):
    """The connection could not be established or was lost mid-operation."""

    code = "connection_error"
    category = ErrorCategory.CONNECTION
    retryable = True


class DatabasePoolTimeoutError(DatabaseError):
    """No pooled connection became available before the pool checkout timeout elapsed."""

    code = "pool_timeout"
    category = ErrorCategory.POOL
    retryable = True


# --- Timeouts & locking ------------------------------------------------------------- #


class DatabaseQueryTimeoutError(DatabaseError):
    """A single statement exceeded its query timeout."""

    code = "query_timeout"
    category = ErrorCategory.TIMEOUT


class DatabaseLockTimeoutError(DatabaseError):
    """A statement gave up waiting on a row/table lock (PostgreSQL ``lock_timeout``)."""

    code = "lock_timeout"
    category = ErrorCategory.LOCK
    retryable = True


class DatabaseDeadlockError(DatabaseError):
    """PostgreSQL detected and broke a deadlock by aborting this transaction."""

    code = "deadlock"
    category = ErrorCategory.LOCK
    retryable = True


class DatabaseSerializationError(DatabaseError):
    """A ``SERIALIZABLE``/``REPEATABLE READ`` transaction failed to serialize; safe to retry."""

    code = "serialization_failure"
    category = ErrorCategory.CONCURRENCY
    retryable = True


# --- Integrity ---------------------------------------------------------------------- #


class DatabaseIntegrityError(DatabaseError):
    """Base class for constraint violations (SQLSTATE class 23)."""

    code = "integrity_error"
    category = ErrorCategory.INTEGRITY


class DatabaseUniqueViolationError(DatabaseIntegrityError):
    """A unique/primary-key constraint rejected the row."""

    code = "unique_violation"


class DatabaseForeignKeyViolationError(DatabaseIntegrityError):
    """A foreign-key constraint rejected the row."""

    code = "foreign_key_violation"


class DatabaseNotNullViolationError(DatabaseIntegrityError):
    """A ``NOT NULL`` column was given a null value."""

    code = "not_null_violation"


class DatabaseCheckViolationError(DatabaseIntegrityError):
    """A ``CHECK`` constraint rejected the row."""

    code = "check_violation"


# --- Programming / permission ------------------------------------------------------- #


class DatabaseProgrammingError(DatabaseError):
    """The statement itself is invalid (bad SQL, wrong types, undefined objects)."""

    code = "programming_error"
    category = ErrorCategory.PROGRAMMING


class DatabaseSyntaxError(DatabaseProgrammingError):
    """The statement failed to parse."""

    code = "syntax_error"


class DatabasePermissionError(DatabaseError):
    """The connected role lacks the privilege required for this operation."""

    code = "permission_denied"
    category = ErrorCategory.PERMISSION


class DatabaseReadOnlyError(DatabaseError):
    """A write was attempted against a read-only transaction or replica target."""

    code = "read_only_transaction"
    category = ErrorCategory.PERMISSION


# --- Transactions ------------------------------------------------------------------- #


class DatabaseTransactionError(DatabaseError):
    """A transaction-lifecycle operation (begin/commit/rollback/savepoint) failed."""

    code = "transaction_error"
    category = ErrorCategory.TRANSACTION


class DatabaseCommitUnknownError(DatabaseError):
    """Commit outcome is genuinely unknown — do not retry unless idempotent (§15)."""

    code = "commit_unknown"
    category = ErrorCategory.TRANSACTION

    def __init__(self, message: str = "", **kwargs: Any) -> None:
        """Always marks ``transaction_state_unknown``/``connection_invalidated`` (§15)."""
        kwargs.setdefault("transaction_state_unknown", True)
        kwargs.setdefault("connection_invalidated", True)
        super().__init__(message, **kwargs)


class DatabaseCancellationError(DatabaseError):
    """The operation was cancelled (e.g. by ``asyncio`` task cancellation or a client timeout)."""

    code = "cancelled"
    category = ErrorCategory.CANCELLED


# --- Results & mapping -------------------------------------------------------------- #


class DatabaseResultError(DatabaseError):
    """A cardinality expectation (exactly-one / at-most-one / scalar) was violated."""

    code = "result_error"
    category = ErrorCategory.RESULT


class DatabaseMappingError(DatabaseError):
    """A row could not be mapped to the requested ``map_to`` type."""

    code = "mapping_error"
    category = ErrorCategory.RESULT


# --- Resilience --------------------------------------------------------------------- #


class DatabaseCircuitOpenError(DatabaseError):
    """The circuit breaker for this database/shard/role is open; the call was short-circuited."""

    code = "circuit_open"
    category = ErrorCategory.RESILIENCE
    retryable = True


class DatabaseOverloadedError(DatabaseError):
    """A concurrency limiter rejected the call because its semaphore was exhausted."""

    code = "overloaded"
    category = ErrorCategory.CONCURRENCY
    retryable = True


class DatabaseUnsupportedOperationError(DatabaseError):
    """The requested operation isn't supported by the current dialect/driver/configuration."""

    code = "unsupported_operation"
    category = ErrorCategory.UNSUPPORTED
