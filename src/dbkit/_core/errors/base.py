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
    code = "configuration_error"
    category = ErrorCategory.CONFIGURATION


class DatabaseRoutingError(DatabaseError):
    code = "routing_error"
    category = ErrorCategory.ROUTING


# --- Availability & connectivity ---------------------------------------------------- #


class DatabaseUnavailableError(DatabaseError):
    code = "unavailable"
    category = ErrorCategory.AVAILABILITY
    retryable = True


class DatabaseConnectionError(DatabaseError):
    code = "connection_error"
    category = ErrorCategory.CONNECTION
    retryable = True


class DatabasePoolTimeoutError(DatabaseError):
    code = "pool_timeout"
    category = ErrorCategory.POOL
    retryable = True


# --- Timeouts & locking ------------------------------------------------------------- #


class DatabaseQueryTimeoutError(DatabaseError):
    code = "query_timeout"
    category = ErrorCategory.TIMEOUT


class DatabaseLockTimeoutError(DatabaseError):
    code = "lock_timeout"
    category = ErrorCategory.LOCK
    retryable = True


class DatabaseDeadlockError(DatabaseError):
    code = "deadlock"
    category = ErrorCategory.LOCK
    retryable = True


class DatabaseSerializationError(DatabaseError):
    code = "serialization_failure"
    category = ErrorCategory.CONCURRENCY
    retryable = True


# --- Integrity ---------------------------------------------------------------------- #


class DatabaseIntegrityError(DatabaseError):
    code = "integrity_error"
    category = ErrorCategory.INTEGRITY


class DatabaseUniqueViolationError(DatabaseIntegrityError):
    code = "unique_violation"


class DatabaseForeignKeyViolationError(DatabaseIntegrityError):
    code = "foreign_key_violation"


class DatabaseNotNullViolationError(DatabaseIntegrityError):
    code = "not_null_violation"


class DatabaseCheckViolationError(DatabaseIntegrityError):
    code = "check_violation"


# --- Programming / permission ------------------------------------------------------- #


class DatabaseProgrammingError(DatabaseError):
    code = "programming_error"
    category = ErrorCategory.PROGRAMMING


class DatabaseSyntaxError(DatabaseProgrammingError):
    code = "syntax_error"


class DatabasePermissionError(DatabaseError):
    code = "permission_denied"
    category = ErrorCategory.PERMISSION


class DatabaseReadOnlyError(DatabaseError):
    code = "read_only_transaction"
    category = ErrorCategory.PERMISSION


# --- Transactions ------------------------------------------------------------------- #


class DatabaseTransactionError(DatabaseError):
    code = "transaction_error"
    category = ErrorCategory.TRANSACTION


class DatabaseCommitUnknownError(DatabaseError):
    """Commit outcome is genuinely unknown — do not retry unless idempotent (§15)."""

    code = "commit_unknown"
    category = ErrorCategory.TRANSACTION

    def __init__(self, message: str = "", **kwargs: Any) -> None:
        kwargs.setdefault("transaction_state_unknown", True)
        kwargs.setdefault("connection_invalidated", True)
        super().__init__(message, **kwargs)


class DatabaseCancellationError(DatabaseError):
    code = "cancelled"
    category = ErrorCategory.CANCELLED


# --- Results & mapping -------------------------------------------------------------- #


class DatabaseResultError(DatabaseError):
    code = "result_error"
    category = ErrorCategory.RESULT


class DatabaseMappingError(DatabaseError):
    code = "mapping_error"
    category = ErrorCategory.RESULT


# --- Resilience --------------------------------------------------------------------- #


class DatabaseCircuitOpenError(DatabaseError):
    code = "circuit_open"
    category = ErrorCategory.RESILIENCE
    retryable = True


class DatabaseOverloadedError(DatabaseError):
    code = "overloaded"
    category = ErrorCategory.CONCURRENCY
    retryable = True


class DatabaseUnsupportedOperationError(DatabaseError):
    code = "unsupported_operation"
    category = ErrorCategory.UNSUPPORTED
