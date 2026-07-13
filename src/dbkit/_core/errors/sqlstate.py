"""PostgreSQL SQLSTATE → dbkit error class mapping (§13.3).

The classifier prefers SQLSTATE codes over matching driver message strings. Phase 1 covers
the core set (integrity, timeout/cancel, connectivity, concurrency, read-only). The table is
structured so Phase 2 can extend it without touching the classifier.

Reference: https://www.postgresql.org/docs/current/errcodes-appendix.html
"""

from __future__ import annotations

from . import base as e

# Exact SQLSTATE code -> error class.
SQLSTATE_MAP: dict[str, type[e.DatabaseError]] = {
    # Class 23 — integrity constraint violation
    "23505": e.DatabaseUniqueViolationError,
    "23503": e.DatabaseForeignKeyViolationError,
    "23502": e.DatabaseNotNullViolationError,
    "23514": e.DatabaseCheckViolationError,
    "23000": e.DatabaseIntegrityError,
    # Class 40 — transaction rollback
    "40001": e.DatabaseSerializationError,
    "40P01": e.DatabaseDeadlockError,
    "40000": e.DatabaseTransactionError,
    "40002": e.DatabaseIntegrityError,  # transaction integrity constraint violation
    "40003": e.DatabaseCommitUnknownError,  # statement completion unknown
    # Class 55 — object not in prerequisite state
    "55P03": e.DatabaseLockTimeoutError,  # lock_not_available
    # Class 57 — operator intervention
    "57014": e.DatabaseQueryTimeoutError,  # query_canceled (statement_timeout)
    "57P01": e.DatabaseConnectionError,  # admin_shutdown
    "57P02": e.DatabaseConnectionError,  # crash_shutdown
    "57P03": e.DatabaseUnavailableError,  # cannot_connect_now
    # Class 53 — insufficient resources
    "53300": e.DatabaseUnavailableError,  # too_many_connections
    "53400": e.DatabaseUnavailableError,  # configuration_limit_exceeded
    # Class 25 — invalid transaction state
    "25006": e.DatabaseReadOnlyError,  # read_only_sql_transaction
    "25P02": e.DatabaseTransactionError,  # in_failed_sql_transaction
    # Class 08 — connection exception
    "08000": e.DatabaseConnectionError,
    "08003": e.DatabaseConnectionError,
    "08006": e.DatabaseConnectionError,
    "08001": e.DatabaseConnectionError,
    "08004": e.DatabaseConnectionError,
    # Class 42 — syntax error or access rule violation
    "42601": e.DatabaseSyntaxError,
    "42501": e.DatabasePermissionError,  # insufficient_privilege
    "42P01": e.DatabaseProgrammingError,  # undefined_table
    "42703": e.DatabaseProgrammingError,  # undefined_column
    "42883": e.DatabaseProgrammingError,  # undefined_function
    # Class 22 — data exception (caller/data problem, not retryable)
    "22P02": e.DatabaseProgrammingError,  # invalid_text_representation
}

# Two-character SQLSTATE class -> fallback error class when the exact code is unknown.
SQLSTATE_CLASS_MAP: dict[str, type[e.DatabaseError]] = {
    "08": e.DatabaseConnectionError,
    "23": e.DatabaseIntegrityError,
    "42": e.DatabaseProgrammingError,
    "40": e.DatabaseTransactionError,
    "53": e.DatabaseUnavailableError,
    "22": e.DatabaseProgrammingError,
}


def error_class_for_sqlstate(sqlstate: str | None) -> type[e.DatabaseError] | None:
    """Return the dbkit error class for a SQLSTATE, or ``None`` if unrecognized."""
    if not sqlstate:
        return None
    if sqlstate in SQLSTATE_MAP:
        return SQLSTATE_MAP[sqlstate]
    return SQLSTATE_CLASS_MAP.get(sqlstate[:2])
