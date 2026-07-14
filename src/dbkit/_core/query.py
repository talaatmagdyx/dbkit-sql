"""Query objects, the ``sql()`` wrapper, and the query registry (§8, §18.3).

Every method on the database facade accepts one of:

* a :class:`Query` (named, with metadata),
* a SQLAlchemy Core ``Executable`` / ``TextClause``, or
* the result of :func:`sql` — the *only* accepted path for a raw SQL string.

Passing a bare ``str`` is rejected (§18.4): it is the single most common SQL-injection and
un-parameterized-query footgun, so we force an explicit, greppable wrapper.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.expression import Executable

from .errors import DatabaseProgrammingError

Operation = Literal["read", "write", "ddl"]
Cardinality = Literal["one", "optional", "many", "none"]

#: What the facade will actually execute — a Core construct or a text clause.
Statement = Executable | TextClause


def sql(statement: str) -> TextClause:
    """Wrap a raw SQL string in a SQLAlchemy ``text()`` clause.

    This is the only supported way to pass raw SQL. Parameters must be bound with
    ``:name`` placeholders — never interpolated (§18.4)::

        sql("SELECT * FROM users WHERE id = :user_id")
    """
    if not isinstance(statement, str):
        raise DatabaseProgrammingError(
            "sql() takes a raw SQL string; pass Core constructs directly instead"
        )
    return text(statement)


def coerce_statement(obj: object) -> Statement:
    """Validate/normalize a caller-supplied statement, rejecting bare strings."""
    if isinstance(obj, Query):
        return obj.statement
    if isinstance(obj, (TextClause, Executable)):
        return obj
    if isinstance(obj, str):
        raise DatabaseProgrammingError(
            "raw SQL strings must be wrapped with sql(...) — refusing bare str to prevent "
            "un-parameterized queries"
        )
    raise DatabaseProgrammingError(
        f"unsupported query type: {type(obj).__name__}; expected Query, sql(...), or a "
        "SQLAlchemy Core statement"
    )


@dataclass(frozen=True, slots=True)
class Query:
    """A named, parameterized query with execution metadata (§8.5).

    ``name`` is a stable *logical* label used for metrics, tracing, and logs — never the raw
    SQL text (§18.3). ``sensitive_parameters`` are redacted everywhere (§13.4).
    """

    name: str
    statement: Statement
    operation: Operation = "read"
    timeout: float | None = None
    #: Declares this write *may* be retried by dbkit's retry executor (§14) — it does **not**
    #: verify the SQL is actually safe to run twice. Marking a plain ``INSERT`` idempotent
    #: without an ``ON CONFLICT``/uniqueness guard is the most common way a transient network
    #: blip (the write commits, the client never sees the ack) turns into a duplicate row on
    #: retry. Only set this when the statement itself is genuinely safe to repeat (``ON
    #: CONFLICT``, ``WHERE NOT EXISTS``, or a natural-key ``UPDATE``/``DELETE``) — see
    #: ``dbkit query-list``, which flags writes that look unsafe by this heuristic.
    idempotent: bool = False
    #: Marks this query for the separate ``expensive_queries`` concurrency tier
    #: (``ConcurrencyConfig.expensive_queries``, §17), acquired *in addition to* the normal
    #: reads/writes tier — for a small number of known-heavy queries (a big aggregation, an
    #: ad-hoc report) that would otherwise be free to pile up alongside ordinary traffic and
    #: saturate the database. Most queries should leave this ``False``.
    expensive: bool = False
    expected_cardinality: Cardinality | None = None
    sensitive_parameters: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        """Reject bare-string statements/empty names and normalize ``sensitive_parameters``."""
        if isinstance(self.statement, str):  # pragma: no cover - guarded by typing too
            raise DatabaseProgrammingError(
                f"Query(name={self.name!r}) statement must be sql(...) or a Core construct, "
                "not a bare string"
            )
        if not self.name:
            raise DatabaseProgrammingError("Query.name must be a non-empty logical name")
        # Normalize sensitive_parameters to a frozenset even if a set/list was passed.
        object.__setattr__(self, "sensitive_parameters", frozenset(self.sensitive_parameters))

    @property
    def is_write(self) -> bool:
        """Whether this query mutates data or schema (``operation`` is ``write``/``ddl``)."""
        return self.operation in ("write", "ddl")


class QueryRegistry:
    """In-process registry of named queries for CLI listing and duplicate detection (§8.5)."""

    def __init__(self) -> None:
        """Create an empty registry."""
        self._queries: dict[str, Query] = {}

    def register(self, query: Query) -> Query:
        """Register ``query`` and return it; raises if the name is already taken by a
        *different* :class:`Query` object (registering the same object twice is a no-op)."""
        existing = self._queries.get(query.name)
        if existing is not None and existing is not query:
            raise DatabaseProgrammingError(
                f"duplicate query name {query.name!r} registered with a different statement"
            )
        self._queries[query.name] = query
        return query

    def get(self, name: str) -> Query | None:
        """The registered :class:`Query` named ``name``, or ``None`` if none was registered."""
        return self._queries.get(name)

    def names(self) -> list[str]:
        """Every registered query name, sorted."""
        return sorted(self._queries)

    def all(self) -> list[Query]:
        """Every registered :class:`Query`, ordered by name."""
        return [self._queries[n] for n in self.names()]


#: Process-global default registry. Applications may create their own.
default_registry = QueryRegistry()
