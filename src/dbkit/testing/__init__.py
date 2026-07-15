"""dbkit testing — in-memory fakes so consumers unit-test without a live server.

``FakeAsyncDatabase`` / ``FakeDatabase`` mirror the query surface of the real
facades: they record every call and return rows you queue, in FIFO order::

    fake = FakeAsyncDatabase()
    fake.queue_rows([{"id": 1, "email": "a@x"}])
    rows = await fake.fetch_all(GET_USERS, {"limit": 10}, target=target)

    call = fake.calls[0]
    assert call.query_name == "users.list"
    assert call.params == {"limit": 10}
    assert call.target.database == "app"

Transactions yield the same fake, so queries inside ``async with fake.transaction(...)``
are recorded in the same ``calls`` list (with ``in_transaction=True``). Dynamic
registration (``ensure_database`` etc.) is tracked in ``registered``.
"""

from __future__ import annotations

import contextlib
from collections import deque
from collections.abc import AsyncIterator, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any

from .._core.query import Query, coerce_statement
from ..errors import DatabaseResultError

__all__ = ["FakeAsyncDatabase", "FakeDatabase", "RecordedCall"]


@dataclass(frozen=True)
class RecordedCall:
    """One recorded query execution."""

    method: str  # fetch_all | fetch_one | fetch_optional | fetch_value | execute
    query_name: str
    statement: str
    params: Mapping[str, Any] | None
    target: Any
    settings: Mapping[str, str] | None = None
    in_transaction: bool = False


@dataclass
class _FakeState:
    calls: list[RecordedCall] = field(default_factory=list)
    queued: deque[list[Mapping[str, Any]]] = field(default_factory=deque)
    registered: dict[str, Any] = field(default_factory=dict)
    started: bool = False
    closed: bool = False


class _FakeCore:
    """Shared machinery for both fakes."""

    def __init__(self) -> None:
        self._state = _FakeState()
        self._in_transaction = False

    # -- test-facing helpers -------------------------------------------------------- #

    @property
    def calls(self) -> list[RecordedCall]:
        """Every recorded query, in execution order."""
        return self._state.calls

    @property
    def registered(self) -> dict[str, Any]:
        """Dynamically registered database configs by name."""
        return self._state.registered

    @property
    def started(self) -> bool:
        return self._state.started

    @property
    def closed(self) -> bool:
        return self._state.closed

    def queue_rows(self, rows: list[Mapping[str, Any]]) -> None:
        """Queue one result set; each fetch consumes the next queued set (FIFO).
        An empty queue yields ``[]``."""
        self._state.queued.append(list(rows))

    def calls_named(self, query_name: str) -> list[RecordedCall]:
        """The recorded calls for one logical query name."""
        return [c for c in self._state.calls if c.query_name == query_name]

    # -- internals ------------------------------------------------------------------ #

    def _record(
        self,
        method: str,
        query: object,
        params: Mapping[str, Any] | None,
        target: Any,
    ) -> None:
        q = query if isinstance(query, Query) else None
        statement = str(coerce_statement(query))
        self._state.calls.append(
            RecordedCall(
                method=method,
                query_name=q.name if q else "adhoc",
                statement=statement,
                params=dict(params) if params else None,
                target=target,
                settings=dict(q.settings) if q and q.settings else None,
                in_transaction=self._in_transaction,
            )
        )

    def _next_rows(self) -> list[Mapping[str, Any]]:
        if self._state.queued:
            return self._state.queued.popleft()
        return []

    def _one(self, method: str) -> Mapping[str, Any]:
        rows = self._next_rows()
        if len(rows) != 1:
            raise DatabaseResultError(f"{method} expected exactly one row, queued {len(rows)}")
        return rows[0]

    def _register(self, name: str, config: Any) -> bool:
        replaced = name in self._state.registered
        self._state.registered[name] = config
        return replaced

    def _ensure(self, name: str, config: Any) -> bool:
        if self._state.registered.get(name) == config:
            return False
        self._state.registered[name] = config
        return True


class FakeAsyncDatabase(_FakeCore):
    """Drop-in test double for :class:`dbkit.AsyncDatabase` (query surface only)."""

    async def start(self, *, warm: bool = False) -> None:
        self._state.started = True

    async def close(self, grace_period: float = 10.0) -> None:
        self._state.closed = True

    async def fetch_all(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: Any = None,
        **_: Any,
    ) -> list[Any]:
        self._record("fetch_all", query, params, target)
        return self._next_rows()

    async def fetch_one(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: Any = None,
        **_: Any,
    ) -> Any:
        self._record("fetch_one", query, params, target)
        return self._one("fetch_one")

    async def fetch_optional(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: Any = None,
        **_: Any,
    ) -> Any | None:
        self._record("fetch_optional", query, params, target)
        rows = self._next_rows()
        return rows[0] if rows else None

    async def fetch_value(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: Any = None,
        **_: Any,
    ) -> Any:
        self._record("fetch_value", query, params, target)
        row = self._one("fetch_value")
        return next(iter(row.values()))

    async def execute(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: Any = None,
        **_: Any,
    ) -> int:
        self._record("execute", query, params, target)
        return len(self._next_rows())

    @contextlib.asynccontextmanager
    async def transaction(
        self, *, target: Any = None, **_: Any
    ) -> AsyncIterator[FakeAsyncDatabase]:
        """Yields the fake itself; nested calls record with ``in_transaction=True``."""
        self._in_transaction = True
        try:
            yield self
        finally:
            self._in_transaction = False

    async def register_database(
        self, name: str, config: Any, *, connect: bool | None = None
    ) -> bool:
        return self._register(name, config)

    async def ensure_database(self, name: str, config: Any, *, connect: bool | None = None) -> bool:
        return self._ensure(name, config)

    async def unregister_database(self, name: str) -> bool:
        return self._state.registered.pop(name, None) is not None


class FakeDatabase(_FakeCore):
    """Drop-in test double for :class:`dbkit.Database` (query surface only)."""

    def start(self, *, warm: bool = False) -> None:
        self._state.started = True

    def close(self, grace_period: float = 10.0) -> None:
        self._state.closed = True

    def fetch_all(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: Any = None,
        **_: Any,
    ) -> list[Any]:
        self._record("fetch_all", query, params, target)
        return self._next_rows()

    def fetch_one(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: Any = None,
        **_: Any,
    ) -> Any:
        self._record("fetch_one", query, params, target)
        return self._one("fetch_one")

    def fetch_optional(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: Any = None,
        **_: Any,
    ) -> Any | None:
        self._record("fetch_optional", query, params, target)
        rows = self._next_rows()
        return rows[0] if rows else None

    def fetch_value(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: Any = None,
        **_: Any,
    ) -> Any:
        self._record("fetch_value", query, params, target)
        row = self._one("fetch_value")
        return next(iter(row.values()))

    def execute(
        self,
        query: object,
        params: Mapping[str, Any] | None = None,
        *,
        target: Any = None,
        **_: Any,
    ) -> int:
        self._record("execute", query, params, target)
        return len(self._next_rows())

    @contextlib.contextmanager
    def transaction(self, *, target: Any = None, **_: Any) -> Iterator[FakeDatabase]:
        """Yields the fake itself; nested calls record with ``in_transaction=True``."""
        self._in_transaction = True
        try:
            yield self
        finally:
            self._in_transaction = False

    def register_database(self, name: str, config: Any, *, connect: bool | None = None) -> bool:
        return self._register(name, config)

    def ensure_database(self, name: str, config: Any, *, connect: bool | None = None) -> bool:
        return self._ensure(name, config)

    def unregister_database(self, name: str) -> bool:
        return self._state.registered.pop(name, None) is not None
