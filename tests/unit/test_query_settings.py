"""Query.settings — transaction-local GUCs applied via parameterized set_config."""

from __future__ import annotations

import pytest

from dbkit import Query, sql
from dbkit.errors import DatabaseProgrammingError


def test_query_accepts_settings() -> None:
    q = Query(name="q", statement=sql("SELECT 1"), settings={"jit": "off"})
    assert q.settings == {"jit": "off"}


def test_settings_default_none() -> None:
    q = Query(name="q", statement=sql("SELECT 1"))
    assert q.settings is None


async def test_invalid_setting_name_rejected() -> None:
    from dbkit._async.connection import _apply_local_settings

    class _Conn:
        async def execute(self, *a, **k):  # pragma: no cover - must not be reached
            raise AssertionError("should not execute")

    with pytest.raises(DatabaseProgrammingError, match="invalid setting name"):
        await _apply_local_settings(_Conn(), {"jit; DROP TABLE x": "off"}, is_postgres=True)


async def test_settings_use_parameterized_set_config() -> None:
    from dbkit._async.connection import _apply_local_settings

    executed: list[tuple[str, dict]] = []

    class _Conn:
        async def execute(self, statement, params=None):
            executed.append((str(statement), params))

    await _apply_local_settings(_Conn(), {"jit": "off", "work_mem": "64MB"}, is_postgres=True)
    assert len(executed) == 2
    stmt, params = executed[0]
    assert "set_config" in stmt
    assert ":setting_name" in stmt  # values bound, never interpolated
    assert params == {"setting_name": "jit", "setting_value": "off"}


async def test_settings_skipped_when_empty() -> None:
    from dbkit._async.connection import _apply_local_settings

    await _apply_local_settings(None, None, is_postgres=True)  # no-op, no attribute access
    await _apply_local_settings(None, {}, is_postgres=True)
