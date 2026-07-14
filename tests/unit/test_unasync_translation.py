"""Translation-completeness smoke test for ``tools/run_unasync.py`` (see ``docs/testing.md``).

``--check`` (wired into CI) only proves regeneration is deterministic — it can't catch a rule
that's simply *wrong* or incomplete, since a mistranslation that still parses and imports would
pass `--check` forever. This test feeds the transform a deliberately awkward async fixture
(nested ``async with``, ``async for``, chained ``await``, ``asynccontextmanager``,
``__aenter__``/``__aexit__``, an ``_async`` import path) and asserts the sync output is exactly
what a human would write by hand — so a future edit to the token table that breaks one of these
shapes fails loudly here instead of silently producing subtly-wrong generated code.
"""

from __future__ import annotations

import importlib.util
import pathlib

_TOOL_PATH = pathlib.Path(__file__).resolve().parents[2] / "tools" / "run_unasync.py"
_spec = importlib.util.spec_from_file_location("run_unasync", _TOOL_PATH)
assert _spec is not None and _spec.loader is not None
run_unasync = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(run_unasync)


FIXTURE_ASYNC = """\
import asyncio
import contextlib
from collections.abc import AsyncIterator
from sqlalchemy.ext.asyncio import AsyncConnection

from ._async.helpers import thing


@contextlib.asynccontextmanager
async def scope(conn: AsyncConnection) -> AsyncIterator[None]:
    async with conn.begin():
        async for row in await conn.stream(thing()):
            await handle(await transform(row))
    yield


class Widget:
    async def __aenter__(self) -> "Widget":
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


async def with_stack(conn: AsyncConnection) -> None:
    async with contextlib.AsyncExitStack() as stack:
        await stack.enter_async_context(conn.begin())
"""

EXPECTED_SYNC_BODY = """\
import threading
import contextlib
from collections.abc import Iterator
from sqlalchemy import Connection

from ._sync.helpers import thing


@contextlib.contextmanager
def scope(conn: Connection) -> Iterator[None]:
    with conn.begin():
        for row in conn.stream(thing()):
            handle(transform(row))
    yield


class Widget:
    def __enter__(self) -> "Widget":
        return self

    def __exit__(self, *exc: object) -> None:
        return None


def with_stack(conn: Connection) -> None:
    with contextlib.ExitStack() as stack:
        stack.enter_context(conn.begin())
"""


def test_transform_handles_awkward_async_constructs_exactly() -> None:
    result = run_unasync.transform(FIXTURE_ASYNC, "fixture.py")
    # strip the generated-file header the tool always prepends
    body = result.split("\n\n", 1)[1]
    assert body == EXPECTED_SYNC_BODY


def test_transform_leaves_no_async_only_tokens_behind() -> None:
    result = run_unasync.transform(FIXTURE_ASYNC, "fixture.py")
    body = result.split("\n\n", 1)[1]  # the header itself mentions "async" by name
    forbidden_tokens = (
        "async ",
        "await ",
        "Async",
        "__aenter__",
        "__aexit__",
        "asynccontextmanager",
    )
    for forbidden in forbidden_tokens:
        assert forbidden not in body, f"{forbidden!r} leaked into generated sync code"


def test_compat_py_is_never_generated() -> None:
    assert "_compat.py" in run_unasync.HANDWRITTEN
