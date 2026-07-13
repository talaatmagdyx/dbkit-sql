#!/usr/bin/env python3
"""Generate the synchronous package ``src/dbkit/_sync`` from ``src/dbkit/_async``.

The async package is the single source of truth. The sync package is produced by
token substitution plus a small set of line/block markers, and is checked into git so
that users can read, debug, and navigate it. CI runs this with ``--check`` to fail on
drift.

Markers (put them in the *async* source):

    something_async_only()            # unasync: remove
        drop this single line in the generated sync file.

    # unasync: remove-start
    ... async-only block ...
    # unasync: remove-end
        drop the marker lines and everything between them.

Everything else is a straight token replacement (see ``TOKENS``). Replacements are
applied longest-key-first so that, e.g., ``AsyncConnection`` is handled before
``Connection`` would ever match.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ASYNC_DIR = ROOT / "src" / "dbkit" / "_async"
SYNC_DIR = ROOT / "src" / "dbkit" / "_sync"

# Files whose sync/async forms genuinely diverge (client-side timeout, cancellation
# semantics). These are hand-written on BOTH sides and never generated.
HANDWRITTEN = {"_compat.py"}

# Order matters: applied longest-first, whole-word where it makes sense.
TOKENS: dict[str, str] = {
    # import module paths first
    "sqlalchemy.ext.asyncio": "sqlalchemy",
    "import asyncio": "import threading",
    "AsyncAdaptedQueuePool": "QueuePool",
    "create_async_engine": "create_engine",
    "async_sessionmaker": "sessionmaker",
    # dbkit's own classes (explicit whole-name maps so substrings stay consistent)
    "_AsyncTransactionManager": "_TransactionManager",
    "AsyncTransactionScope": "TransactionScope",
    "AsyncConnectionScope": "ConnectionScope",
    "AsyncEngineRegistry": "EngineRegistry",
    "AsyncResultStream": "ResultStream",
    "AsyncDatabase": "Database",
    # SQLAlchemy async types
    "AsyncConnection": "Connection",
    "AsyncEngine": "Engine",
    # concurrency primitives
    "asyncio.BoundedSemaphore": "threading.BoundedSemaphore",
    "asyncio.Semaphore": "threading.Semaphore",
    "asyncio.Lock": "threading.Lock",
    # dunder + control flow
    "__aenter__": "__enter__",
    "__aexit__": "__exit__",
    "__aiter__": "__iter__",
    "__anext__": "__next__",
    "async def": "def",
    "async with": "with",
    "async for": "for",
    "await ": "",
    "AsyncIterator": "Iterator",
    "AsyncGenerator": "Generator",
    "asynccontextmanager": "contextmanager",
    "aclose": "close",
    "aiter(": "iter(",
    "anext(": "next(",
    # package path (keep last so it doesn't touch the specific names above)
    "_async": "_sync",
}

REMOVE_LINE = re.compile(r"#\s*unasync:\s*remove\s*$")
REMOVE_START = re.compile(r"#\s*unasync:\s*remove-start\s*$")
REMOVE_END = re.compile(r"#\s*unasync:\s*remove-end\s*$")

HEADER = (
    "# This file is GENERATED from ../_async/{name} by tools/run_unasync.py.\n"
    "# Do not edit by hand. Run `make unasync` after changing the async source.\n"
)

# Compiled token matchers, longest key first.
_COMPILED = [
    (re.compile(re.escape(src)), dst)
    for src, dst in sorted(TOKENS.items(), key=lambda kv: len(kv[0]), reverse=True)
]


def transform(source: str, filename: str) -> str:
    out_lines: list[str] = []
    skipping = False
    for line in source.splitlines():
        if skipping:
            if REMOVE_END.search(line):
                skipping = False
            continue
        if REMOVE_START.search(line):
            skipping = True
            continue
        if REMOVE_LINE.search(line):
            continue
        for pattern, repl in _COMPILED:
            line = pattern.sub(repl, line)
        out_lines.append(line)
    body = "\n".join(out_lines)
    if body and not body.endswith("\n"):
        body += "\n"
    return HEADER.format(name=filename) + "\n" + body


def generate() -> dict[Path, str]:
    result: dict[Path, str] = {}
    for src in sorted(ASYNC_DIR.rglob("*.py")):
        if src.name in HANDWRITTEN:
            continue
        rel = src.relative_to(ASYNC_DIR)
        dst = SYNC_DIR / rel
        result[dst] = transform(src.read_text(), rel.as_posix())
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="exit non-zero if generated code is stale"
    )
    args = parser.parse_args()

    generated = generate()

    if args.check:
        stale: list[str] = []
        for dst, content in generated.items():
            if not dst.exists() or dst.read_text() != content:
                stale.append(str(dst.relative_to(ROOT)))
        # Also flag orphaned files that no longer have an async counterpart.
        expected = set(generated)
        for existing in SYNC_DIR.rglob("*.py"):
            if existing.name in HANDWRITTEN:
                continue
            if existing not in expected:
                stale.append(f"{existing.relative_to(ROOT)} (orphaned)")
        if stale:
            print("Sync package is stale. Run `make unasync`:", file=sys.stderr)
            for s in sorted(stale):
                print(f"  - {s}", file=sys.stderr)
            return 1
        print("Sync package is up to date.")
        return 0

    for dst, content in generated.items():
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(content)
        print(f"wrote {dst.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
