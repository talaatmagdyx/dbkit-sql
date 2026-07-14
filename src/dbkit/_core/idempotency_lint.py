"""Best-effort static heuristic flagging writes marked ``idempotent=True`` whose SQL text has
no visible guard against duplicate execution (§14, §18.3).

``Query.idempotent`` only tells dbkit's retry executor it's *allowed* to retry a write — it is
a self-declared flag, not a verified property. Marking a plain ``INSERT`` idempotent without an
``ON CONFLICT``/uniqueness guard is the single most common way to turn a transient network blip
into a duplicate row. This module is a *lint*, not a gate: it recognizes common textual patterns
that make a write self-evidently safe to repeat, so ``dbkit query-list`` can nudge a reviewer
toward double-checking, not enforce anything (dbkit cannot know the schema's unique constraints
from SQL text alone).
"""

from __future__ import annotations

import re

from .query import Query

# Patterns that make a write's own SQL text self-evidently safe to run twice: an explicit
# conflict/idempotency guard.
_GUARD_PATTERNS = (
    re.compile(r"\bON\s+CONFLICT\b", re.IGNORECASE),
    re.compile(r"\bWHERE\s+NOT\s+EXISTS\b", re.IGNORECASE),
    re.compile(r"\bMERGE\b", re.IGNORECASE),
    re.compile(r"\bINSERT\s+IGNORE\b", re.IGNORECASE),  # MySQL-style; harmless to recognize
)

# INSERT is the operation most likely to duplicate rows on a naive retry. UPDATE/DELETE/etc.
# targeting a specific row via WHERE are typically naturally idempotent (repeating them leaves
# the same end state), so they're not flagged even without an explicit guard.
_INSERT_RE = re.compile(r"^\s*INSERT\b", re.IGNORECASE)


def statement_text(query: Query) -> str | None:
    """Best-effort plain SQL text for ``query.statement``, or ``None`` if unavailable (e.g. a
    SQLAlchemy Core construct rather than a ``sql()``/``text()`` clause)."""
    text_attr = getattr(query.statement, "text", None)
    return text_attr if isinstance(text_attr, str) else None


def looks_unsafe_to_retry(query: Query) -> bool:
    """Whether ``query`` is marked ``idempotent=True`` for a write but its SQL text has no
    visible guard against duplication on retry.

    This is a heuristic, not a guarantee either way: a false positive (flagging a genuinely-safe
    query) just means an extra line in ``dbkit query-list``; a false negative is expected and
    unavoidable — recognizing that a write is *actually* safe to repeat requires knowing the
    schema's unique constraints, which isn't visible from SQL text alone.
    """
    if not (query.is_write and query.idempotent):
        return False
    text_ = statement_text(query)
    if text_ is None:
        return False  # can't inspect a bare Core construct's text; nothing to flag
    if not _INSERT_RE.match(text_):
        return False  # UPDATE/DELETE/etc. are typically naturally idempotent
    return not any(p.search(text_) for p in _GUARD_PATTERNS)
