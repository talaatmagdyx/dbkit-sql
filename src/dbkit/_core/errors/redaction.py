"""Secret redaction for messages, DSNs, and parameters (§13.4, §29).

Nothing that could carry credentials, tokens, or personal data may reach a log, trace,
metric, or error message unredacted. These helpers are intentionally conservative: when in
doubt, redact.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import unquote, urlsplit, urlunsplit

REDACTED = "***"

# Redaction must stay linear on long, attacker-influenced error text (a server error can quote user
# input in full). The search-heavy parts (scheme, user name, key names, the DSN password search) are
# bounded; a value that follows a recognised key is consumed greedily to its natural end, with no
# closing delimiter required, so there is nothing to backtrack over and an over-long value is
# redacted whole (fail closed). Messages are also capped (_MAX_MESSAGE).
#
# postgresql+psycopg://user:secret@host:5432/db  ->  redact the password component. As in
# SQLAlchemy's URL parser, the password runs from the ':' after the (possibly empty) user name to
# the next '@', so it may contain '/', '+', '=' or spaces; it stops at a line end. When in doubt
# this redacts more than the password (e.g. up to an unrelated '@' later on the line).
_DSN_RE = re.compile(
    r"(?P<pre>[a-z][a-z0-9+.\-]{0,31}://[^:/@\s]{0,1024}:)(?P<pw>[^@\n]{0,4096}?)(?P<post>@)",
    re.IGNORECASE,
)
# A URL query parameter whose (percent-decoded) key contains "password": SQLAlchemy splits the
# query on '&' only and decodes keys, so the value runs to '&' or whitespace ('#' is part of it).
_QUERY_RE = re.compile(r"(?P<pre>[?&](?P<key>[\w%.\-]{1,128})=)(?P<pw>[^&\s]*)")
# Any other key containing "password" followed by '=' (PGPASSWORD=, db_password=, libpq keyword
# DSNs): an unquoted value runs to whitespace (',', '}', '&' and '#' are ordinary characters there),
# a quoted one to its closing quote or the line end.
_KEYWORD_RE = re.compile(
    r"(?P<pre>(?<![\w?&%])\w{0,64}password\w{0,64}\s{0,8}=\s{0,8})"
    r"(?P<pw>'(?:[^'\\\n]|\\.)*'?|\S+)",
    re.IGNORECASE,
)
# Mapping and JSON forms ('password': 'x', "db_password": "x"): a quoted value to its closing quote
# or the line end, an unquoted one to ',', '}' or whitespace.
_MAPPING_RE = re.compile(
    r"(?P<pre>[\"']\w{0,64}password\w{0,64}[\"']\s{0,8}:\s{0,8})"
    r"(?P<pw>\"(?:[^\"\\\n]|\\.)*\"?|'(?:[^'\\\n]|\\.)*'?|[^\s,}]+)",
    re.IGNORECASE,
)
_MAX_MESSAGE = 8192
_WINDOW = _MAX_MESSAGE + 1024 + 4096 + 64  # redacted before cutting, so a cut never splits a secret


def _query_value(match: re.Match[str]) -> str:
    if "password" in unquote(match["key"]).lower():
        return f"{match['pre']}{REDACTED}"
    return match[0]


# Common secret-ish/PII key fragments; redacted from parameter maps unless caller opts out.
# This list is deliberately conservative and documented (not just here — see
# docs/troubleshooting.md's redaction section): it substring-matches credential and
# unambiguous-PII fragments, but cannot know an application's full schema. Anything outside
# this list — including context-dependent fields like email/phone that aren't always secret —
# must be declared via ``Query.sensitive_parameters``. See
# ``tests/property/test_invariants.py::test_hint_list_boundary_is_documented_and_tested`` for
# the exact, tested catch/miss boundary.
_SENSITIVE_KEY_HINTS = (
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "authorization",
    "auth",
    "credential",
    "private_key",
    "access_key",
    "ssn",
    "national_id",
    "credit_card",
    "card_number",
    "cvv",
    "iban",
    "dob",
    "date_of_birth",
    "pin",
)


def redact_dsn(value: str) -> str:
    """Redact the password in a URL/DSN while keeping it recognizable for debugging."""
    redacted = _DSN_RE.sub(rf"\g<pre>{REDACTED}\g<post>", value)
    redacted = _QUERY_RE.sub(_query_value, redacted)
    redacted = _KEYWORD_RE.sub(rf"\g<pre>{REDACTED}", redacted)
    redacted = _MAPPING_RE.sub(rf"\g<pre>{REDACTED}", redacted)
    # If urlsplit disagrees (e.g. odd formatting), fall back to a full rebuild.
    try:
        parts = urlsplit(redacted)
        if parts.password:
            netloc = parts.netloc.replace(f":{parts.password}@", f":{REDACTED}@")
            redacted = urlunsplit(parts._replace(netloc=netloc))
    except ValueError:
        pass
    return redacted


def is_sensitive_key(key: str) -> bool:
    """Whether ``key``'s name suggests it holds a secret (password, token, etc.), by substring."""
    lowered = key.lower()
    return any(hint in lowered for hint in _SENSITIVE_KEY_HINTS)


def redact_params(
    params: Mapping[str, Any] | None,
    *,
    sensitive: set[str] | None = None,
) -> dict[str, Any]:
    """Return a copy of ``params`` with sensitive values replaced by ``***``.

    A key is redacted if it is named in ``sensitive`` (from ``Query.sensitive_parameters``)
    or if it matches a built-in secret-ish hint.
    """
    if not params:
        return {}
    sensitive = sensitive or set()
    out: dict[str, Any] = {}
    for key, value in params.items():
        if key in sensitive or is_sensitive_key(key):
            out[key] = REDACTED
        else:
            out[key] = value
    return out


def sanitize_message(message: str) -> str:
    """Strip DSN passwords out of a free-form error/driver message. Long messages are cut to
    ``_MAX_MESSAGE`` characters first, so redaction time stays bounded (a server error can quote a
    large user input)."""
    if len(message) <= _MAX_MESSAGE:
        return redact_dsn(message)
    # Redact a window longer than the cap first, so a secret that straddles the cut is still found.
    return redact_dsn(message[:_WINDOW])[:_MAX_MESSAGE] + " ...(truncated)"
