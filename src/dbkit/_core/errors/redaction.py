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
# bounded and anchored; a value that follows a recognised key is consumed greedily to its natural
# end, with no closing delimiter required, so there is nothing to backtrack over and an over-long
# value is redacted whole (fail closed). Messages are cut to _MAX_MESSAGE before redaction.
#
# postgresql+psycopg://user:secret@host:5432/db  ->  redact the password component. As in
# SQLAlchemy's URL parser, the password runs from the ':' after the (possibly empty) user name to
# the next '@', so it may contain '/', '+', '=' or spaces; it stops at a line end. A password-less
# URL followed by an unrelated '@' later on the line ("postgresql://db:5432/app ... ops@corp") is
# recognised by its "port[/path]" then whitespace before any '@', and left readable.
_DSN_RE = re.compile(
    r"(?P<pre>(?<![a-z0-9+.\-])[a-z][a-z0-9+.\-]{0,31}://[^:/@\s]{0,1024}:)"
    r"(?!\d{1,5}(?:[/?][^@\s]{0,1024})?\s)(?P<pw>[^@\n]{0,4096}?)(?P<post>@)",
    re.IGNORECASE,
)
# Only when a message was cut: a DSN whose '@' was cut off has its password run to the end (fail
# closed).
_CUT_DSN_RE = re.compile(
    r"(?P<pre>(?<![a-z0-9+.\-])[a-z][a-z0-9+.\-]{0,31}://[^:/@\s]{0,1024}:)(?P<pw>[^@\n]*)\Z",
    re.IGNORECASE,
)
# A URL query parameter whose (percent-decoded) key contains "password": SQLAlchemy splits the
# query on '&' only and decodes keys, so the value runs to '&' or whitespace ('#' is part of it).
# Only keys that can be such a key (containing "password" or a '%' escape) are consumed, so a
# nested "?next=/cb?password=..." is still seen.
_QUERY_RE = re.compile(
    r"(?P<pre>[?&](?P<key>(?=[\w%.\-]{0,128}(?:password|%))[\w%.\-]{1,128})=)(?P<pw>[^&\s]*)",
    re.IGNORECASE,
)
# Any other key containing "password", quoted or not, followed by '=' or ':' (PGPASSWORD=,
# db_password=, libpq keyword DSNs, "password": "x", password: x): a quoted value runs to its
# closing quote or the line end; an unquoted one to whitespace (',', '}', '&' and '#' are part of
# it; for a JSON number or literal this redacts a trailing ',' or '}' too, which is harmless).
_KEYWORD_RE = re.compile(
    r"(?P<pre>(?<![\w?&%])[\"']?\w{0,64}password\w{0,64}[\"']?\s{0,8}[=:]\s{0,8})"
    r"(?P<pw>\"(?:[^\"\\\n]|\\.)*\"?|'(?:[^'\\\n]|\\.)*'?|\S+)",
    re.IGNORECASE,
)
_MAX_MESSAGE = 8192


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
    # If urlsplit disagrees (e.g. odd formatting), fall back to a full rebuild.
    try:
        parts = urlsplit(redacted)
        # Only for a URL value: in free text the "netloc" can run across words to an unrelated '@'.
        if parts.password and not any(c.isspace() for c in parts.netloc):
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
    ``_MAX_MESSAGE`` characters before redaction, so redaction time stays bounded (a server error
    can quote a large user input) and no text from past the cut can reach the output."""
    if len(message) <= _MAX_MESSAGE:
        return redact_dsn(message)
    # Cut first, so nothing past the cut can reach the output; a DSN password cut before its '@'
    # is redacted to the end (keyword and query values already run to the end without a delimiter).
    cut = _CUT_DSN_RE.sub(rf"\g<pre>{REDACTED}", message[:_MAX_MESSAGE])
    return redact_dsn(cut) + " ...(truncated)"
