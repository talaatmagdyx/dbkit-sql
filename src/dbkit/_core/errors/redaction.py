"""Secret redaction for messages, DSNs, and parameters (§13.4, §29).

Nothing that could carry credentials, tokens, or personal data may reach a log, trace,
metric, or error message unredacted. These helpers are intentionally conservative: when in
doubt, redact.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit, urlunsplit

REDACTED = "***"

# postgresql+psycopg://user:secret@host:5432/db  ->  redact the password component.
_DSN_RE = re.compile(r"(?P<pre>[a-z0-9+]+://[^:/\s]+:)(?P<pw>[^@/\s]+)(?P<post>@)", re.IGNORECASE)

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
    """Strip DSN passwords out of a free-form error/driver message."""
    return redact_dsn(message)
