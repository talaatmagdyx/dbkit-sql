# Security

## SQL injection

`coerce_statement()` rejects any bare `str` passed as a query — every raw SQL string must go
through `sql()` (a thin `sqlalchemy.text()` wrapper), which requires `:name` bound parameters.
There is no code path that concatenates caller-supplied values into SQL text. Table/column
*identifiers* passed to `db.insert_many`/`db.upsert_many`/`db.copy_records` are the one
exception: they come from your own application code, not end users, and must not be built from
untrusted input (§18.5) — dbkit does not quote-escape identifiers beyond `"..."` wrapping.

## Redaction

Nothing that could carry credentials or personal data reaches a log, trace, metric, or error
message unredacted by default (§13.4, §29):

- **DSN passwords** are stripped from every error message and from `DbkitConfig.redacted()`
  (used by `dbkit check`/`config-validate`).
- **Bound parameters** are redacted by two independent mechanisms:
  1. `Query.sensitive_parameters` — an explicit, per-query `frozenset[str]` of parameter names
     you declare. Always redacted, regardless of the name.
  2. `is_sensitive_key()` — a built-in substring heuristic that redacts a parameter automatically
     if its name contains one of these fragments (case-insensitive):

     ```
     password, passwd, secret, token, api_key, apikey, authorization, auth, credential,
     private_key, access_key, ssn, national_id, credit_card, card_number, cvv, iban, dob,
     date_of_birth, pin
     ```

     **This list has real, unavoidable false negatives.** `email`, `phone_number`, `address`,
     and similar context-dependent fields are *not* redacted automatically — they aren't always
     secret, and dbkit has no way to know your schema. The exact catch/miss boundary above is
     enforced by a test
     (`tests/property/test_invariants.py::test_hint_list_boundary_is_documented_and_tested`), so
     any future change to the list is a deliberate, reviewed decision.

  **If your schema binds anything sensitive that isn't in the list above, declare it explicitly
  via `Query.sensitive_parameters` — do not rely on the heuristic alone.** Parameters are only
  logged at all when `observability.log_parameters=True`, which defaults to `False`; review the
  hint list above against your own schema before enabling it in any environment with real data.

- **Span/trace attributes** never carry SQL text or bound parameters — only logical metadata
  (query name, database, shard, role, row counts, timings). See `docs/api/observability.md`.

## Tenant and shard authorization

dbkit trusts the `DatabaseTarget` (database name, role, shard key) it's given — it has no
concept of "who is asking." Binding a `shard_key`/`database` to the correct, authenticated
tenant is entirely your application's responsibility, checked *before* constructing the
`DatabaseTarget`. `DirectoryShardResolver` fails closed on an unmapped key, but that only
protects against a *missing* mapping, not a caller passing the *wrong* (valid) key for another
tenant.

## Transport security (TLS)

dbkit does not inspect or enforce `sslmode`/TLS settings — that's entirely delegated to the DSN
and driver (psycopg/asyncpg each have their own `sslmode`/`ssl` parameters). Make sure your
production DSNs specify an explicit `sslmode=require` (or stricter, e.g. `verify-full`) rather
than relying on a driver default, which can vary by version and environment.

`dbkit check`/`config-validate` add one cheap, non-enforcing check for this: when
`environment != "development"`, any target URL with no `sslmode`/`ssl` query parameter at all
prints a `[WARNING]` line. This only catches "TLS posture was never even stated" — it does not
verify the value is strict enough, and it never fails the command; a warning is the whole
guarantee. The same commands also warn when a non-development environment has no connection
budget configured, or has one configured but not enforced at startup (`DbkitConfig.
budget_enforcement_warnings()` / `tls_warnings()`) — see `docs/troubleshooting.md` for what to do
about either warning.

## No cross-shard transactions

Each `DatabaseTarget` resolves to exactly one shard; dbkit has no distributed-transaction/2PC
primitive. An operation that must atomically touch multiple shards needs an application-level
pattern (outbox, saga) — this is a deliberate scope boundary, not a missing feature.
