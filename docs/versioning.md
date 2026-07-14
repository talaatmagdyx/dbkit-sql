# Versioning and Deprecation Policy

dbkit follows [Semantic Versioning](https://semver.org/) (`MAJOR.MINOR.PATCH`), with the
current pre-1.0 caveats below made explicit.

## Pre-1.0 (current: `0.1.0`, alpha)

Per SemVer, a `0.y.z` version makes **no compatibility guarantee** — any `0.y` bump may contain
breaking changes. In practice, dbkit tries to be more conservative than the spec strictly
requires:

- Breaking changes to the public API (anything importable from `dbkit`, `dbkit.errors`,
  `dbkit.observability`, `dbkit.integrations`, or the CLI) are called out explicitly in
  `CHANGELOG.md` under a `### Changed` or `### Removed` heading — never silently.
- Internal modules (anything under `dbkit._core`, `dbkit._async`, `dbkit._sync`,
  `dbkit._pool`) carry no stability guarantee at all, pre- or post-1.0 — the underscore prefix
  is the signal. `dbkit._sync` specifically is generated output; never depend on its internals
  directly regardless of version.
- Config schema changes (new `DbkitConfig`/`*Config` fields) are additive with safe defaults
  wherever possible, so existing config dicts/YAML keep working across `0.y` bumps.

## What "stable 1.0" means for dbkit

Before tagging `1.0.0`, the following must hold (tracked in
`PRODUCTION_READINESS_REVIEW.md`'s "Recommended Changes Before Stable 1.0"):

- A real production track record — at least one non-trivial deployment running dbkit, not just
  test coverage (no amount of test coverage substitutes for production traffic surfacing
  timing-dependent bugs, driver-version interactions, or operational surprises).
- asyncpg's support tier is either fully CI-covered (as it now is for the async frontend) or
  explicitly documented as a narrower contract in the 1.0 API reference.
- The public API surface is deliberately reviewed once for anything that reads as an accident
  (an internal helper importable from a public path, a config field with unclear semantics).

## Post-1.0 policy

Once `1.0.0` ships:

- **`MAJOR`** — breaking changes to the public API surface, config schema removals, or dropped
  Python/PostgreSQL/driver support.
- **`MINOR`** — new features, new config fields (additive, with defaults), new CLI commands.
  Deprecating something (see below) is a `MINOR` bump, not `MAJOR`.
- **`PATCH`** — bug fixes with no API surface change.

**Deprecation window:** a deprecated feature emits a `DeprecationWarning` (Python's own, so it's
visible via `-W` and pytest's default filters) for at least one `MINOR` release before removal
in the next `MAJOR`. The `CHANGELOG.md` entry that introduces a deprecation states the intended
removal version explicitly.

**Driver/Python support:** dropping support for a Python version or a PostgreSQL major version
is itself a `MAJOR` bump, announced at least one `MINOR` release in advance in `CHANGELOG.md`.

## What is never covered by any compatibility guarantee

- The exact wording of exception messages (only the exception *type*, `code`, `category`, and
  attributes like `sqlstate`/`retryable` are part of the contract — see `docs/api/errors.md`).
- The exact set/naming of structured log event fields beyond what's documented in §25.3 and
  `docs/observability.md`.
- Benchmark numbers in `CHANGELOG.md`/`docs/roadmap.md` — illustrative, not a performance SLA.
