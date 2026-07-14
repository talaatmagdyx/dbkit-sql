# Security Policy

## Supported Versions

dbkit follows [SemVer](https://semver.org/). Security fixes are backported to
the latest minor release on the current major version; older majors are not
supported. Pre-1.0 (`0.x`), only the latest `0.y` release is supported.

| Version | Supported          |
| ------- | ------------------ |
| 0.x     | :white_check_mark: (latest `0.y` only) |

## Reporting a Vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.**

Report suspected vulnerabilities privately by emailing
**talaatmagdy75@gmail.com** with:

- A description of the vulnerability and its potential impact.
- Steps to reproduce (a minimal repro is ideal — a script or failing test
  against a local/disposable PostgreSQL instance).
- The affected version(s) and, if known, the affected file/function.

If the repository has GitHub's [private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing/privately-reporting-a-security-vulnerability)
enabled, you may instead open a report via the **Security** tab → **Report a
vulnerability**.

### What to expect

- **Acknowledgement** within 5 business days.
- An initial assessment (severity, affected versions) within 10 business days
  of acknowledgement.
- A fix or mitigation plan communicated before any public disclosure.
  Coordinated disclosure is expected — please give us a reasonable window
  (typically 90 days, sooner for actively-exploited issues) to ship a fix
  before disclosing publicly.

## Scope

In scope: the `dbkit` package itself (`src/dbkit/`), including its sync and
async frontends, error classification, config parsing, routing/sharding,
observability, bulk/streaming paths, and the CLI.

Out of scope: vulnerabilities in PostgreSQL, SQLAlchemy, psycopg/asyncpg, or
other third-party dependencies (report those upstream), and issues that
require an already-compromised database/network to exploit.

## Known Security-Relevant Design Notes

These are documented behaviors, not vulnerabilities, but are worth reading
before relying on them for security-sensitive deployments (see
[`docs/security.md`](docs/security.md) for the full posture):

- **dbkit trusts the `DatabaseTarget`/shard key you give it.** Tenant/shard
  authorization is your application's responsibility — dbkit routes to
  whatever target it's handed and does not enforce tenant isolation itself.
- **Secret redaction is best-effort, pattern-based.** Logs and error messages
  redact known secret-bearing fields (passwords in DSNs, configured hint
  keys), but a secret you place somewhere unexpected in a query or parameter
  may not be caught. Do not log untrusted/secret data through query text.
- **Connection-budget enforcement is opt-in.** `enforce_at_startup` defaults
  to off so a config change can't silently fail an existing deployment's
  startup; `dbkit check` warns when it's unset in a non-development
  environment. Enable it in production to prevent a misconfigured pool from
  exhausting PostgreSQL's `max_connections`.
- **`retry_writes` + `idempotent=True` are trust-based flags.** dbkit will
  retry a write you marked idempotent; if it isn't actually idempotent, a
  retry after a transient failure can duplicate data. The idempotency lint
  flags obvious misses but cannot see your schema's constraints.
