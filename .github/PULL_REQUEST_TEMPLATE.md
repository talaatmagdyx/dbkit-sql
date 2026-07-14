## Summary

<!-- What does this PR change, and why? Link the issue it addresses. -->

## Checklist

- [ ] `make lint` passes (`ruff check` + `ruff format --check`)
- [ ] `make type` passes (`mypy --strict`)
- [ ] `make test` passes (unit + property + security, no database)
- [ ] Edited `src/dbkit/_async/` only — never hand-edited `src/dbkit/_sync/`;
      ran `make unasync` so the generated sync code is in sync (CI fails on drift)
- [ ] Tests added/updated (`tests/unit/…` and/or `tests/integration/…`)
- [ ] `CHANGELOG.md` updated under `[Unreleased]`
- [ ] `README.md` / `docs/` updated (if user-facing)
- [ ] If this touches SQL execution, transactions, pooling, or routing, verified
      against `tests/integration/` (real PostgreSQL via `DBKIT_TEST_DSN`/testcontainers)

## Test plan

<!-- How did you verify this? What did you run, and what did it prove? -->
