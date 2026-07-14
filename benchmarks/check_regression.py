"""CI performance-regression gate (performance review §14: "no CI-gating benchmark exists").

Deliberately self-contained — no stored historical baseline to keep in sync or go stale:

1. Compares dbkit against raw SQLAlchemy Core *in the same run* (``bench_overhead``) and fails
   if the gap is at or beyond a generous ceiling. The threshold is set just above dbkit's own
   historical worst case (~31%, before the per-op ``SET statement_timeout`` round-trip was
   dropped from the async path) — loose enough to avoid flaking on noisy shared runners, tight
   enough to catch a genuine regression of that shape reappearing.
2. Re-runs the pool-exhaustion behavioral contract (``bench_pool_exhaustion``): excess demand
   must fail fast with a classified error, never hang. This is a hard pass/fail check, not a
   threshold.

Run:

    uv run python -m benchmarks.check_regression --dsn postgresql+psycopg://dbkit:dbkit@localhost:55432/dbkit
"""

from __future__ import annotations

import argparse

from . import _common, bench_overhead, bench_pool_exhaustion

#: dbkit's own historical worst case (before the async-path SET-statement_timeout fix) was
#: ~31% over raw SQLAlchemy Core. Anything at or beyond that again is a real regression.
MAX_OVERHEAD_PCT = 40.0


def run(dsn: str | None = None) -> int:
    failures: list[str] = []
    with _common.dsn_context(dsn) as resolved:
        overhead = bench_overhead.run_all(resolved)
        pct = overhead.get("overhead_vs_sqlalchemy_pct", 0.0)
        print(
            f"dbkit overhead vs raw SQLAlchemy Core: {pct:+.1f}% (gate: < {MAX_OVERHEAD_PCT:.0f}%)"
        )
        if pct >= MAX_OVERHEAD_PCT:
            failures.append(
                f"dbkit overhead vs raw SQLAlchemy Core is {pct:.1f}%, at or above the "
                f"{MAX_OVERHEAD_PCT:.0f}% regression gate"
            )

        try:
            bench_pool_exhaustion.run_all(resolved)
            print("pool-exhaustion contract: PASS (fails fast, classified, no hang)")
        except AssertionError as exc:
            failures.append(f"pool-exhaustion contract broke: {exc}")

    if failures:
        print("\nPERFORMANCE REGRESSION GATE FAILED:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nperformance regression gate: PASS")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m benchmarks.check_regression")
    parser.add_argument("--dsn", default=None, help="use an existing PostgreSQL DSN")
    args = parser.parse_args()
    raise SystemExit(run(args.dsn))


if __name__ == "__main__":
    main()
