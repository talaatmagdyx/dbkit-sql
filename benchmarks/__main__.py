"""Benchmark runner — starts ONE PostgreSQL (or uses --dsn) and runs the selected suites.

python -m benchmarks                          # all suites, testcontainers PostgreSQL
python -m benchmarks --dsn postgresql+psycopg://localhost/postgres
python -m benchmarks --only crud              # one suite (overhead/throughput/latency/batch/crud)
python -m benchmarks --no-save
"""

from __future__ import annotations

import argparse

from . import (
    _common,
    _results,
    _stats,
    bench_batch,
    bench_crud,
    bench_latency,
    bench_overhead,
    bench_throughput,
)

SUITES = {
    "overhead": bench_overhead,
    "throughput": bench_throughput,
    "latency": bench_latency,
    "batch": bench_batch,
    "crud": bench_crud,
}


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m benchmarks")
    parser.add_argument("--dsn", default=None, help="use an existing PostgreSQL DSN")
    parser.add_argument("--only", choices=sorted(SUITES), help="run a single suite")
    parser.add_argument("--no-save", action="store_true", help="do not persist results")
    args = parser.parse_args()

    suites = {args.only: SUITES[args.only]} if args.only else SUITES

    metrics: dict[str, float] = {}
    with _common.dsn_context(args.dsn) as dsn:
        print(f"benchmarking against: {dsn.rsplit('@', 1)[-1]}")
        for name, module in suites.items():
            try:
                metrics.update(module.run_all(dsn))
            except Exception as exc:
                print(f"  ! suite {name!r} failed: {exc}")

    previous = _results.load_previous()
    if not args.no_save:
        path = _results.save(metrics, _stats.env_fingerprint())
        if path:
            print(f"\nsaved results to {path}")
    _results.print_delta(metrics, previous)


if __name__ == "__main__":
    main()
