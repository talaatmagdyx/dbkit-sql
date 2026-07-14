"""Run every example in this directory against one PostgreSQL and report pass/fail.

    DBKIT_DSN=postgresql+psycopg://localhost/postgres python examples/run_all.py

Every example is idempotent (safe to run repeatedly against the same database).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

EXAMPLES_DIR = Path(__file__).resolve().parent

# Order matters a little for readability but every example creates/truncates its own tables.
SCRIPTS = [
    "quickstart_async.py",
    "quickstart_sync.py",
    "transactions_savepoints.py",
    "error_handling.py",
    "retries_and_circuit_breaker.py",
    "streaming.py",
    "bulk_insert_upsert.py",
    "copy_ingest.py",
    "inbox_idempotent_consumer.py",
    "batch_collector.py",
    "health_and_pool.py",
    "sync_feature_parity.py",
]


def main() -> int:
    dsn = os.environ.get("DBKIT_DSN", "postgresql+psycopg://localhost/postgres")
    env = {**os.environ, "DBKIT_DSN": dsn}
    results: list[tuple[str, bool]] = []
    for name in SCRIPTS:
        path = EXAMPLES_DIR / name
        print(f"\n{'=' * 72}\n{name}\n{'=' * 72}")
        proc = subprocess.run([sys.executable, str(path)], env=env)
        results.append((name, proc.returncode == 0))

    print(f"\n{'=' * 72}\nsummary\n{'=' * 72}")
    for name, ok in results:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    failures = [n for n, ok in results if not ok]
    if failures:
        print(f"\n{len(failures)} example(s) failed: {failures}")
        return 1
    print(f"\nall {len(results)} examples passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
