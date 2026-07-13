"""Benchmark result persistence — save runs and compare against the previous one.

Results are JSON at ``benchmarks/results/run_<timestamp>.json`` holding a flat
``{metric_name: float}`` for all suites. Metric keys encode direction so regression deltas
know which way is better: ``*_ops_s`` is higher-better, ``*_ms``/``*_ns``/``*_pct`` are
lower-better.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

RESULTS_DIR = Path(__file__).parent / "results"

HIGHER_BETTER_SUFFIXES = ("_ops_s", "_rows_s")
LOWER_BETTER_HINTS = ("_ms", "_ns", "_pct", "_rss_mb")


def save(metrics: dict[str, float], fingerprint: dict[str, Any] | None = None) -> Path | None:
    try:
        RESULTS_DIR.mkdir(exist_ok=True)
        ts = int(time.time())
        path = RESULTS_DIR / f"run_{ts}.json"
        with open(path, "w") as f:
            json.dump({"timestamp": ts, "env": fingerprint or {}, "metrics": metrics}, f, indent=2)
        return path
    except Exception:
        return None


def load_previous() -> dict[str, float] | None:
    if not RESULTS_DIR.exists():
        return None
    files = sorted(RESULTS_DIR.glob("run_*.json"))
    if not files:
        return None
    try:
        with open(files[-1]) as f:
            obj: Any = json.load(f)
        return obj.get("metrics", {})
    except Exception:
        return None


def print_delta(current: dict[str, float], previous: dict[str, float] | None) -> None:
    """Print a compact regression table vs the previous run (2% dead-band)."""
    if not previous:
        print("\n(no previous run to compare against)")
        return
    print("\n=== regression vs previous run ===")
    for key, val in sorted(current.items()):
        prev = previous.get(key)
        if prev is None or prev == 0:
            continue
        pct = (val - prev) / abs(prev) * 100
        higher_better = any(key.endswith(s) for s in HIGHER_BETTER_SUFFIXES)
        lower_better = any(h in key for h in LOWER_BETTER_HINTS) and not higher_better
        if higher_better:
            trend = "[+]" if pct > 2 else ("[-]" if pct < -2 else "[~]")
        elif lower_better:
            trend = "[-]" if pct > 2 else ("[+]" if pct < -2 else "[~]")
        else:
            trend = "[~]"
        print(f"  {trend} {key:<40} {prev:>12,.2f} -> {val:>12,.2f}  ({pct:+.1f}%)")
