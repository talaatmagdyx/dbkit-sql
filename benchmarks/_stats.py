"""Statistical rigor for benchmarks — robust summaries, percentiles, env fingerprint.

Single-point numbers are fine for "did it get 10x slower" smoke checks but useless for
detecting real regressions on noisy shared runners. Everything here exists to make a number
worth comparing: median-centred summaries (CI runners have heavy right tails), a coefficient
of variation so a reader can tell signal from noise, and an environment fingerprint (a number
without the machine it came from is not comparable).
"""

from __future__ import annotations

import platform
import statistics
import subprocess
import sys
from typing import Any

# Coefficient-of-variation threshold above which a measurement is noise-dominated. 5% is
# strict for shared runners; tables flag unstable rows rather than hiding them.
CV_UNSTABLE = 0.05


def robust(samples: list[float]) -> dict[str, float]:
    """Median-centred summary of repeated measurements."""
    if not samples:
        return {"n": 0, "median": 0.0, "mean": 0.0, "cv": 0.0, "min": 0.0, "max": 0.0}
    med = statistics.median(samples)
    mean = statistics.fmean(samples)
    stdev = statistics.stdev(samples) if len(samples) > 1 else 0.0
    return {
        "n": len(samples),
        "median": med,
        "mean": mean,
        "cv": (stdev / mean) if mean else 0.0,
        "min": min(samples),
        "max": max(samples),
    }


def fmt_rate(stats: dict[str, float], unit: str = "ops/s") -> str:
    """``median rate ±cv%`` with an instability flag."""
    flag = " (unstable)" if stats["cv"] > CV_UNSTABLE else ""
    return f"{stats['median']:>12,.0f} {unit} ±{stats['cv'] * 100:>4.1f}%{flag}"


def percentiles(
    samples: list[float], points: tuple[float, ...] = (50, 95, 99, 99.9)
) -> dict[str, float]:
    """Nearest-rank percentiles. A p99.9 needs ~10k samples to mean anything; callers that
    can't collect that many should omit the point."""
    if not samples:
        return {}
    s = sorted(samples)
    out: dict[str, float] = {}
    for p in points:
        idx = min(len(s) - 1, max(0, round(p / 100 * len(s)) - 1))
        out[f"p{p:g}"] = s[idx]
    out["max"] = s[-1]
    return out


def env_fingerprint() -> dict[str, Any]:
    """The machine context a result is only meaningful together with."""
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        ).stdout.strip()
    except Exception:
        sha = "unknown"
    try:
        import os

        cpu = os.cpu_count() or 0
    except Exception:
        cpu = 0
    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": cpu,
        "git_sha": sha,
    }
