"""Statistical rigor for benchmarks — robust summaries, percentiles, env fingerprint.

Single-point numbers are fine for "did it get 10x slower" smoke checks but useless for
detecting real regressions on noisy shared runners. Everything here exists to make a number
worth comparing: median-centred summaries (CI runners have heavy right tails), a coefficient
of variation so a reader can tell signal from noise, and an environment fingerprint (a number
without the machine it came from is not comparable).
"""

from __future__ import annotations

import platform
import random
import statistics
import subprocess
import sys
from typing import Any

# Coefficient-of-variation threshold above which a measurement is noise-dominated. 5% is
# strict for shared runners; tables flag unstable rows rather than hiding them.
CV_UNSTABLE = 0.05

#: Bootstrap resamples for the median CI. Pure-stdlib (no scipy/numpy) percentile bootstrap —
#: deliberately not a normal-approximation CI, since REPS is typically only 3-5 in this suite
#: and a handful of samples doesn't justify assuming normality (performance review §14).
_BOOTSTRAP_RESAMPLES = 2000


def bootstrap_ci(samples: list[float], *, confidence: float = 0.95) -> tuple[float, float]:
    """Percentile bootstrap confidence interval for the median of ``samples``.

    Honest about uncertainty for small sample counts rather than a false-precision point
    estimate: with only 3-5 reps (this suite's typical ``REPS``), the interval is often wide —
    that width *is* the signal, not a flaw in the method.
    """
    if len(samples) < 2:
        only = samples[0] if samples else 0.0
        return (only, only)
    rng = random.Random(0)  # fixed seed: the resampling procedure is deterministic, not the data
    n = len(samples)
    medians = sorted(
        statistics.median(samples[rng.randrange(n)] for _ in range(n))
        for _ in range(_BOOTSTRAP_RESAMPLES)
    )
    alpha = (1 - confidence) / 2
    lo = medians[max(0, int(alpha * _BOOTSTRAP_RESAMPLES))]
    hi = medians[min(_BOOTSTRAP_RESAMPLES - 1, int((1 - alpha) * _BOOTSTRAP_RESAMPLES))]
    return (lo, hi)


def robust(samples: list[float]) -> dict[str, float]:
    """Median-centred summary of repeated measurements, with a bootstrap CI on the median."""
    if not samples:
        return {
            "n": 0,
            "median": 0.0,
            "mean": 0.0,
            "cv": 0.0,
            "min": 0.0,
            "max": 0.0,
            "ci_low": 0.0,
            "ci_high": 0.0,
        }
    med = statistics.median(samples)
    mean = statistics.fmean(samples)
    stdev = statistics.stdev(samples) if len(samples) > 1 else 0.0
    ci_low, ci_high = bootstrap_ci(samples)
    return {
        "n": len(samples),
        "median": med,
        "mean": mean,
        "cv": (stdev / mean) if mean else 0.0,
        "min": min(samples),
        "max": max(samples),
        "ci_low": ci_low,
        "ci_high": ci_high,
    }


def fmt_rate(stats: dict[str, float], unit: str = "ops/s") -> str:
    """``median rate ±cv% [95% CI: low-high]`` with an instability flag."""
    flag = " (unstable)" if stats["cv"] > CV_UNSTABLE else ""
    ci = (
        f" [95% CI: {stats['ci_low']:,.0f}-{stats['ci_high']:,.0f}]"
        if stats.get("n", 0) > 1
        else ""
    )
    return f"{stats['median']:>12,.0f} {unit} ±{stats['cv'] * 100:>4.1f}%{ci}{flag}"


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
