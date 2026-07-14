"""Unit tests for benchmarks/_stats.py's statistical helpers (performance review §14: the
benchmark suite itself had no confidence-interval computation and no test coverage)."""

from __future__ import annotations

from benchmarks import _stats


def test_robust_empty_samples() -> None:
    stats = _stats.robust([])
    assert stats == {
        "n": 0,
        "median": 0.0,
        "mean": 0.0,
        "cv": 0.0,
        "min": 0.0,
        "max": 0.0,
        "ci_low": 0.0,
        "ci_high": 0.0,
    }


def test_robust_single_sample_has_a_degenerate_ci() -> None:
    stats = _stats.robust([42.0])
    assert stats["median"] == 42.0
    assert stats["cv"] == 0.0
    assert stats["ci_low"] == stats["ci_high"] == 42.0


def test_robust_identical_samples_have_zero_width_ci() -> None:
    stats = _stats.robust([10.0, 10.0, 10.0, 10.0])
    assert stats["median"] == 10.0
    assert stats["cv"] == 0.0
    assert stats["ci_low"] == stats["ci_high"] == 10.0


def test_bootstrap_ci_contains_the_true_median_for_a_stable_signal() -> None:
    # low-noise samples clustered tightly around 100 -- the CI should be narrow and centred there
    samples = [98.0, 99.0, 100.0, 101.0, 102.0]
    lo, hi = _stats.bootstrap_ci(samples)
    assert lo <= 100.0 <= hi
    assert hi - lo < 10.0  # narrow for low-variance input


def test_bootstrap_ci_widens_for_noisy_samples() -> None:
    tight = _stats.bootstrap_ci([100.0, 100.0, 100.0, 100.0])
    noisy = _stats.bootstrap_ci([50.0, 100.0, 150.0, 200.0])
    assert (noisy[1] - noisy[0]) > (tight[1] - tight[0])


def test_bootstrap_ci_is_deterministic_across_calls() -> None:
    samples = [12.0, 15.0, 9.0, 20.0, 11.0]
    assert _stats.bootstrap_ci(samples) == _stats.bootstrap_ci(samples)


def test_fmt_rate_includes_ci_for_multiple_samples() -> None:
    stats = _stats.robust([100.0, 105.0, 95.0])
    text = _stats.fmt_rate(stats)
    assert "95% CI" in text


def test_fmt_rate_omits_ci_for_a_single_sample() -> None:
    stats = _stats.robust([100.0])
    text = _stats.fmt_rate(stats)
    assert "95% CI" not in text


def test_fmt_rate_flags_unstable_measurements() -> None:
    noisy = _stats.robust([10.0, 100.0, 1000.0])
    assert "unstable" in _stats.fmt_rate(noisy)


def test_percentiles_nearest_rank() -> None:
    pct = _stats.percentiles(list(range(1, 101)), points=(50, 99))
    assert pct["p50"] == 50
    assert pct["p99"] == 99
    assert pct["max"] == 100


def test_percentiles_empty() -> None:
    assert _stats.percentiles([]) == {}
