"""
Tests for per-series distribution statistics used in RCA reports.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import pytest

from config import settings

_WARN_FILTERS = (
    "ignore:Precision loss:RuntimeWarning",
    "ignore:overflow encountered:RuntimeWarning",
    "ignore:invalid value encountered:RuntimeWarning",
)

from engine.anomaly.stats import compute_series_distribution_stats


def test_compute_series_distribution_stats_insufficient_samples():
    vals = [float(i) for i in range(max(0, settings.min_samples - 1))]
    assert compute_series_distribution_stats("q::m", "m", vals) is None


def test_compute_series_distribution_stats_filters_non_finite():
    base = [float(i) for i in range(settings.min_samples)]
    vals = base[:-3] + [float("nan"), float("inf"), float("-inf")] + base[-3:]
    out = compute_series_distribution_stats("q::m", "m", vals)
    assert out is not None
    assert out.sample_count == settings.min_samples
    assert out.metric_name == "m"
    assert out.series_key == "q::m"


@pytest.mark.filterwarnings(*_WARN_FILTERS)
def test_compute_series_distribution_stats_mean_near_zero_cv_branch():
    vals = [1e-13] * settings.min_samples
    out = compute_series_distribution_stats("k", "low_mean", vals)
    assert out is not None
    assert abs(out.mean) <= 1e-12
    assert out.coefficient_of_variation == 0.0


@pytest.mark.filterwarnings(*_WARN_FILTERS)
def test_compute_series_distribution_stats_non_finite_cv_resets_to_zero():
    # Sum order: large +/- pair first so small terms survive; std overflows while mean stays > 1e-12.
    vals = [1e308, -1e308] + [1e-11] * (settings.min_samples - 2)
    out = compute_series_distribution_stats("k", "cv_inf", vals)
    assert out is not None
    assert out.coefficient_of_variation == 0.0


@pytest.mark.filterwarnings(*_WARN_FILTERS)
def test_compute_series_distribution_stats_nan_skew_kurtosis_use_zero():
    vals = [1.0] * settings.min_samples
    out = compute_series_distribution_stats("k", "constant", vals)
    assert out is not None
    assert out.skewness == 0.0
    assert out.kurtosis == 0.0


def test_compute_series_distribution_stats_typical_series():
    vals = [float(i) for i in range(settings.min_samples)]
    out = compute_series_distribution_stats("k", "ramp", vals)
    assert out is not None
    assert out.sample_count == settings.min_samples
    assert out.iqr >= 0.0
    assert out.mad >= 0.0
    assert out.min <= out.max


def test_sample_moment_edge_cases_for_skewness_and_kurtosis():
    from engine.anomaly.stats import _sample_skewness, _sample_excess_kurtosis

    import numpy as np

    assert _sample_skewness(np.array([1.0, 1.0])) == 0.0
    assert _sample_skewness(np.array([1.0, 1.0, 1.0])) == 0.0
    assert _sample_excess_kurtosis(np.array([1.0, 1.0, 1.0])) == 0.0
    assert _sample_excess_kurtosis(np.array([1.0, 1.0, 1.0, 1.0])) == 0.0
