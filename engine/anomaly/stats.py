"""
Descriptive statistics for metric series (IQR, MAD, skewness, kurtosis) used in RCA reports.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import math
from typing import cast

import numpy as np
from numpy.typing import NDArray

from api.responses.analysis import MetricSeriesDistributionStats
from config import settings


def _finite_array(vals: list[float]) -> NDArray[np.float64]:
    arr = np.asarray(vals, dtype=float)
    finite = arr[np.isfinite(arr)]
    return cast(NDArray[np.float64], finite)


def _sample_skewness(vals: NDArray[np.float64]) -> float:
    n = int(vals.size)
    if n < 3:
        return 0.0

    mean = float(vals.mean())
    centered = vals - mean
    m2 = float(np.mean(centered**2))
    if m2 <= 0.0:
        return 0.0
    m3 = float(np.mean(centered**3))
    g1 = m3 / (m2**1.5)
    return float((math.sqrt(n * (n - 1)) / (n - 2)) * g1)


def _sample_excess_kurtosis(vals: NDArray[np.float64]) -> float:
    n = int(vals.size)
    if n < 4:
        return 0.0

    mean = float(vals.mean())
    centered = vals - mean
    m2 = float(np.mean(centered**2))
    if m2 <= 0.0:
        return 0.0
    m4 = float(np.mean(centered**4))
    g2 = (m4 / (m2 * m2)) - 3.0
    factor = ((n - 1) / ((n - 2) * (n - 3))) * ((n + 1) * g2 + 6.0)
    return float(factor)


def compute_series_distribution_stats(
    series_key: str,
    metric_name: str,
    vals: list[float],
) -> MetricSeriesDistributionStats | None:
    finite = _finite_array(vals)
    n = int(finite.size)
    if n < settings.min_samples:
        return None

    mean = float(finite.mean())
    std = float(finite.std(ddof=0))
    vmin = float(finite.min())
    vmax = float(finite.max())
    median = float(np.median(finite))
    q1 = float(np.percentile(finite, 25))
    q3 = float(np.percentile(finite, 75))
    iqr = float(q3 - q1)
    mad = float(np.median(np.abs(finite - median)))

    skew_raw = _sample_skewness(finite)
    kurt_raw = _sample_excess_kurtosis(finite)
    skewness = skew_raw if math.isfinite(skew_raw) else 0.0
    kurtosis = kurt_raw if math.isfinite(kurt_raw) else 0.0

    if abs(mean) > 1e-12:
        cv = float(std / mean)
    else:
        cv = 0.0
    if not math.isfinite(cv):
        cv = 0.0

    return MetricSeriesDistributionStats(
        series_key=series_key,
        metric_name=metric_name,
        sample_count=n,
        mean=round(mean, 6),
        std=round(std, 6),
        min=round(vmin, 6),
        max=round(vmax, 6),
        median=round(median, 6),
        q1=round(q1, 6),
        q3=round(q3, 6),
        iqr=round(iqr, 6),
        mad=round(mad, 6),
        skewness=round(skewness, 6),
        kurtosis=round(kurtosis, 6),
        coefficient_of_variation=round(cv, 6),
    )
