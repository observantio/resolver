"""
Compute logic for calculating baseline statistics (mean, standard deviation, confidence intervals) for a given set of
time series data points, with optional seasonal adjustment based on hourly patterns, to assist in anomaly detection by
providing a reference point for identifying significant deviations in metric values.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from config import settings


@dataclass(frozen=True)
class Baseline:
    mean: float
    std: float
    lower: float
    upper: float
    seasonal_mean: float | None = None
    sample_count: int = 0


def _hour_buckets(ts: list[float]) -> list[int]:
    return [(int(t) % 86400) // 3600 for t in ts]


def compute(ts: list[float], vals: list[float], z_threshold: float | None = None) -> Baseline:
    if z_threshold is None:
        z_threshold = settings.baseline_zscore_threshold
    arr = np.array(vals, dtype=float)
    n = len(arr)

    if n < settings.baseline_min_samples:
        m = float(np.mean(arr))
        s = float(np.std(arr)) or 1.0
        return Baseline(mean=m, std=s, lower=m - z_threshold * s, upper=m + z_threshold * s, sample_count=n)

    seasonal_mean: float | None = None

    if n >= settings.baseline_seasonal_min_samples:
        buckets = _hour_buckets(ts)
        bucket_map: dict[int, list[float]] = {}
        for b, v in zip(buckets, vals):
            bucket_map.setdefault(b, []).append(v)
        hour_avgs = {h: float(np.mean(v)) for h, v in bucket_map.items()}
        detrended = np.array([v - hour_avgs.get(b, 0.0) for b, v in zip(buckets, vals)])
        m = float(np.mean(arr))
        s = float(np.std(detrended)) or 1.0
        seasonal_mean = float(np.mean(list(hour_avgs.values())))
    else:
        m = float(np.mean(arr))
        s = float(np.std(arr)) or 1.0

    return Baseline(
        mean=m,
        std=s,
        lower=m - z_threshold * s,
        upper=m + z_threshold * s,
        seasonal_mean=seasonal_mean,
        sample_count=n,
    )


def score(val: float, baseline: Baseline) -> tuple[bool, float]:
    z = abs(val - baseline.mean) / baseline.std if baseline.std else 0.0
    return (val < baseline.lower or val > baseline.upper), round(z, 3)
