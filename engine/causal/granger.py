"""
Granger causality analysis logic for determining whether one time series can be considered a cause of another based on
the predictability of the effect series using past values of the cause series, to assist in root cause analysis and
understanding of relationships between metrics.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module

import numpy as np

from config import settings

_scipy_stats = import_module("scipy.stats")


@dataclass(frozen=True)
class GrangerResult:
    cause_metric: str
    effect_metric: str
    max_lag: int
    f_statistic: float
    p_value: float
    is_causal: bool
    strength: float


def _ols(x: np.ndarray, y: np.ndarray) -> tuple[np.ndarray, float]:
    coeffs, _, _, _ = np.linalg.lstsq(x, y, rcond=None)
    predicted = x @ coeffs
    ss_res = float(np.sum((y - predicted) ** 2))
    return coeffs, ss_res


def _lag_matrix(series: np.ndarray, max_lag: int) -> np.ndarray:
    n = len(series) - max_lag
    cols = [np.ones(n)]
    for lag in range(1, max_lag + 1):
        cols.append(series[max_lag - lag : max_lag - lag + n])
    return np.column_stack(cols)


def granger_pair_analysis(
    cause_name: str,
    cause_vals: list[float],
    effect_name: str,
    effect_vals: list[float],
    max_lag: int | None = None,
    p_threshold: float | None = None,
) -> GrangerResult | None:
    if max_lag is None:
        max_lag = settings.granger_max_lag
    if p_threshold is None:
        p_threshold = settings.granger_p_threshold
    if len(cause_vals) != len(effect_vals) or len(cause_vals) < max_lag + 10:
        return None

    cause = np.array(cause_vals, dtype=float)
    effect = np.array(effect_vals, dtype=float)

    n = len(effect) - max_lag
    y = effect[max_lag:]

    x_restricted = _lag_matrix(effect, max_lag)
    _, ss_restricted = _ols(x_restricted, y)

    cause_lags = np.column_stack([cause[max_lag - lag : max_lag - lag + n] for lag in range(1, max_lag + 1)])
    x_unrestricted = np.hstack([x_restricted, cause_lags])
    _, ss_unrestricted = _ols(x_unrestricted, y)

    k = max_lag
    denom_df = n - 2 * max_lag - 1
    if denom_df <= 0 or ss_unrestricted == 0:
        return None

    f_stat = ((ss_restricted - ss_unrestricted) / k) / (ss_unrestricted / denom_df)
    f_stat = float(f_stat)

    p_value = float(1.0 - _scipy_stats.f.cdf(f_stat, k, denom_df))

    is_causal = p_value < p_threshold and f_stat > 1.0
    strength = round(
        max(0.0, 1.0 - p_value) * min(1.0, f_stat / settings.granger_strength_scale),
        3,
    )

    return GrangerResult(
        cause_metric=cause_name,
        effect_metric=effect_name,
        max_lag=max_lag,
        f_statistic=round(f_stat, 4),
        p_value=round(p_value, 6),
        is_causal=is_causal,
        strength=strength,
    )


def granger_multiple_pairs(
    series_map: dict[str, list[float]],
    max_lag: int | None = None,
    p_threshold: float | None = None,
) -> list[GrangerResult]:
    if max_lag is None:
        max_lag = settings.granger_max_lag
    if p_threshold is None:
        p_threshold = settings.granger_p_threshold
    names = list(series_map.keys())
    results: list[GrangerResult] = []

    for i, cause in enumerate(names):
        for j, effect in enumerate(names):
            if i == j:
                continue
            result = granger_pair_analysis(
                cause,
                series_map[cause],
                effect,
                series_map[effect],
                max_lag=max_lag,
                p_threshold=p_threshold,
            )
            if result and result.is_causal:
                results.append(result)

    return sorted(results, key=lambda r: r.strength, reverse=True)
