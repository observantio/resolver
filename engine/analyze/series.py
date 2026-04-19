"""
Shared series processing utilities for analyzers.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

import numpy as np

from config import SLO_ERROR_QUERY, SLO_TOTAL_QUERY, settings
from engine import anomaly


def slo_series_pairs(
    err_raw: anomaly.series.WrappedMimirResponse,
    tot_raw: anomaly.series.WrappedMimirResponse,
    warnings: list[str],
    *,
    error_query: str = SLO_ERROR_QUERY,
    total_query: str = SLO_TOTAL_QUERY,
) -> list[tuple[list[float], list[float], list[float]]]:
    err_series = list(anomaly.iter_series(err_raw, query_hint=error_query))
    tot_series = list(anomaly.iter_series(tot_raw, query_hint=total_query))

    if len(err_series) != len(tot_series):
        warnings.append(
            f"SLO series mismatch: errors={len(err_series)} totals={len(tot_series)}. "
            f"Using first {min(len(err_series), len(tot_series))} pair(s)."
        )

    pairs = []
    for idx in range(min(len(err_series), len(tot_series))):
        _, err_ts, err_vals = err_series[idx]
        _, _tot_ts, tot_vals = tot_series[idx]
        if len(err_vals) != len(tot_vals):
            n = min(len(err_vals), len(tot_vals))
            warnings.append(f"SLO sample length mismatch at pair {idx}: errors={len(err_vals)} totals={len(tot_vals)}.")
            err_vals = err_vals[:n]
            tot_vals = tot_vals[:n]
            err_ts = err_ts[:n]
        if err_vals and tot_vals and err_ts:
            pairs.append((err_ts, err_vals, tot_vals))
    return pairs


def select_granger_series(series_map: dict[str, list[float]]) -> dict[str, list[float]]:
    min_samples = max(2, int(settings.analyzer_granger_min_samples))
    max_series = max(2, int(settings.analyzer_granger_max_series))

    eligible: list[tuple[str, float]] = []
    for name, values in series_map.items():
        arr = np.array(values, dtype=float)
        finite = arr[np.isfinite(arr)]
        if finite.size < min_samples:
            continue
        var = float(np.var(finite))
        if var <= 0:
            continue
        eligible.append((name, var))

    eligible.sort(key=lambda x: x[1], reverse=True)
    selected_names = {name for name, _ in eligible[:max_series]}
    return {name: vals for name, vals in series_map.items() if name in selected_names}
