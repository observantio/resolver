"""
Trajectory forecasting logic for metrics, using linear regression to predict future values based on recent trends, and
estimating time to breach thresholds with confidence scoring and severity classification.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np

from config import settings
from engine.enums import Severity


@dataclass(frozen=True)
class TrajectoryForecast:
    metric_name: str
    current_value: float
    slope_per_second: float
    predicted_value_at_horizon: float
    time_to_threshold_seconds: float | None
    breach_threshold: float
    confidence: float
    severity: Severity


def _linear_fit(ts: Sequence[float], vals: Sequence[float]) -> tuple[float, float]:
    t = np.array(ts, dtype=float)
    v = np.array(vals, dtype=float)
    t_norm = t - t[0]
    slope, intercept = np.polyfit(t_norm, v, 1)
    return float(slope), float(intercept)


def _r_squared(ts: Sequence[float], vals: Sequence[float], slope: float, intercept: float) -> float:
    t_norm = np.array(ts, dtype=float) - ts[0]
    v = np.array(vals, dtype=float)
    predicted = slope * t_norm + intercept
    ss_res = np.sum((v - predicted) ** 2)
    ss_tot = np.sum((v - np.mean(v)) ** 2)
    return float(1.0 - ss_res / ss_tot) if ss_tot > 0 else 0.0


def forecast(
    metric_name: str,
    ts: Sequence[float],
    vals: Sequence[float],
    threshold: float,
    *,
    horizon_seconds: float | None = None,
) -> TrajectoryForecast | None:
    if horizon_seconds is None:
        horizon_seconds = settings.forecast_trajectory_horizon_cutoff
    if len(vals) < settings.forecast_trajectory_min_length:
        return None

    slope, intercept = _linear_fit(ts, vals)
    r2 = _r_squared(ts, vals, slope, intercept)

    if r2 < settings.forecast_trajectory_r2_threshold or slope == 0:
        return None

    now_offset = ts[-1] - ts[0]
    current = slope * now_offset + intercept
    predicted_at_horizon = slope * (now_offset + horizon_seconds) + intercept

    time_to_threshold: float | None = None
    if slope > 0 and current < threshold:
        time_to_threshold = (threshold - current) / slope
    elif slope < 0 and current > threshold:
        time_to_threshold = (current - threshold) / abs(slope)

    will_breach = time_to_threshold is not None and time_to_threshold <= horizon_seconds
    if (
        not will_breach
        and abs(predicted_at_horizon - threshold) / (abs(threshold) + 1e-9)
        > settings.forecast_trajectory_ratio_threshold
    ):
        return None

    confidence = round(min(0.99, r2 * (1.0 - min(1.0, abs(slope) / (abs(current) + 1e-9)))), 3)

    window = settings.forecast_trajectory_window_seconds
    if time_to_threshold and time_to_threshold < window:
        sev = Severity.CRITICAL
    elif time_to_threshold and time_to_threshold < window * 3:
        sev = Severity.HIGH
    elif will_breach:
        sev = Severity.MEDIUM
    else:
        sev = Severity.LOW

    return TrajectoryForecast(
        metric_name=metric_name,
        current_value=round(current, 4),
        slope_per_second=round(slope, 6),
        predicted_value_at_horizon=round(predicted_at_horizon, 4),
        time_to_threshold_seconds=round(time_to_threshold, 1) if time_to_threshold else None,
        breach_threshold=threshold,
        confidence=confidence,
        severity=sev,
    )
